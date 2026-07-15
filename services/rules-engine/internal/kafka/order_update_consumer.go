package kafka

import (
	"context"
	"encoding/json"
	"log"
	"runtime/debug"
	"strings"
	"time"

	segkafka "github.com/segmentio/kafka-go"
)

// SignalStatusUpdater is the slice of the trade-signal repository this consumer
// needs. Satisfied by *repository.TradeSignalRepository.
type SignalStatusUpdater interface {
	UpdateSignalStatus(ctx context.Context, orderID, status string, executionPrice float64, brokerOrderID, errorMsg string) error
}

// orderUpdate is the subset of trade-execution's OrderUpdate we consume. We only
// need the fields that drive a trade_signals.status transition; everything else
// in the payload is ignored.
type orderUpdate struct {
	OrderID    string `json:"order_id"`
	UpdateType string `json:"update_type"`
	Status     string `json:"status"`
}

// OrderUpdateConsumer closes the loop that was previously missing: it consumes
// the "order-updates" events trade-execution already publishes and writes the
// resulting execution status back onto trade_signals (via the long-dormant
// UpdateSignalStatus). This keeps the durable committed-trade count accurate —
// so a Redis flush can be reseeded correctly — and lets a below_min watch that
// fills or is cancelled release the per-stock daily lock.
//
// Best-effort and defensive: a bad or unmapped message is committed and skipped,
// never retried forever, and a panic in one message never kills the loop.
type OrderUpdateConsumer struct {
	reader     KafkaReader
	signalRepo SignalStatusUpdater
	logger     *log.Logger
}

// NewOrderUpdateConsumer builds the consumer. logger may be nil (falls back to
// the stdlib default logger, matching the other consumers in this package).
func NewOrderUpdateConsumer(reader KafkaReader, signalRepo SignalStatusUpdater) *OrderUpdateConsumer {
	return &OrderUpdateConsumer{reader: reader, signalRepo: signalRepo, logger: log.Default()}
}

func (c *OrderUpdateConsumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Printf("order-update consumer: fetch error: %v", err)
			time.Sleep(250 * time.Millisecond)
			continue
		}
		c.processMessageRecovered(ctx, msg)
	}
}

func (c *OrderUpdateConsumer) processMessageRecovered(ctx context.Context, msg segkafka.Message) {
	defer func() {
		if rec := recover(); rec != nil {
			c.logger.Printf("order-update consumer: PANIC recovered (message skipped): %v\n%s", rec, debug.Stack())
			_ = c.reader.CommitMessages(ctx, msg)
		}
	}()
	c.processMessage(ctx, msg)
}

func (c *OrderUpdateConsumer) processMessage(ctx context.Context, msg segkafka.Message) {
	var ev orderUpdate
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		c.logger.Printf("order-update consumer: parse error: %v", err)
		_ = c.reader.CommitMessages(ctx, msg)
		return
	}

	if status, ok := signalStatusFor(ev); ok && ev.OrderID != "" && c.signalRepo != nil {
		if err := c.signalRepo.UpdateSignalStatus(ctx, ev.OrderID, status, 0, "", ""); err != nil {
			c.logger.Printf("order-update consumer: failed to update signal %s → %s: %v", ev.OrderID, status, err)
		}
	}

	_ = c.reader.CommitMessages(ctx, msg)
}

// signalStatusFor maps an OrderUpdate to the trade_signals.status it should
// produce, or ok=false for transient updates that should not change status
// (e.g. PRICE_MONITOR_TRIGGERED, which fires before the order is confirmed).
//
// The mapping is deliberately conservative and terminal-only so the durable
// committed-trade count (CountCommittedTradesToday) stays truthful:
//   - EXECUTED  → the trade actually happened (counts toward the cap on reseed).
//   - CANCELLED → the watch/order was withdrawn (never counts; frees the stock lock).
//   - FAILED    → the order was rejected downstream (never counts).
func signalStatusFor(ev orderUpdate) (string, bool) {
	switch strings.ToUpper(ev.UpdateType) {
	case "PRICE_WATCH_CANCELLED", "STRATEGY_TRADE_CAP_REACHED":
		return "CANCELLED", true
	case "ORDER_REJECTED":
		return "FAILED", true
	}

	switch strings.ToUpper(ev.Status) {
	case "FILLED", "EXECUTED", "TRADED", "PARTIALLY_FILLED":
		return "EXECUTED", true
	case "CANCELLED", "EXPIRED":
		return "CANCELLED", true
	case "REJECTED", "FAILED":
		return "FAILED", true
	}
	return "", false
}
