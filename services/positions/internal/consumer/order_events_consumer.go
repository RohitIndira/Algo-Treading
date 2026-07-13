// Package consumer owns the order.events Kafka reader for positions svc.
//
// One reader, one goroutine, at-least-once delivery. Every message is
// parsed into an OrderEvent envelope and handed to the injected Handler.
// P.B (this chunk) uses a log-only Handler — real state machine work
// lands in P.C via the Handler interface.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// OrderEvent is the JSON envelope orderstatus svc publishes to order.events.
// Field naming matches services/orderstatus/internal/publisher/publisher.go —
// we parse locally rather than import across the service boundary. The wire
// is the contract; the Go type is duplicated by design.
type OrderEvent struct {
	// Envelope
	EventID      string `json:"event_id"`
	EventType    string `json:"event_type"`
	EventSeq     int64  `json:"event_seq"`
	Source       string `json:"source"`
	ProducedAtMs int64  `json:"produced_at_ms"`
	BrokerTsMs   int64  `json:"broker_ts_ms,omitempty"`

	// Identity
	BrokerOrderID   string `json:"broker_order_id"`
	ExchangeOrderID string `json:"exchange_order_id,omitempty"`
	UserID          string `json:"user_id"`
	Symbol          string `json:"symbol,omitempty"`
	Exchange        string `json:"exchange,omitempty"`
	BuySell         string `json:"buy_sell,omitempty"` // "1"=BUY "2"=SELL
	OrderType       string `json:"order_type,omitempty"`
	Product         string `json:"product,omitempty"`

	// State
	Status        string `json:"status,omitempty"`
	OMSStatusCode int    `json:"oms_status_code,omitempty"`

	// Numbers
	OrderPrice   float64 `json:"order_price,omitempty"`
	TriggerPrice float64 `json:"trigger_price,omitempty"`
	Quantity     int     `json:"quantity,omitempty"`
	FilledQty    int     `json:"filled_qty,omitempty"`
	TradedPrice  float64 `json:"traded_price,omitempty"`
	PendingQty   int     `json:"pending_qty,omitempty"`

	// Reason (rejections, cancellations)
	Reason string `json:"reason,omitempty"`

	// Raw source message (populated by the consumer, not by orderstatus) —
	// carries the full JSON envelope for downstream audit into position_events.
	// This is not a wire field; it's a runtime aid.
	RawMessage []byte `json:"-"`
}

// Handler processes one parsed OrderEvent. Return an error to signal the
// consumer NOT to commit the offset (message will be re-delivered on next
// FetchMessage / restart).
//
// P.B (this chunk) uses LoggingHandler — see below. P.C replaces it with
// the state-machine handler that enriches via LookupOrderMeta and writes
// positions + position_events.
type Handler interface {
	Handle(ctx context.Context, ev *OrderEvent) error
}

// LoggingHandler is the P.B stub — just logs each event. Zero side-effects
// beyond the log line. Useful for proving the consumer plumbing works
// before P.C's state machine takes over.
type LoggingHandler struct {
	Logger *zap.Logger
}

func (h *LoggingHandler) Handle(_ context.Context, ev *OrderEvent) error {
	h.Logger.Info("order.events received (P.B stub — no processing yet)",
		zap.String("event_id", ev.EventID),
		zap.String("event_type", ev.EventType),
		zap.String("broker_order_id", ev.BrokerOrderID),
		zap.String("user_id", ev.UserID),
		zap.String("symbol", ev.Symbol),
		zap.String("buy_sell", ev.BuySell),
		zap.String("status", ev.Status),
		zap.String("source", ev.Source),
		zap.Int64("event_seq", ev.EventSeq))
	return nil
}

// OrderEventsConsumer wraps a *kafka.Reader + Handler. Start blocks
// until ctx is cancelled; commits the offset AFTER a successful Handle.
type OrderEventsConsumer struct {
	reader  *kafka.Reader
	handler Handler
	logger  *zap.Logger
}

// Config for the Kafka reader.
type Config struct {
	KafkaBrokers []string
	Topic        string // default: order.events
	GroupID      string // default: positions-order-events
}

// New wires the reader + handler. Uses StartOffset=LastOffset so a first
// boot doesn't replay history — production-appropriate. To catch up on
// pre-existing events, set POSITIONS_START_FROM=FIRST env var (handled
// in main.go).
func New(cfg Config, h Handler, logger *zap.Logger, startFromFirst bool) *OrderEventsConsumer {
	if cfg.Topic == "" {
		cfg.Topic = "order.events"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "positions-order-events"
	}

	startOffset := kafka.LastOffset
	if startFromFirst {
		startOffset = kafka.FirstOffset
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: startOffset,
	})

	return &OrderEventsConsumer{
		reader:  reader,
		handler: h,
		logger:  logger,
	}
}

// Start blocks until ctx is cancelled. At-least-once delivery: commit
// AFTER Handle succeeds. On Handle error, no commit → message re-delivers
// on next poll or restart. Downstream Handle is expected to be idempotent
// (position_events UNIQUE constraint or state-machine dedup by
// (broker_order_id, event_seq)).
func (c *OrderEventsConsumer) Start(ctx context.Context) {
	c.logger.Info("order.events consumer started",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group_id", c.reader.Config().GroupID))
	defer c.logger.Info("order.events consumer stopped")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("order.events fetch error", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		ev, parseErr := parseEnvelope(msg.Value)
		if parseErr != nil {
			c.logger.Warn("order.events unmarshal failed — dropping",
				zap.Int64("offset", msg.Offset),
				zap.Int("partition", msg.Partition),
				zap.Error(parseErr))
			// Commit anyway — bad JSON isn't fixable by re-delivery. Alert
			// downstream (metrics or alarm topic) once monitoring lands.
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}
		ev.RawMessage = msg.Value

		if err := c.handler.Handle(ctx, ev); err != nil {
			c.logger.Warn("order.events handle failed — not committing offset (will re-deliver)",
				zap.String("event_id", ev.EventID),
				zap.String("broker_order_id", ev.BrokerOrderID),
				zap.Int64("offset", msg.Offset),
				zap.Error(err))
			// Backoff before next fetch so a persistent Handle failure
			// doesn't tightloop.
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			// Non-fatal — at-least-once means re-delivery is OK. Handle
			// is expected to be idempotent.
			c.logger.Warn("order.events commit failed",
				zap.Int64("offset", msg.Offset), zap.Error(err))
		}
	}
}

// Close stops the reader — call before shutdown to flush the group leave.
func (c *OrderEventsConsumer) Close() error {
	return c.reader.Close()
}

// parseEnvelope is exported-only-in-name — unit tests use it.
func parseEnvelope(b []byte) (*OrderEvent, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("empty message body")
	}
	var ev OrderEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	if ev.BrokerOrderID == "" {
		return nil, fmt.Errorf("envelope missing broker_order_id")
	}
	return &ev, nil
}
