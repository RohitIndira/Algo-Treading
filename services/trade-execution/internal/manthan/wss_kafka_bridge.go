package manthan

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// WSSKafkaBridge publishes WSS fill events to Kafka `order.events` topic
// so positions svc gets the authoritative `traded_price` at fill time —
// closing the race documented in the 2026-07-17 postmortem:
//
// Before this bridge:
//   1. Broker fills → in-process WSS handler writes manthan_orders.avg_fill_price
//   2. Orderstatus svc's REST poll (15s cadence) → publishes order.events
//      with source=REST_ORDERBOOK and traded_price=NULL (Indira OrderBook
//      API doesn't carry avg fill for LIMIT orders)
//   3. Positions svc consumes → traded_price=null → calls LookupOrderMeta
//      → RACES against step 1's DB write. Fast fills lose the race and
//      the position defers forever with no retry.
//
// After this bridge:
//   1. Broker fills → in-process WSS handler writes manthan_orders +
//      ALSO publishes an order.events message with source=WSS_MANTHAN
//      and traded_price=avgPrice — the SSOT value
//   2. Positions svc consumes → traded_price>0 → inserts position with
//      the real broker fill price. No gRPC round-trip. No race.
//
// The REST_ORDERBOOK path still runs; it acts as a redundant safety net
// (dedupe at positions svc idempotency layer).
type WSSKafkaBridge struct {
	writer *kafka.Writer
	repo   *Repository
	logger *zap.Logger
}

// NewWSSKafkaBridge builds an async Kafka writer for order.events. Async
// because the WSS goroutine must not block on Kafka round-trips — failed
// writes are logged via Completion callback (positions svc still gets a
// backup event via REST_ORDERBOOK when this async path fails).
func NewWSSKafkaBridge(brokers []string, repo *Repository, logger *zap.Logger) *WSSKafkaBridge {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "order.events",
		Balancer:     &kafka.Hash{}, // partition-by-key = broker_order_id — preserves per-order ordering
		BatchSize:    10,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				logger.Error("WSS→order.events async write failed",
					zap.Int("messages", len(messages)),
					zap.Error(err))
			}
		},
	}
	return &WSSKafkaBridge{writer: writer, repo: repo, logger: logger}
}

// Close flushes any pending async messages.
func (b *WSSKafkaBridge) Close() error {
	if b == nil || b.writer == nil {
		return nil
	}
	return b.writer.Close()
}

// (This bridge reuses the orderEventEnvelope defined in
// order_events_consumer.go — same package, same wire contract.)

// PublishFill emits an order.events message for a WSS-observed fill.
// Called from the statusSvc.SetManthanBridge callback after a successful
// bridge.HandleUpdate route.
//
// Only publishes when there's actually a fill to report (avgPrice>0 AND
// filledQty>0). Non-fill status transitions (PENDING, OPEN, CANCELLED
// without partial fill) fall through — the REST_ORDERBOOK path already
// carries those to positions svc.
//
// Errors are logged and swallowed — the WSS hot path must not block on
// Kafka or repo lookup failures.
func (b *WSSKafkaBridge) PublishFill(
	ctx context.Context,
	brokerOrderID, status string,
	filledQty int,
	avgPrice, triggerPrice float64,
	reason string,
) {
	if b == nil || b.writer == nil || b.repo == nil {
		return
	}
	if avgPrice <= 0 || filledQty <= 0 {
		return
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	oc, err := b.repo.GetOrderContext(lookupCtx, brokerOrderID)
	if err != nil {
		b.logger.Warn("wss→order.events: GetOrderContext failed",
			zap.String("broker_order_id", brokerOrderID), zap.Error(err))
		return
	}
	if oc == nil {
		// Not a Manthan order — nothing to publish. Should never happen
		// on this callback path since HandleUpdate already checked the
		// pending map, but the belt-and-braces guard is cheap.
		return
	}

	nowMs := time.Now().UnixMilli()
	eventType := "FILLED"
	if oc.Qty > 0 && filledQty < oc.Qty {
		eventType = "PARTIAL_FILLED"
	}

	env := orderEventEnvelope{
		EventID:       fmt.Sprintf("%s-wss-%d-%d", brokerOrderID, filledQty, nowMs),
		EventType:     eventType,
		EventSeq:      deriveWSSEventSeq(brokerOrderID, filledQty, status),
		Source:        "WSS_MANTHAN",
		ProducedAtMs:  nowMs,
		BrokerOrderID: brokerOrderID,
		UserID:        oc.UserID,
		Symbol:        oc.IndiraSymbol,
		Exchange:      oc.Exchange,
		BuySell:       oc.OrderSide,
		OrderType:     string(oc.OrderType),
		Status:        strings.ToUpper(strings.TrimSpace(status)),
		TradedPrice:   avgPrice,
		TriggerPrice:  triggerPrice,
		Quantity:      oc.Qty,
		FilledQty:     filledQty,
		Reason:        reason,
	}

	payload, err := json.Marshal(env)
	if err != nil {
		b.logger.Error("wss→order.events: marshal failed",
			zap.String("broker_order_id", brokerOrderID), zap.Error(err))
		return
	}

	if err := b.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(brokerOrderID),
		Value: payload,
	}); err != nil {
		// Async writer only surfaces synchronous errors (e.g. context cancel).
		// Real send errors go through the Completion callback.
		b.logger.Warn("wss→order.events: WriteMessages returned",
			zap.String("broker_order_id", brokerOrderID), zap.Error(err))
		return
	}

	b.logger.Info("wss→order.events published",
		zap.String("broker_order_id", brokerOrderID),
		zap.String("user_id", oc.UserID),
		zap.String("symbol", oc.IndiraSymbol),
		zap.Float64("traded_price", avgPrice),
		zap.Int("filled_qty", filledQty),
		zap.String("event_type", eventType))
}

// deriveWSSEventSeq synthesizes a monotonic-ish sequence key for
// idempotency. Positions svc's UNIQUE(position_id, source_event_id)
// dedupes on event_id, but event_seq is still used for downstream
// ordering decisions in some paths. We hash the fill fingerprint so a
// WSS repost of the same fill collides with itself.
//
// This isn't guaranteed monotonic across broker_order_ids (nor does it
// need to be — the fill has already happened at the broker).
func deriveWSSEventSeq(brokerOrderID string, filledQty int, status string) int64 {
	// Simple, deterministic combination — good enough for dedup.
	// FNV would be preferable but stdlib hash/fnv adds an import for
	// no material benefit at fill event rate.
	var h int64
	for _, c := range brokerOrderID {
		h = h*31 + int64(c)
	}
	for _, c := range status {
		h = h*31 + int64(c)
	}
	return h*1000 + int64(filledQty)
}
