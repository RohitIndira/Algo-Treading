package manthan

// TODO(orderstatus): this file moves to a shared internal/orderstatus/consumer
// package (or its own file in that folder) alongside the WSS listener when
// orderstatus svc's Kafka path is proven in prod. Today it lives under
// manthan/ because the wssBridge it feeds is a manthan-package type.

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// OrderEventsConsumer subscribes to the `order.events` Kafka topic produced
// by orderstatus svc and routes each observed broker event into the same
// in-process wssBridge that the (soon-retiring) WSS listener already feeds.
//
// From entry_handler / sl_handler's perspective nothing changes — they still
// register a channel via wssBridge.Register(brokerOrderID) and select on it.
// Only the SOURCE of events changes: instead of trade-execution's own WSS
// listener, it's orderstatus svc → Kafka → this consumer → wssBridge.
//
// Rollout is dual-path safe:
//   - WSS listener in trade-execution (existing) fires wssBridge.HandleUpdate
//   - This consumer (new) ALSO fires wssBridge.HandleUpdate
//   - Whichever arrives first pushes the update to entry_handler's channel;
//     the second call finds no registered channel (Unregister already ran
//     after processing) and no-ops via HandleUpdate's ok-check.
//
// Once orderstatus svc has proven itself, the in-process WSS listener gets
// deleted and this Kafka path becomes the only source.
type OrderEventsConsumer struct {
	reader *kafka.Reader
	bridge *WSSBridge
	logger *zap.Logger
}

// OrderEventsConsumerConfig holds Kafka wiring.
type OrderEventsConsumerConfig struct {
	KafkaBrokers []string
	Topic        string // default: order.events
	GroupID      string // default: trade-exec-order-events
}

// orderEventEnvelope is the JSON shape orderstatus svc's publisher produces
// on order.events. Field naming matches publisher.OrderEvent in the
// orderstatus binary — but we parse locally rather than import across the
// service boundary. The wire is the contract, not the Go type.
type orderEventEnvelope struct {
	EventID       string `json:"event_id"`
	EventType     string `json:"event_type"`
	EventSeq      int64  `json:"event_seq"`
	Source        string `json:"source"`
	ProducedAtMs  int64  `json:"produced_at_ms"`
	BrokerTsMs    int64  `json:"broker_ts_ms,omitempty"`

	BrokerOrderID   string `json:"broker_order_id"`
	ExchangeOrderID string `json:"exchange_order_id,omitempty"`
	UserID          string `json:"user_id"`
	Symbol          string `json:"symbol,omitempty"`
	Exchange        string `json:"exchange,omitempty"`
	BuySell         string `json:"buy_sell,omitempty"`
	OrderType       string `json:"order_type,omitempty"`
	Product         string `json:"product,omitempty"`

	Status        string `json:"status,omitempty"`
	OMSStatusCode int    `json:"oms_status_code,omitempty"`

	OrderPrice   float64 `json:"order_price,omitempty"`
	TriggerPrice float64 `json:"trigger_price,omitempty"`
	Quantity     int     `json:"quantity,omitempty"`
	FilledQty    int     `json:"filled_qty,omitempty"`
	TradedPrice  float64 `json:"traded_price,omitempty"`
	PendingQty   int     `json:"pending_qty,omitempty"`

	Reason string `json:"reason,omitempty"`
}

// NewOrderEventsConsumer wires the Kafka reader. Defaults topic +
// GroupID sensibly.
func NewOrderEventsConsumer(cfg OrderEventsConsumerConfig, bridge *WSSBridge, logger *zap.Logger) *OrderEventsConsumer {
	if cfg.Topic == "" {
		cfg.Topic = "order.events"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "trade-exec-order-events"
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset, // don't replay history on first boot
	})
	return &OrderEventsConsumer{
		reader: reader,
		bridge: bridge,
		logger: logger,
	}
}

// Start blocks until ctx is cancelled. Fetch → parse → HandleUpdate → commit.
//
// Delivery model is at-least-once (commit AFTER routing). Because
// HandleUpdate is a channel push and channels have Register/Unregister
// gating around them, a re-delivered event finds no listener and no-ops.
// The receiving handler (entry_handler / sl_handler) is the last defense —
// they already dedup on their own state.
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
			// Small backoff so we don't tightloop on a broken Kafka.
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		c.handleMessage(msg)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			// Non-fatal: at-least-once will re-deliver, and downstream
			// no-ops on the repeated push.
			c.logger.Warn("order.events commit failed",
				zap.Int64("offset", msg.Offset), zap.Error(err))
		}
	}
}

// Close stops the reader — call before shutdown to flush the group leave.
func (c *OrderEventsConsumer) Close() error {
	return c.reader.Close()
}

// handleMessage routes one Kafka message into the wssBridge. Any parse or
// routing error is logged but never aborts the loop — losing one event
// beats stalling every subsequent event behind a bad message.
//
// Fill-price policy (2026-07-15): orderstatus svc publishes to order.events
// from TWO sources — WSS (real-time push, has `traded_price`) and
// REST_ORDERBOOK (poll fallback, has `order_price` which for MARKET
// orders is 0 and for LIMIT orders is the LIMIT price, not the fill
// price). Propagating a REST fallback event with traded_price=0 into
// the wssBridge would race the WSS event (whichever arrives first
// "wins" at handleFill), corrupting downstream: services/positions
// rejects zero-price fills, SL calculation multiplies 0 × 0.80 = 0,
// P&L becomes noise. Verified 2026-07-15 (AADHARHFC id=1 filled_qty=38
// avg=0 — REST event arrived before WSS and was propagated blindly).
//
// Rule: broker is truth. WSS is the authoritative fill-price source.
// If we get a FILLED/PARTIAL event WITHOUT a real traded_price, we
// suppress it — the WSS event with the real price is either coming or
// already arrived. If BOTH sources fail to provide a real price, the
// reconciler (runs every 5min, does its own tradebook lookup) will
// eventually flip the row to FILLED with the correct avg. That's
// eventual consistency, safely — never write a made-up price.
func (c *OrderEventsConsumer) handleMessage(msg kafka.Message) {
	var env orderEventEnvelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		c.logger.Warn("order.events unmarshal failed — dropping",
			zap.Int64("offset", msg.Offset), zap.Error(err))
		return
	}
	if env.BrokerOrderID == "" {
		c.logger.Warn("order.events envelope missing broker_order_id — dropping",
			zap.String("event_id", env.EventID))
		return
	}

	// Guard: fill event without a real price. Skip propagation — do NOT
	// mark the row FILLED with a zero price. The paired WSS event (with
	// traded_price) either arrived already (and won the race) or is
	// coming; either way this REST fallback is redundant when zeroed out.
	// Non-fill events (CANCELLED / REJECTED / PENDING) pass through
	// unaffected — they don't carry price and downstream doesn't derive
	// anything price-dependent from them.
	if isFillEventType(env.EventType, env.Status) && env.TradedPrice <= 0 {
		c.logger.Info("order.events fill event has no traded_price — skipping (waiting for WSS event)",
			zap.String("event_id", env.EventID),
			zap.String("broker_order_id", env.BrokerOrderID),
			zap.String("source", env.Source),
			zap.String("status", env.Status),
			zap.Int("filled_qty", env.FilledQty))
		return
	}

	// Feed into the same bridge the in-process WSS listener uses. If the
	// order isn't a Manthan order (no Register call has landed for it),
	// HandleUpdate returns false — silent no-op.
	handled := c.bridge.HandleUpdate(
		env.BrokerOrderID,
		env.Status,
		env.FilledQty,
		env.TradedPrice, // avgFillPrice — guarded above to always be > 0 on FILLED events
		env.TriggerPrice,
		env.Reason,
	)

	c.logger.Debug("order.events routed",
		zap.String("event_id", env.EventID),
		zap.String("broker_order_id", env.BrokerOrderID),
		zap.String("status", env.Status),
		zap.String("source", env.Source),
		zap.Bool("bridge_handled", handled))
}

// isFillEventType returns true for events that MUST carry a real
// traded_price. Both event_type ("FILLED") and status ("EXECUTED" or
// terminal fill statuses) are checked because different orderstatus
// svc code paths populate different fields — WSS emits event_type,
// REST emits status directly.
func isFillEventType(eventType, status string) bool {
	if eventType == "FILLED" || eventType == "PARTIAL_FILLED" || eventType == "PARTIAL_FILL" {
		return true
	}
	// IsFilledWSStatus / IsPartialWSStatus live in wss_bridge.go —
	// same broker status enum we already use downstream.
	return IsFilledWSStatus(status) || IsPartialWSStatus(status)
}
