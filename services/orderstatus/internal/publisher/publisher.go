// Package publisher owns Kafka fan-out for orderstatus svc.
//
// Every successful broker_events INSERT triggers ONE order.events publish.
// The Kafka message body is the same JSON shape downstream consumers
// (positions svc, trade-execution's wait-for-fill, api-gateway's live-orders
// push) parse.
//
// Partition key = broker_order_id per docs/orderstatus_service_design.md §5.1:
// all events for one order land on the same partition = strict per-order
// ordering. Cross-order ordering isn't needed by any consumer.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

// OrderEvent is the JSON envelope on Kafka topic order.events.
// Field naming matches §5.2 of the design doc so consumers can parse
// without version-guessing.
type OrderEvent struct {
	// Envelope
	EventID       string `json:"event_id"`               // uuid-like unique per publish (for consumer dedup)
	EventType     string `json:"event_type"`             // FILLED / STATUS_CHANGED / CANCELLED / REJECTED / MODIFIED / TRIGGERED / EXPIRED / PARTIALLY_FILLED / PLACED
	EventSeq      int64  `json:"event_seq"`              // deterministic across paths — used for consumer dedup within a broker_order_id
	Source        string `json:"source"`                 // WSS / REST_ORDERBOOK / REST_RECONCILER / REST_TRADEBOOK
	ProducedAtMs  int64  `json:"produced_at_ms"`         // orderstatus svc wall clock at publish
	BrokerTsMs    int64  `json:"broker_ts_ms,omitempty"` // broker's own timestamp if available

	// Order identity (join keys)
	BrokerOrderID   string `json:"broker_order_id"`
	ExchangeOrderID string `json:"exchange_order_id,omitempty"`
	UserID          string `json:"user_id"`
	Symbol          string `json:"symbol,omitempty"`
	Exchange        string `json:"exchange,omitempty"`
	BuySell         string `json:"buy_sell,omitempty"`
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
}

// FromStoreEvent projects a persisted store.Event into the wire envelope.
// EventID is derived from (broker_order_id, event_seq) so re-publishing the
// same event never fabricates a different EventID — consumer dedup stays
// idempotent.
func FromStoreEvent(ev *store.Event) OrderEvent {
	return OrderEvent{
		EventID:         fmt.Sprintf("%s-%d", ev.BrokerOrderID, ev.EventSeq),
		EventType:       string(ev.EventType),
		EventSeq:        ev.EventSeq,
		Source:          string(ev.Source),
		ProducedAtMs:    time.Now().UnixMilli(),
		BrokerTsMs:      ev.BrokerTsMs,
		BrokerOrderID:   ev.BrokerOrderID,
		ExchangeOrderID: ev.ExchangeOrderID,
		UserID:          ev.UserID,
		Symbol:          ev.Symbol,
		Exchange:        ev.Exchange,
		BuySell:         ev.BuySell,
		OrderType:       ev.OrderType,
		Product:         ev.Product,
		Status:          ev.Status,
		OMSStatusCode:   ev.OMSStatusCode,
		OrderPrice:      ev.OrderPrice,
		TriggerPrice:    ev.TriggerPrice,
		Quantity:        ev.Quantity,
		FilledQty:       ev.FilledQty,
		TradedPrice:     ev.TradedPrice,
		PendingQty:      ev.PendingQty,
		Reason:          ev.Reason,
	}
}

// Publisher publishes OrderEvents to Kafka topic order.events.
type Publisher struct {
	writer *kafka.Writer
	logger *zap.Logger

	// workerStore is the broker_events writer the durability worker scans for
	// unpublished rows. Set by RunWorker; nil on the inline-only publish path.
	workerStore *store.Writer
}

// NewPublisher wraps an existing *kafka.Writer configured for the
// order.events topic. main.go owns the writer's lifecycle.
func NewPublisher(w *kafka.Writer, logger *zap.Logger) *Publisher {
	return &Publisher{writer: w, logger: logger}
}

// Publish serializes ev to JSON and produces to Kafka with broker_order_id
// as the partition key. Returns true iff the message was accepted by Kafka.
//
// Best-effort on the caller's timeline: a false return is logged, not fatal,
// because losing a single inline publish is less bad than blocking WSS event
// processing behind a Kafka outage. Durability is the worker's job — the
// caller stamps broker_events.published_at only on a true return, so a false
// leaves the row for RunWorker to retry.
func (p *Publisher) Publish(ctx context.Context, ev *store.Event) bool {
	if p == nil || p.writer == nil || ev == nil {
		return false
	}
	envelope := FromStoreEvent(ev)
	body, err := json.Marshal(envelope)
	if err != nil {
		p.logger.Warn("order.events marshal failed",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.Error(err))
		return false
	}

	msg := kafka.Message{
		Key:   []byte(ev.BrokerOrderID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(envelope.EventType)},
			{Key: "source", Value: []byte(envelope.Source)},
			{Key: "user_id", Value: []byte(envelope.UserID)},
			{Key: "broker_order_id", Value: []byte(envelope.BrokerOrderID)},
			{Key: "event_seq", Value: []byte(fmt.Sprintf("%d", envelope.EventSeq))},
		},
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Warn("order.events publish failed — event is safe in broker_events, worker will retry",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.Int64("event_seq", envelope.EventSeq),
			zap.Error(err))
		return false
	}
	p.logger.Debug("order.events published",
		zap.String("broker_order_id", envelope.BrokerOrderID),
		zap.String("event_type", envelope.EventType),
		zap.Int64("event_seq", envelope.EventSeq))
	return true
}

// durabilityGracePeriod is the single source of truth for how long the inline
// publish path exclusively owns a freshly-inserted broker_events row. The
// worker ignores rows younger than this. Tune here.
const durabilityGracePeriod = 5 * time.Second

// RunWorker is the durability backstop for broker_events → order.events.
// Invariant: inline publishers own rows for the first durabilityGracePeriod
// after INSERT. The worker only touches rows that have been published_at IS
// NULL for longer than that — meaning the inline path genuinely failed (Kafka
// unreachable, marshal error, etc.). There is NO window where both the inline
// path and the worker publish the same row.
//
// Every `interval`, it republishes those genuinely-failed rows and stamps them
// once Kafka confirms. Blocks until ctx is cancelled.
//
// Ordering: FetchUnpublished returns rows in id order, so per-broker_order_id
// FIFO is preserved. On the FIRST publish failure in a drain pass we stop and
// retry the whole pass next tick — this keeps a stuck row from letting later
// rows for the same order jump ahead of it.
func (p *Publisher) RunWorker(ctx context.Context, w *store.Writer, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	p.workerStore = w
	p.logger.Info("order.events durability worker started",
		zap.Duration("interval", interval))
	defer p.logger.Info("order.events durability worker stopped")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.drainOnce(ctx)
		}
	}
}

// drainOnce publishes as many pending rows as it can in id order, stopping at
// the first failure. Uses p.workerStore (set by RunWorker) — kept as a field so
// the signature stays clean.
func (p *Publisher) drainOnce(ctx context.Context) {
	if p.workerStore == nil {
		return
	}
	rows, err := p.workerStore.FetchUnpublished(ctx, 500, durabilityGracePeriod.Seconds())
	if err != nil {
		p.logger.Warn("durability worker: fetch unpublished failed", zap.Error(err))
		return
	}
	if len(rows) == 0 {
		return
	}
	published := 0
	for _, r := range rows {
		if !p.Publish(ctx, &r.Event) {
			// Kafka still unhappy — stop; retry the rest next tick so ordering
			// per broker_order_id is preserved.
			break
		}
		if err := p.workerStore.MarkPublished(ctx, r.ID); err != nil {
			p.logger.Warn("durability worker: mark published failed — will retry",
				zap.Int64("id", r.ID), zap.Error(err))
			break
		}
		published++
	}
	if published > 0 {
		p.logger.Info("durability worker: republished pending events",
			zap.Int("count", published),
			zap.Int("pending_this_pass", len(rows)))
	}
}
