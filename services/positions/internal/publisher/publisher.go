// Package publisher owns the position.events Kafka fan-out for positions svc.
//
// After every successful state-machine mutation (BUY insert, SELL close,
// SELL partial), the handler calls Publisher.Publish so downstream services
// see position lifecycle in real time:
//
//	rules-engine     — INSERT manthan_cooldown on POSITION_EXITED
//	                    with origin=MANTHAN and exit_reason=SL_TRIGGER
//	api-gateway      — push to /ws/live-orders for the user
//	notification svc — future; user-facing "position exited" messages
//
// Partition key = position_id per §5.2 of docs/positions_service_design.md.
// All events for one position land on the same partition = strict lifecycle
// ordering.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// EventType — the values published on position.events. Matches the enum
// captured in §5.2 of the design doc.
const (
	EventPositionOpened = "POSITION_OPENED"
	EventPositionExited = "POSITION_EXITED"
	EventSLTrailed      = "SL_TRAILED"
	EventManualInterrupt = "MANUAL_INTERRUPT" // reserved for future partial user activity
)

// Action — coarse-grained "what happened" tag inside an event. The specific
// state-transition semantics live in the exit_reason / event_type combo;
// action is a convenience for consumers that don't want to fold enums.
const (
	ActionEntry  = "ENTRY"
	ActionExit   = "EXIT"
	ActionTrail  = "TRAIL"
	ActionTopUp  = "TOP_UP"
)

// PositionEvent is the JSON envelope produced on Kafka topic position.events.
// Matches §5.2 of the design doc.
type PositionEvent struct {
	EventID      string `json:"event_id"`
	EventType    string `json:"event_type"`
	ProducedAtMs int64  `json:"produced_at_ms"`

	PositionID string `json:"position_id"`
	Origin     string `json:"origin"`               // MANTHAN | USER_MANUAL
	UserID     string `json:"user_id"`
	StrategyID string `json:"strategy_id,omitempty"` // NULL/"" for USER_MANUAL
	SignalID   string `json:"signal_id,omitempty"`   // NULL/"" for USER_MANUAL
	Symbol     string `json:"symbol"`

	Action   string  `json:"action"`
	Price    float64 `json:"price,omitempty"`
	Quantity int     `json:"quantity,omitempty"`

	// Only populated on POSITION_EXITED
	ExitReason  string  `json:"exit_reason,omitempty"`
	RealizedPnL float64 `json:"realized_pnl,omitempty"`

	// Broker linkage (helps consumers correlate with orderstatus svc's
	// broker_events and trade-execution's manthan_orders)
	BrokerOrderID string `json:"broker_order_id,omitempty"`
}

// Publisher wraps a *kafka.Writer configured for position.events.
type Publisher struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewPublisher wraps an existing *kafka.Writer whose Topic is position.events.
// main.go owns the writer's lifecycle. Passing nil returns a nil publisher —
// legal, used by tests and by boot paths that want to skip Kafka fan-out.
func NewPublisher(w *kafka.Writer, logger *zap.Logger) *Publisher {
	if w == nil {
		return nil
	}
	return &Publisher{writer: w, logger: logger}
}

// Publish serialises to JSON and produces to Kafka with position_id as the
// partition key. Best-effort — errors are logged, not returned. The state
// machine has already committed the DB write; blocking the consumer offset
// commit on Kafka health would just cause replay storms.
//
// If durability > availability becomes important later, add an outbox
// table + background retryer.
func (p *Publisher) Publish(ctx context.Context, ev *PositionEvent) {
	if p == nil || p.writer == nil || ev == nil {
		return
	}

	if ev.EventID == "" {
		// Derive a stable idempotency key: <position_id>-<produced_at_ms>
		// so re-publishes for the same position at the same instant dedup.
		ev.EventID = fmt.Sprintf("%s-%d", ev.PositionID, ev.ProducedAtMs)
	}
	if ev.ProducedAtMs == 0 {
		ev.ProducedAtMs = time.Now().UnixMilli()
	}

	body, err := json.Marshal(ev)
	if err != nil {
		p.logger.Warn("position.events marshal failed",
			zap.String("position_id", ev.PositionID),
			zap.Error(err))
		return
	}

	msg := kafka.Message{
		Key:   []byte(ev.PositionID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(ev.EventType)},
			{Key: "origin", Value: []byte(ev.Origin)},
			{Key: "user_id", Value: []byte(ev.UserID)},
			{Key: "symbol", Value: []byte(ev.Symbol)},
			{Key: "position_id", Value: []byte(ev.PositionID)},
			{Key: "action", Value: []byte(ev.Action)},
		},
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Warn("position.events publish failed — DB state is safe, downstream will lag until reconcile",
			zap.String("position_id", ev.PositionID),
			zap.String("event_type", ev.EventType),
			zap.Error(err))
		return
	}
	p.logger.Debug("position.events published",
		zap.String("position_id", ev.PositionID),
		zap.String("event_type", ev.EventType),
		zap.String("action", ev.Action))
}

// Ensure the uuid import is used somewhere — position_id UUIDs go on the
// wire as strings via the field above. Import kept for future typed helpers.
var _ = uuid.UUID{}
