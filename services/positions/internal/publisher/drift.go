package publisher

// DriftPublisher owns the positions.drift.detected Kafka fan-out. Emits one
// envelope per (user_id, symbol) where the reconciler found a mismatch
// between the broker's holdings and positions_db.positions.
//
// CRITICAL — per docs/positions_service_design.md §11 Q2 and the 2026-06-12
// S4450 liquidation incident (memory: project_manthan_safety_monitor_liquidation_hazard):
// this publisher NEVER triggers auto-fix. It only NOTIFIES. Downstream:
//
//	notification svc  — page/slack the desk on any drift
//	ops dashboard     — surface counts for the human to reconcile
//	analytics         — track drift rate over time
//
// Partition key = user_id + ":" + symbol so all drift events for the same
// pair land on the same partition (stable ordering for consumers).

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// DriftType — the three shapes of drift the reconciler can surface.
const (
	DriftQtyMismatch = "QTY_MISMATCH" // both sides have the symbol; qty differs
	DriftBrokerOnly  = "BROKER_ONLY"  // broker has it, we don't (untracked buy)
	DriftDBOnly      = "DB_ONLY"      // we have ACTIVE, broker doesn't (phantom)
)

// DriftEvent is the JSON envelope produced on Kafka topic positions.drift.detected.
// One event per (user_id, symbol) drift observation per reconciler sweep.
type DriftEvent struct {
	EventID       string `json:"event_id"`
	DetectedAtMs  int64  `json:"detected_at_ms"`

	UserID    string `json:"user_id"`
	Symbol    string `json:"symbol"`
	DriftType string `json:"drift_type"`

	BrokerQty int `json:"broker_qty"`
	OurQty    int `json:"our_qty"`

	// PositionIDs are the ACTIVE lots on our side that contribute to OurQty.
	// Empty for BROKER_ONLY drifts. Callers use these to walk audit history.
	PositionIDs []string `json:"position_ids,omitempty"`

	// OriginBreakdown gives a per-origin split when both MANTHAN and
	// USER_MANUAL lots contribute — helps ops decide who owns the drift.
	// Omitted when only one origin is present.
	ManthanQty    int `json:"manthan_qty,omitempty"`
	UserManualQty int `json:"user_manual_qty,omitempty"`
}

// DriftPublisher wraps a *kafka.Writer configured for positions.drift.detected.
type DriftPublisher struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewDriftPublisher wraps an existing *kafka.Writer whose Topic is
// positions.drift.detected. main.go owns the writer lifecycle. Nil-tolerant
// so tests and boot paths that disable the reconciler can pass nil.
func NewDriftPublisher(w *kafka.Writer, logger *zap.Logger) *DriftPublisher {
	if w == nil {
		return nil
	}
	return &DriftPublisher{writer: w, logger: logger}
}

// PublishDrift serialises + produces one drift event. Best-effort: errors
// are logged, not returned — dropping a Kafka publish here means the drift
// is undetected downstream *this cycle only*, since the next reconciler
// sweep will re-observe the same drift and republish (idempotent by EventID).
func (p *DriftPublisher) PublishDrift(ctx context.Context, ev *DriftEvent) {
	if p == nil || p.writer == nil || ev == nil {
		return
	}

	if ev.DetectedAtMs == 0 {
		ev.DetectedAtMs = time.Now().UnixMilli()
	}
	if ev.EventID == "" {
		// Idempotency key: consumers can dedup identical drifts across
		// sweeps (same user+symbol+type+qty within the same second).
		ev.EventID = fmt.Sprintf("drift-%s-%s-%s-%d-%d",
			ev.UserID, ev.Symbol, ev.DriftType, ev.BrokerQty, ev.OurQty)
	}

	body, err := json.Marshal(ev)
	if err != nil {
		p.logger.Warn("drift marshal failed",
			zap.String("user_id", ev.UserID),
			zap.String("symbol", ev.Symbol),
			zap.Error(err))
		return
	}

	key := ev.UserID + ":" + ev.Symbol
	msg := kafka.Message{
		Key:   []byte(key),
		Value: body,
		Headers: []kafka.Header{
			{Key: "drift_type", Value: []byte(ev.DriftType)},
			{Key: "user_id", Value: []byte(ev.UserID)},
			{Key: "symbol", Value: []byte(ev.Symbol)},
		},
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Warn("drift publish failed — next reconciler sweep will retry",
			zap.String("user_id", ev.UserID),
			zap.String("symbol", ev.Symbol),
			zap.String("drift_type", ev.DriftType),
			zap.Error(err))
		return
	}
	p.logger.Warn("DRIFT DETECTED",
		zap.String("user_id", ev.UserID),
		zap.String("symbol", ev.Symbol),
		zap.String("drift_type", ev.DriftType),
		zap.Int("broker_qty", ev.BrokerQty),
		zap.Int("our_qty", ev.OurQty))
}
