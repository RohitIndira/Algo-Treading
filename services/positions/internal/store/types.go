// Package store owns positions_db writes for positions svc.
//
// All writes are transactional. Idempotency is enforced at the SQL layer:
//
//	positions:       UNIQUE partial index on entry_broker_order_id (BUY dedup)
//	position_events: UNIQUE (position_id, source_event_id)         (all events)
//
// Callers of the state machine (Chunk P.C) invoke the "WithEvent" variants
// which INSERT the position/UPDATE + INSERT the audit row in the same tx.
package store

import (
	"time"

	"github.com/google/uuid"
)

// Origin — matches the CHECK constraint on positions.origin.
const (
	OriginManthan    = "MANTHAN"
	OriginUserManual = "USER_MANUAL"
)

// Status — matches the CHECK constraint on positions.status.
const (
	StatusActive = "ACTIVE"
	StatusExited = "EXITED"
)

// ExitReason — matches the CHECK constraint on positions.exit_reason.
const (
	ExitReasonSLTrigger    = "SL_TRIGGER"
	ExitReasonManualExit   = "MANUAL_EXIT"
	ExitReasonStrategyExit = "STRATEGY_EXIT"
	ExitReasonLiquidation  = "LIQUIDATION"
)

// PositionEventType — matches the loose set of strings expected in
// position_events.event_type. Not a DB CHECK, but a source-of-truth constant.
const (
	EventTypeEntryFilled        = "ENTRY_FILLED"
	EventTypeUserManualEntry    = "USER_MANUAL_ENTRY"
	EventTypeSLModified         = "SL_MODIFIED"  // trigger set/updated (any observation with a real trigger_price)
	EventTypeSLCancelled        = "SL_CANCELLED" // SL order cancelled without firing — current_sl cleared to NULL
	EventTypeSLFilled           = "SL_FILLED"
	EventTypeManualSellApplied  = "MANUAL_SELL_APPLIED"
	EventTypeLiquidation        = "LIQUIDATION"
	EventTypeReconcilerDriftFix = "RECONCILER_DRIFT_FIX"
)

// Position is the in-Go shape of a positions row. Field types match the
// SQL column types 1:1 so scans + inserts don't need converters.
//
// Time.IsZero() is used to mean "NULL" for exit_time only (schema-level
// CHECK enforces exit_time IS NULL ⇔ status='ACTIVE').
type Position struct {
	PositionID uuid.UUID

	Origin     string
	UserID     string
	StrategyID string // "" for USER_MANUAL
	SignalID   string // "" for USER_MANUAL
	Symbol     string
	Exchange   string

	Status         string
	EntryPrice     float64
	EntryTime      time.Time
	Quantity       int // current net qty; drops on partial exits
	InvestedAmount float64

	ExitPrice   float64
	ExitTime    time.Time
	ExitReason  string
	RealizedPnL float64

	EntryBrokerOrderID string
	ExitBrokerOrderID  string

	CurrentSL       float64
	HighSinceEntry  float64
	LastTrailLevel  float64
}

// PositionEvent is one audit row. `SourceEventID` is the idempotency key —
// per (position_id, source_event_id) UNIQUE index. Same event replayed via
// Kafka → INSERT no-ops via ON CONFLICT DO NOTHING.
type PositionEvent struct {
	PositionID        uuid.UUID
	EventType         string
	BrokerOrderID     string
	SignalID          string
	DeltaQty          int // positive on entry, negative on exit
	FillPrice         float64
	RealizedPnLDelta  float64
	Reason            string

	RawSourceEvent []byte // full order.events envelope JSON
	SourceTopic    string // "order.events" for the P.C hot path
	SourceOffset   int64
	SourceEventID  string // idempotency key
}
