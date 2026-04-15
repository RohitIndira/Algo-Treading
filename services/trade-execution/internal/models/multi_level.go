package models

import (
	"time"

	"github.com/google/uuid"
)

// MultiLevel exit status constants.
const (
	MLStatusPending   = "PENDING"   // Level configured, not yet active
	MLStatusActive    = "ACTIVE"    // TP limit order placed (live) / price monitor watching (SL/paper)
	MLStatusTriggered = "TRIGGERED" // Exit order filled
	MLStatusCancelled = "CANCELLED" // Cancelled before triggering
)

// MultiLevel exit type constants.
const (
	MLExitTypeSL = "SL"
	MLExitTypeTP = "TP"
)

// MultiLevelExitRecord is the DB model for one exit level row in
// multi_level_exit_levels. One row per level per entry order.
type MultiLevelExitRecord struct {
	ID            int        `db:"id"             json:"id"`
	EntryOrderID  uuid.UUID  `db:"entry_order_id" json:"entry_order_id"`
	ExitType      string     `db:"exit_type"      json:"exit_type"`      // "SL" or "TP"
	LevelNum      int        `db:"level_num"      json:"level_num"`      // 1..5
	PricePct      float64    `db:"price_pct"      json:"price_pct"`      // % from entry (always positive)
	QtyPct        float64    `db:"qty_pct"        json:"qty_pct"`        // % of total qty
	TriggerPrice  *float64   `db:"trigger_price"  json:"trigger_price"`  // Absolute price, set after entry fills
	ExitQty       *int32     `db:"exit_qty"       json:"exit_qty"`       // Absolute qty, set after entry fills
	Status        string     `db:"status"         json:"status"`
	ExitOrderID   *uuid.UUID `db:"exit_order_id"  json:"exit_order_id"`  // Order placed for this exit
	BrokerOrderID *string    `db:"broker_order_id" json:"broker_order_id"`
	TriggeredAt   *time.Time `db:"triggered_at"   json:"triggered_at"`
	ExitPrice     *float64   `db:"exit_price"     json:"exit_price"`    // Actual fill price
	CreatedAt     time.Time  `db:"created_at"     json:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"     json:"updated_at"`
}

func (MultiLevelExitRecord) TableName() string { return "multi_level_exit_levels" }
