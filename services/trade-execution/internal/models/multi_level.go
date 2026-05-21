package models

import (
	"time"

	"github.com/google/uuid"
)

// MultiLevel exit status constants.
const (
	MLStatusPending    = "PENDING"    // Level configured, not yet active
	MLStatusActive     = "ACTIVE"     // TP limit order placed (live) / price monitor watching (SL/paper)
	MLStatusTriggered  = "TRIGGERED"  // Exit order filled
	MLStatusCancelled  = "CANCELLED"  // Cancelled before triggering (strategy deactivated/deleted)
	MLStatusSuperseded = "SUPERSEDED" // Position fully closed by an external exit (monitor SL/TP,
	//                                   square-off or force-exit) before this level could fire.
	//                                   Distinct from CANCELLED: the qty WAS exited, just not via
	//                                   this ladder level — see the PAPER_EXITED execution event.
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
	ExitQty       *int32     `db:"exit_qty"       json:"exit_qty"`       // Current effective qty (may be reduced by rebalancing)
	Status        string     `db:"status"         json:"status"`
	ExitOrderID   *uuid.UUID `db:"exit_order_id"  json:"exit_order_id"`  // Order placed for this exit
	BrokerOrderID *string    `db:"broker_order_id" json:"broker_order_id"`
	TriggeredAt   *time.Time `db:"triggered_at"   json:"triggered_at"`
	ExitPrice     *float64   `db:"exit_price"     json:"exit_price"`     // Actual fill price

	// Rebalancing audit — set when this level's qty is reduced after the opposite
	// side fires (e.g. SL L1 fires → TP L2/L3 qty reduced to fit remaining position).
	OriginalExitQty  *int32     `db:"original_exit_qty"  json:"original_exit_qty,omitempty"`  // Qty before any rebalancing
	RebalancedAt     *time.Time `db:"rebalanced_at"      json:"rebalanced_at,omitempty"`      // When rebalancing occurred
	RebalanceReason  *string    `db:"rebalance_reason"   json:"rebalance_reason,omitempty"`   // e.g. "SL_L1_TRIGGERED"

	CreatedAt     time.Time  `db:"created_at"     json:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"     json:"updated_at"`
}

func (MultiLevelExitRecord) TableName() string { return "multi_level_exit_levels" }
