package oco

import (
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/google/uuid"
)

// OCOState represents the lifecycle state of an OCO group.
type OCOState string

const (
	// StatePendingEntry — entry SL order placed, waiting for broker fill.
	StatePendingEntry OCOState = "PENDING_ENTRY"
	// StatePlacingLegs — entry filled, SL+TP legs being placed.
	StatePlacingLegs OCOState = "PLACING_LEGS"
	// StateActive — both SL and TP legs placed and open at broker.
	StateActive OCOState = "ACTIVE"
	// StateSLTriggered — SL leg executed, cancelling TP.
	StateSLTriggered OCOState = "SL_TRIGGERED"
	// StateTPTriggered — TP leg executed, cancelling SL.
	StateTPTriggered OCOState = "TP_TRIGGERED"
	// StateCompleted — one leg filled, other cancelled. Terminal.
	StateCompleted OCOState = "COMPLETED"
	// StateFailed — unrecoverable error. Terminal.
	StateFailed OCOState = "FAILED"
	// StateCancelled — user or system cancelled the OCO group. Terminal.
	StateCancelled OCOState = "CANCELLED"
)

// IsTerminal returns true if the OCO group is in a terminal state.
func (s OCOState) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCancelled:
		return true
	}
	return false
}

// OCORole identifies a single order's role within the OCO group.
type OCORole string

const (
	RoleEntry  OCORole = "ENTRY"
	RoleSLLeg  OCORole = "SL_LEG"
	RoleTPLeg  OCORole = "TP_LEG"
)

// OCOGroup tracks the full state of one OCO trade (entry + SL leg + TP leg).
//
// All fields are protected by the OCOManager's per-group locking. Do NOT share
// an OCOGroup pointer across goroutines without synchronization.
type OCOGroup struct {
	// ── Identity ────────────────────────────────────────────────────────────
	GroupID uuid.UUID `json:"group_id"`
	UserID  string    `json:"user_id"`

	// ── Entry Order ─────────────────────────────────────────────────────────
	EntryOrderID   uuid.UUID `json:"entry_order_id"`
	EntryBrokerID  string    `json:"entry_broker_id,omitempty"` // Indira order ID
	EntryFillPrice float64   `json:"entry_fill_price,omitempty"`

	// ── Stop-Loss Leg ───────────────────────────────────────────────────────
	SLOrderID      uuid.UUID `json:"sl_order_id,omitempty"`
	SLBrokerID     string    `json:"sl_broker_id,omitempty"`
	SLTriggerPrice float64   `json:"sl_trigger_price,omitempty"`
	SLLimitPrice   float64   `json:"sl_limit_price,omitempty"`

	// ── Take-Profit Leg ─────────────────────────────────────────────────────
	TPOrderID    uuid.UUID `json:"tp_order_id,omitempty"`
	TPBrokerID   string    `json:"tp_broker_id,omitempty"`
	TPLimitPrice float64   `json:"tp_limit_price,omitempty"`

	// ── Configuration (set once at creation) ────────────────────────────────
	SLPercent     float64 `json:"sl_percent"`      // e.g. 2.0 for 2%
	TPPercent     float64 `json:"tp_percent"`      // e.g. 3.0 for 3%
	TrailingSL    bool    `json:"trailing_sl"`     // true = enable trailing
	TrailingSLPct float64 `json:"trailing_sl_pct"` // trailing percentage (often same as SLPercent)

	// ── State ───────────────────────────────────────────────────────────────
	State        OCOState `json:"state"`
	HighestPrice float64  `json:"highest_price,omitempty"` // for trailing SL
	PnL          float64  `json:"pnl,omitempty"`           // realized P&L when completed

	// ── Stock Info (needed for leg placement & trailing) ────────────────────
	Symbol    string `json:"symbol"`
	Exchange  string `json:"exchange"`
	StockCode int64  `json:"stock_code"`
	Quantity  int32  `json:"quantity"`
	OrderSide string `json:"order_side"` // BUY or SELL (entry side)

	// ── Auth (for broker API calls) ─────────────────────────────────────────
	Auth *indiraClient.AuthContext `json:"-"`

	// ── Product/validity ────────────────────────────────────────────────────
	ProductType string `json:"product_type"` // INTRADAY, DELIVERY, etc.
	Validity    string `json:"validity"`     // DAY

	// ── Strategy context ────────────────────────────────────────────────────
	StrategyID string `json:"strategy_id,omitempty"`
	EventID    uuid.UUID `json:"event_id,omitempty"`

	// ── Timestamps ──────────────────────────────────────────────────────────
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ExitSide returns the opposite side of the entry. Entry BUY → legs SELL.
func (g *OCOGroup) ExitSide() string {
	if g.OrderSide == "BUY" {
		return "SELL"
	}
	return "BUY"
}

// CalculateSLFromFill computes SL trigger & limit prices from the actual fill.
//
//	BUY entry → SL trigger = fillPrice * (1 - slPct/100)
//	            SL limit   = trigger * (1 - 0.005)   (0.5% below to ensure fill)
//	SELL entry → mirror
func (g *OCOGroup) CalculateSLFromFill(fillPrice float64) (trigger, limit float64) {
	if g.OrderSide == "BUY" {
		trigger = fillPrice * (1 - g.SLPercent/100)
		limit = trigger * (1 - 0.005) // 0.5% below trigger
	} else {
		trigger = fillPrice * (1 + g.SLPercent/100)
		limit = trigger * (1 + 0.005) // 0.5% above trigger
	}
	return roundNSE(trigger), roundNSE(limit)
}

// CalculateTPFromFill computes TP limit price from the actual fill.
//
//	BUY entry → TP limit = fillPrice * (1 + tpPct/100)
//	SELL entry → TP limit = fillPrice * (1 - tpPct/100)
func (g *OCOGroup) CalculateTPFromFill(fillPrice float64) float64 {
	if g.OrderSide == "BUY" {
		return roundNSE(fillPrice * (1 + g.TPPercent/100))
	}
	return roundNSE(fillPrice * (1 - g.TPPercent/100))
}

// CalculateTrailingSL computes a new SL trigger given the current highest price.
// Only moves SL in the favorable direction. Returns (trigger, limit, shouldUpdate).
func (g *OCOGroup) CalculateTrailingSL(currentLTP float64) (trigger, limit float64, shouldUpdate bool) {
	if !g.TrailingSL || g.State != StateActive {
		return 0, 0, false
	}

	pct := g.TrailingSLPct
	if pct <= 0 {
		pct = g.SLPercent // default to SL percent if trailing pct not set
	}

	if g.OrderSide == "BUY" {
		// Trail upward: if LTP made new high, move SL up
		if currentLTP <= g.HighestPrice {
			return 0, 0, false
		}
		trigger = currentLTP * (1 - pct/100)
		limit = trigger * (1 - 0.005)
		// Only update if new trigger is higher than current
		if trigger <= g.SLTriggerPrice {
			return 0, 0, false
		}
	} else {
		// Trail downward: if LTP made new low, move SL down
		if currentLTP >= g.HighestPrice || g.HighestPrice == 0 {
			return 0, 0, false
		}
		trigger = currentLTP * (1 + pct/100)
		limit = trigger * (1 + 0.005)
		if trigger >= g.SLTriggerPrice {
			return 0, 0, false
		}
	}
	return roundNSE(trigger), roundNSE(limit), true
}

// roundNSE rounds a price to NSE tick size (0.05).
func roundNSE(price float64) float64 {
	paise := int64(price*100 + 0.5)
	paise = ((paise + 2) / 5) * 5
	return float64(paise) / 100.0
}
