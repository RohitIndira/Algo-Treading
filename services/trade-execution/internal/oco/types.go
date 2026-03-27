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
	// StateLegsSubmitted — SL+TP legs submitted to broker API (got ordId),
	// but NOT yet confirmed on exchange. Waiting for WS PENDING/OPEN.
	StateLegsSubmitted OCOState = "LEGS_SUBMITTED"
	// StateActive — both SL and TP legs confirmed on exchange via broker WS.
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
	State          OCOState `json:"state"`
	HighestPrice   float64  `json:"highest_price,omitempty"` // for trailing SL
	PnL            float64  `json:"pnl,omitempty"`           // realized P&L when completed
	SLLegConfirmed bool     `json:"sl_leg_confirmed"`        // WS confirmed SL leg on exchange
	TPLegConfirmed bool     `json:"tp_leg_confirmed"`        // WS confirmed TP leg on exchange

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
// Uses proportional trailing: SL moves by the same percentage as the price.
//
// Example (BUY, SL=10%, trailPct=1%):
//
//	fill=100, SL=90, highest=100
//	LTP → 101 (+1%): SL = 90 * (101/100) = 90.9 (+1% of SL value)
//	LTP → 105 (+5%): SL = 90.9 * (105/101) = 94.4
//
// trailPct is used as minimum threshold — the SL modify order is only sent
// to the broker when the SL has moved by at least trailPct% from its current
// position, avoiding excessive broker API calls on small price ticks.
func (g *OCOGroup) CalculateTrailingSL(currentLTP float64) (trigger, limit float64, shouldUpdate bool) {
	if !g.TrailingSL || g.State != StateActive {
		return 0, 0, false
	}
	if g.HighestPrice <= 0 || g.SLTriggerPrice <= 0 {
		return 0, 0, false
	}

	if g.OrderSide == "BUY" {
		// Only trail on new highs
		if currentLTP <= g.HighestPrice {
			return 0, 0, false
		}
		// Proportional: SL moves up by the same ratio as the price
		trigger = g.SLTriggerPrice * (currentLTP / g.HighestPrice)
		limit = trigger * (1 - 0.005) // 0.5% buffer below trigger
		if trigger <= g.SLTriggerPrice {
			return 0, 0, false
		}
	} else {
		// SELL: only trail on new lows
		if currentLTP >= g.HighestPrice {
			return 0, 0, false
		}
		// Proportional: SL moves down by the same ratio as the price
		trigger = g.SLTriggerPrice * (currentLTP / g.HighestPrice)
		limit = trigger * (1 + 0.005) // 0.5% buffer above trigger
		if trigger >= g.SLTriggerPrice {
			return 0, 0, false
		}
	}

	// Minimum update threshold: only send modify to broker when SL has
	// moved by at least trailPct% from its current position.
	minPct := g.TrailingSLPct
	if minPct <= 0 {
		minPct = 0.1 // default 0.1% minimum threshold
	}
	changePct := (trigger - g.SLTriggerPrice) / g.SLTriggerPrice * 100
	if changePct < 0 {
		changePct = -changePct
	}
	if changePct < minPct {
		return 0, 0, false
	}

	return roundNSE(trigger), roundNSE(limit), true
}

// roundNSE rounds a price to NSE tick size (0.05).
func roundNSE(price float64) float64 {
	paise := int64(price*100 + 0.5)
	paise = ((paise + 2) / 5) * 5
	return float64(paise) / 100.0
}
