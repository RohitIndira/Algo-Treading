package oco

import (
	"context"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/google/uuid"
)

// OCOState represents the lifecycle state of an OCO group.
type OCOState string

const (
	// StatePendingEntry — entry SL order placed, waiting for broker fill.
	StatePendingEntry OCOState = "PENDING_ENTRY"
	// StateAwaitingFillPrice — entry filled but the execution price is not yet
	// known (the broker WS carried no TradedPrice, e.g. a MARKET order). SL/TP
	// legs are deferred until the fill-price retry worker resolves the real price
	// from the trade book — or, past a deadline, from the buffer-stripped entry
	// limit. NOT terminal: the position exists and must still get protective legs.
	StateAwaitingFillPrice OCOState = "AWAITING_FILL_PRICE"
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
	RoleEntry OCORole = "ENTRY"
	RoleSLLeg OCORole = "SL_LEG"
	RoleTPLeg OCORole = "TP_LEG"
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
	// EntryLimitPrice is the LIMIT price the entry was placed at (LTP×1.005 for
	// BUY). Used only as the last-resort SL/TP reference (buffer-stripped) when
	// the real fill price cannot be resolved from WS or trade book.
	EntryLimitPrice float64 `json:"entry_limit_price,omitempty"`
	// EntryFilledAt is when the broker reported the entry EXECUTED. Starts the
	// deadline clock for the AWAITING_FILL_PRICE → limit-fallback transition.
	EntryFilledAt time.Time `json:"entry_filled_at,omitempty"`

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
	StrategyID   string    `json:"strategy_id,omitempty"`
	StrategyName string    `json:"strategy_name,omitempty"`
	EventID      uuid.UUID `json:"event_id,omitempty"`

	// ── Partial Fill Tracking ───────────────────────────────────────────────
	// When the entry order partially fills (e.g., 80 of 90), SL/TP legs are
	// placed immediately for the filled qty. A timer waits for the remaining
	// qty to fill; if it doesn't fill in time, the remaining entry is cancelled.
	FilledQty               int32              `json:"filled_qty,omitempty"`          // Entry qty filled so far (legs placed for this qty)
	PartialFillActive       bool               `json:"partial_fill_active,omitempty"` // True while waiting for remaining entry qty
	PartialFillCancelFunc   context.CancelFunc `json:"-"`                             // Cancels the partial fill timeout goroutine
	PartialFillTimerStarted bool               `json:"-"`                             // Prevents duplicate timer starts

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

// stripEntryBuffer removes the BUY entry limit buffer (price×1.005) so that SL/TP
// percentages applied to the *entry limit* land relative to the intended entry
// (≈ signal LTP) instead of the inflated limit. It is applied ONLY when falling
// back to the entry limit because the real execution price is unknown — when the
// actual broker fill price is available it is used directly (no strip), so SL/TP
// are relative to the true execution price. SELL entries carry no buffer.
func stripEntryBuffer(limitPrice float64, side string) float64 {
	if side == "BUY" {
		return limitPrice / 1.005
	}
	return limitPrice
}

// CalculateSLFromFill computes the SL trigger price from the execution price.
// The caller passes the resolved fill price (real broker fill preferred; the
// buffer-stripped entry limit only as a last resort), so the percentage is always
// relative to the true execution price. Returns trigger only — SL legs are placed
// as SL-M (stop market) so no limit price is needed.
//
//	BUY entry  → trigger = fill * (1 - slPct/100)
//	SELL entry → trigger = fill * (1 + slPct/100)
func (g *OCOGroup) CalculateSLFromFill(fillPrice float64) float64 {
	if g.OrderSide == "BUY" {
		return roundNSE(fillPrice * (1 - g.SLPercent/100))
	}
	return roundNSE(fillPrice * (1 + g.SLPercent/100))
}

// CalculateTPFromFill computes the TP limit price from the execution price.
//
//	BUY entry  → TP limit = fill * (1 + tpPct/100)
//	SELL entry → TP limit = fill * (1 - tpPct/100)
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
		limit = trigger // SL-M: no limit price needed; kept for signature compatibility
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
		limit = trigger // SL-M: no limit price needed; kept for signature compatibility
		if trigger >= g.SLTriggerPrice {
			return 0, 0, false
		}
	}

	// Minimum broker-API call threshold: only send modify when the SL has
	// moved by at least minPct from its current value (avoids API spam).
	// Cap at 0.5% so that a user-configured TrailingSLPct (e.g. 2%) does not
	// make the trailing effectively unresponsive (2% SL × 2% step = SL never trails
	// until price rises by the full SL distance above entry).
	minPct := g.TrailingSLPct
	if minPct <= 0 || minPct > 0.5 {
		minPct = 0.1
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
