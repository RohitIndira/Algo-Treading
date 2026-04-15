// Package multilevel implements multi-level stop loss and take profit exits.
//
// Feature overview:
//   - Users configure up to 5 SL levels and/or 5 TP levels per strategy.
//   - Each level has a price_pct (% distance from entry) and qty_pct (% of position to exit).
//   - All qty_pct values for a given exit type must sum to 100.
//
// Supported combinations:
//   - Multi-level TP only  (+ any SL mode: none / fixed / trailing)
//   - Multi-level SL only  (+ any TP mode: none / fixed)
//   - Multi-level SL + multi-level TP
//   - Fixed SL  + multi-level TP
//   - Trailing SL + multi-level TP
//
// Live trading:
//   - TP levels: N separate LIMIT exit orders placed after entry fills.
//   - SL levels: application-level price monitoring; market exit placed when breached.
//   - Fixed SL combined with multi-level TP: one SL order placed for full qty; cancelled
//     and replaced with reduced qty as TP levels fill.
//
// Paper trading:
//   - Both SL and TP levels monitored via Redis LTP polling.
//   - Partial exits simulated at each level.
package multilevel

import (
	"context"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/google/uuid"
)

// ── State constants ──────────────────────────────────────────────────────────

// GroupState is the lifecycle state of a MultiLevelGroup.
type GroupState string

const (
	GroupStateActive    GroupState = "ACTIVE"
	GroupStateCompleted GroupState = "COMPLETED"
	GroupStateCancelled GroupState = "CANCELLED"
)

// LevelStatus is the lifecycle state of a single exit level.
type LevelStatus string

const (
	LevelPending   LevelStatus = "PENDING"
	LevelActive    LevelStatus = "ACTIVE"
	LevelTriggered LevelStatus = "TRIGGERED"
	LevelCancelled LevelStatus = "CANCELLED"
)

// SLMode mirrors the user-config constants for routing decisions.
const (
	SLModeFixed      = "FIXED"
	SLModeTrailing   = "TRAILING"
	SLModeMultiLevel = "MULTI_LEVEL"
	TPModeFixed      = "FIXED"
	TPModeMultiLevel = "MULTI_LEVEL"
)

// ── Level types ──────────────────────────────────────────────────────────────

// ExitLevelConfig is the immutable user-configured spec for one exit level.
type ExitLevelConfig struct {
	LevelNum int
	PricePct float64 // % from entry (always positive)
	QtyPct   float64 // % of total position qty
}

// ExitLevelState is the runtime state of one exit level within a Group.
type ExitLevelState struct {
	mu sync.Mutex

	LevelNum     int
	TriggerPrice float64     // Absolute price; set after entry fills
	ExitQty      int32       // Absolute qty; set after entry fills
	Status       LevelStatus // PENDING → ACTIVE → TRIGGERED | CANCELLED

	// For live TP limit orders:
	BrokerOrderID string
	ExitOrderID   uuid.UUID

	// For tracking when triggered
	TriggeredAt *time.Time
	ExitPrice   float64 // Actual fill / execution price
}

func (l *ExitLevelState) markTriggered(price float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.Status = LevelTriggered
	l.TriggeredAt = &now
	l.ExitPrice = price
}

func (l *ExitLevelState) markCancelled() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Status = LevelCancelled
}

func (l *ExitLevelState) markActive(brokerOrderID string, exitOrderID uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Status = LevelActive
	l.BrokerOrderID = brokerOrderID
	l.ExitOrderID = exitOrderID
}

// ── Group ────────────────────────────────────────────────────────────────────

// Group tracks the full runtime state of multi-level exits for one entry order.
//
// Locking: the outer mu guards State, RemainingQty, and slice-level operations.
// Each ExitLevelState has its own inner lock for concurrent broker-WS callbacks.
type Group struct {
	mu sync.Mutex

	// ── Identity ──────────────────────────────────────────────────────────
	GroupID      uuid.UUID
	EntryOrderID uuid.UUID
	UserID       string

	// ── Instrument ────────────────────────────────────────────────────────
	Symbol      string
	Exchange    string
	StockCode   int64
	OrderSide   string // "BUY" or "SELL"
	ProductType string
	Validity    string

	// ── Fill info (populated after entry fills) ───────────────────────────
	FillPrice float64
	TotalQty  int32

	// ── Exit levels ───────────────────────────────────────────────────────
	SLLevels []*ExitLevelState // sorted: shallowest first (level 1 = smallest % move)
	TPLevels []*ExitLevelState // sorted: nearest first (level 1 = smallest % move)

	// ── SL mode context for mixed combinations ────────────────────────────
	// When SLMode is FIXED or TRAILING, a single broker SL order covers
	// remaining qty. We track its broker ID so we can cancel/replace it as
	// TP levels fill.
	SLMode          string  // "FIXED" | "TRAILING" | "MULTI_LEVEL"
	FixedSLPct      float64 // used when SLMode == FIXED
	TrailingSLPct   float64 // used when SLMode == TRAILING
	HighestPrice    float64 // for trailing SL tracking
	SingleSLBrokerID string // broker order ID of the single SL order (FIXED/TRAILING)
	SingleSLOrderID  uuid.UUID

	// ── TP mode ───────────────────────────────────────────────────────────
	TPMode string // "FIXED" | "MULTI_LEVEL"

	// ── State ─────────────────────────────────────────────────────────────
	State        GroupState
	RemainingQty int32 // decremented as levels trigger

	// ── Trading context ───────────────────────────────────────────────────
	TradingMode string // "PAPER" | "LIVE"
	Auth        *indiraClient.AuthContext
	broker      BrokerOrderPlacer // nil for paper trading

	// ── Strategy context ──────────────────────────────────────────────────
	StrategyID   string
	StrategyName string
	EventID      uuid.UUID

	// ── Original level configs (stored for use in OnEntryFill from WS) ──
	// For live orders: RegisterEntry stores these; HandleBrokerUpdate uses them
	// when the entry order's EXECUTED event fires via broker WS.
	SLLevelConfigs []SLTPLevelConfig
	TPLevelConfigs []SLTPLevelConfig

	// ── Internal ──────────────────────────────────────────────────────────
	entryFilled   bool              // true once OnEntryFill has been called
	cancelMonitor context.CancelFunc // stops the price-monitor goroutine
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// SLTPLevelConfig stores the immutable user config for a single exit level.
type SLTPLevelConfig struct {
	LevelNum int
	PricePct float64
	QtyPct   float64
}

// ExitSide returns the side that exits the position (opposite of entry side).
func (g *Group) ExitSide() string {
	if g.OrderSide == "BUY" {
		return "SELL"
	}
	return "BUY"
}

// CalcSLTriggerPrice computes the absolute SL trigger for a given price_pct.
//
//	BUY  → trigger = fillPrice * (1 − pricePct/100)
//	SELL → trigger = fillPrice * (1 + pricePct/100)
func (g *Group) CalcSLTriggerPrice(pricePct float64) float64 {
	if g.OrderSide == "BUY" {
		return roundNSE(g.FillPrice * (1 - pricePct/100))
	}
	return roundNSE(g.FillPrice * (1 + pricePct/100))
}

// CalcTPLimitPrice computes the absolute TP limit for a given price_pct.
//
//	BUY  → limit = fillPrice * (1 + pricePct/100)
//	SELL → limit = fillPrice * (1 − pricePct/100)
func (g *Group) CalcTPLimitPrice(pricePct float64) float64 {
	if g.OrderSide == "BUY" {
		return roundNSE(g.FillPrice * (1 + pricePct/100))
	}
	return roundNSE(g.FillPrice * (1 - pricePct/100))
}

// SLBreached returns true if the current LTP has crossed the SL trigger price.
func (g *Group) SLBreached(triggerPrice, ltp float64) bool {
	if g.OrderSide == "BUY" {
		return ltp <= triggerPrice
	}
	return ltp >= triggerPrice
}

// TPReached returns true if the current LTP has reached the TP limit price.
func (g *Group) TPReached(limitPrice, ltp float64) bool {
	if g.OrderSide == "BUY" {
		return ltp >= limitPrice
	}
	return ltp <= limitPrice
}

// CalcTrailingSL computes a new SL trigger price given the current LTP
// using the same proportional trailing logic as the OCO manager.
func (g *Group) CalcTrailingSL(currentLTP float64) (trigger float64, shouldUpdate bool) {
	if g.SLMode != SLModeTrailing || g.HighestPrice <= 0 || g.SingleSLBrokerID == "" {
		return 0, false
	}

	// Compute current SL trigger from highest price
	currentSLTrigger := g.CalcSLTriggerPrice(g.TrailingSLPct)

	if g.OrderSide == "BUY" {
		if currentLTP <= g.HighestPrice {
			return 0, false
		}
		trigger = currentSLTrigger * (currentLTP / g.HighestPrice)
		if trigger <= currentSLTrigger {
			return 0, false
		}
	} else {
		if currentLTP >= g.HighestPrice {
			return 0, false
		}
		trigger = currentSLTrigger * (currentLTP / g.HighestPrice)
		if trigger >= currentSLTrigger {
			return 0, false
		}
	}

	// Only update when SL has moved by at least the trailing threshold
	minPct := g.TrailingSLPct
	if minPct <= 0 {
		minPct = 0.1
	}
	changePct := (trigger - currentSLTrigger) / currentSLTrigger * 100
	if changePct < 0 {
		changePct = -changePct
	}
	if changePct < minPct {
		return 0, false
	}

	return roundNSE(trigger), true
}

// roundNSE rounds a price to NSE tick size (₹0.05).
func roundNSE(price float64) float64 {
	paise := int64(price*100 + 0.5)
	paise = ((paise + 2) / 5) * 5
	return float64(paise) / 100.0
}
