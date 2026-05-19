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
	"sync/atomic"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/google/uuid"
)

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

// ExitLevelConfig is the immutable user-configured spec for one exit level.
type ExitLevelConfig struct {
	LevelNum int
	PricePct float64 // % from entry (always positive)
	QtyPct   float64 // % of total position qty
}

// ExitLevelState is the runtime state of one exit level within a Group.
type ExitLevelState struct {
	mu sync.Mutex

	LevelNum        int
	TriggerPrice    float64     // Absolute price; set after entry fills
	ExitQty         int32       // Current effective qty (may be reduced by rebalancing)
	OriginalExitQty int32       // Qty as first computed from fill — never modified after set
	Status          LevelStatus // PENDING → ACTIVE → TRIGGERED | CANCELLED

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

// tryMarkTriggered atomically checks Status==LevelActive and transitions to
// LevelTriggered in one lock acquisition. Returns false if already triggered
// or cancelled — callers must skip the exit when false to prevent double-firing
// when two workers race on the same level from rapid consecutive price updates.
func (l *ExitLevelState) tryMarkTriggered(price float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Status != LevelActive {
		return false
	}
	now := time.Now()
	l.Status = LevelTriggered
	l.TriggeredAt = &now
	l.ExitPrice = price
	return true
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

// Group tracks the full runtime state of multi-level exits for one entry order.
//
// Locking: the outer mu guards State, RemainingQty, and slice-level operations.
// Each ExitLevelState has its own inner lock for concurrent broker-WS callbacks.
type Group struct {
	mu sync.Mutex

	GroupID      uuid.UUID
	EntryOrderID uuid.UUID
	UserID       string

	Symbol      string
	Exchange    string
	StockCode   int64
	OrderSide   string // "BUY" or "SELL"
	ProductType string
	Validity    string

	// Populated after entry fills.
	FillPrice float64
	TotalQty  int32

	SLLevels []*ExitLevelState // sorted: shallowest first (level 1 = smallest % move)
	TPLevels []*ExitLevelState // sorted: nearest first (level 1 = smallest % move)

	// When SLMode is FIXED or TRAILING, a single broker SL order covers remaining qty.
	// We track its broker ID so we can cancel/replace it as TP levels fill.
	SLMode           string  // "FIXED" | "TRAILING" | "MULTI_LEVEL"
	FixedSLPct       float64 // initial SL distance from fill price (e.g. 1%)
	TrailingSLPct    float64 // trailing increment threshold (e.g. 0.2%)
	HighestPrice     float64 // peak LTP seen since entry (for trailing SL)
	CurrentSLTrigger float64 // the SL trigger price currently live at broker (TRAILING mode)
	SingleSLBrokerID string  // broker order ID of the single SL order (FIXED/TRAILING)
	SingleSLOrderID  uuid.UUID

	TPMode string // "FIXED" | "MULTI_LEVEL"

	State        GroupState
	RemainingQty int32 // decremented as levels trigger

	TradingMode string // "PAPER" | "LIVE"
	Auth        *indiraClient.AuthContext
	// AuthBearer/AuthAppID/AuthSource are copies of Auth fields — used for
	// goroutines that outlive the Auth pointer lifetime.
	AuthBearer string
	AuthAppID  string
	AuthSource string
	broker     BrokerOrderPlacer // nil for paper trading

	StrategyID   string
	StrategyName string
	EventID      uuid.UUID

	// For live orders: RegisterEntry stores these; HandleBrokerUpdate uses them
	// when the entry order's EXECUTED event fires via broker WS.
	SLLevelConfigs []SLTPLevelConfig
	TPLevelConfigs []SLTPLevelConfig

	entryFilled   bool               // true once OnEntryFill has been called
	cancelMonitor context.CancelFunc // kept for backward compat; nil in shared-worker mode

	// evaluating is a CAS guard — 1 while a shared worker holds this group.
	// Prevents two workers from concurrently mutating RemainingQty / level state
	// when rapid price updates enqueue multiple jobs for the same group.
	evaluating int32

	CreatedAt time.Time
	UpdatedAt time.Time
}

// tryClaimEvaluation atomically claims exclusive evaluation rights for this group.
// Returns false if another worker is already evaluating it — caller must drop the job.
func (g *Group) tryClaimEvaluation() bool {
	return atomic.CompareAndSwapInt32(&g.evaluating, 0, 1)
}

// releaseEvaluation releases the per-group evaluation lock after a worker finishes.
func (g *Group) releaseEvaluation() {
	atomic.StoreInt32(&g.evaluating, 0)
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

// CalcTrailingSL computes a new SL trigger price given the current LTP.
//
// Must be called BEFORE g.HighestPrice is updated to currentLTP — the caller
// (evaluateTrailingSL) only updates HighestPrice after this returns true.
//
// Logic:
//   - New SL = currentLTP ± TrailingSLPct% (0.2% below for BUY, above for SELL).
//   - Only emits an update when the new trigger is strictly better than the
//     currently placed SL (CurrentSLTrigger), and the movement is at least
//     TrailingSLPct% (prevents broker API spam on tiny ticks).
func (g *Group) CalcTrailingSL(currentLTP float64) (trigger float64, shouldUpdate bool) {
	if g.SLMode != SLModeTrailing || g.SingleSLBrokerID == "" {
		return 0, false
	}

	minPct := g.TrailingSLPct
	if minPct <= 0 {
		minPct = 0.1
	}

	if g.OrderSide == "BUY" {
		// HighestPrice has NOT been updated yet; currentLTP > HighestPrice is the caller's guard.
		if currentLTP <= g.HighestPrice {
			return 0, false
		}
		newTrigger := roundNSE(currentLTP * (1 - g.TrailingSLPct/100))
		// Only move the SL upward.
		if g.CurrentSLTrigger > 0 && newTrigger <= g.CurrentSLTrigger {
			return 0, false
		}
		// Require a minimum tick movement to avoid API spam.
		if g.CurrentSLTrigger > 0 {
			changePct := (newTrigger - g.CurrentSLTrigger) / g.CurrentSLTrigger * 100
			if changePct < minPct {
				return 0, false
			}
		}
		return newTrigger, true
	}

	// SELL: HighestPrice tracks the lowest price (best for short position).
	if g.HighestPrice <= 0 || currentLTP >= g.HighestPrice {
		return 0, false
	}
	newTrigger := roundNSE(currentLTP * (1 + g.TrailingSLPct/100))
	if g.CurrentSLTrigger > 0 && newTrigger >= g.CurrentSLTrigger {
		return 0, false
	}
	if g.CurrentSLTrigger > 0 {
		changePct := (g.CurrentSLTrigger - newTrigger) / g.CurrentSLTrigger * 100
		if changePct < minPct {
			return 0, false
		}
	}
	return newTrigger, true
}

// mlLevelRef links a broker order ID to the group and exit level it belongs to.
// Stored in Manager.mlLevelIndex for O(1) broker WS → level routing.
type mlLevelRef struct {
	group    *Group
	exitType string // models.MLExitTypeSL or models.MLExitTypeTP
	levelNum int
}

// roundNSE rounds a price to NSE tick size (₹0.05).
func roundNSE(price float64) float64 {
	paise := int64(price*100 + 0.5)
	paise = ((paise + 2) / 5) * 5
	return float64(paise) / 100.0
}
