package manthan

import (
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// TrailingSLManager manages trailing stop-losses for all positions.
//
// SL logic (Option B — confirmed):
//   - Initial SL = 20% below entry price
//   - Track highest price since entry (HighSinceEntry)
//   - SL trails in 2% increments: each time price rises 2% above
//     the last trail level, SL is recalculated as 20% below new high
//   - SL only moves UP, never down
//   - When LTP ≤ CurrentSL → position exits
//
// Example:
//   Entry  ₹100 → SL ₹80, LastTrailLevel = ₹100
//   LTP ₹102 (+2%) → High=₹102, SL=₹81.60, LastTrailLevel=₹102
//   LTP ₹103       → High=₹103, SL stays ₹81.60 (next trigger at ₹104)
//   LTP ₹104 (+2%) → High=₹104, SL=₹83.20, LastTrailLevel=₹104
//   LTP ₹110       → High=₹110, SL=₹88.00, LastTrailLevel=₹110
//   LTP ₹88        → SL HIT → EXIT
type TrailingSLManager struct {
	logger *zap.Logger
}

func NewTrailingSLManager(logger *zap.Logger) *TrailingSLManager {
	return &TrailingSLManager{logger: logger}
}

// SLAction is the result of processing a tick.
type SLAction int

const (
	SLNoAction   SLAction = iota // no change
	SLModify                     // modify SL order to new price
	SLTriggered                  // SL hit — exit position
)

// SLUpdate holds the details when SL needs modification or was triggered.
type SLUpdate struct {
	Action    SLAction
	Symbol    string
	OldSL     float64
	NewSL     float64
	NewHigh   float64
	ExitPrice float64 // only set when SLTriggered
}

// ProcessTick evaluates a single LTP tick against a position's trailing SL.
// Called on every tick from the websocket feed.
func (m *TrailingSLManager) ProcessTick(pos *types.Position, ltp float64, slPct float64, trailStepPct float64) SLUpdate {
	if !pos.Active || ltp <= 0 {
		return SLUpdate{Action: SLNoAction}
	}

	// Check if SL triggered FIRST (before updating high)
	if ltp <= pos.CurrentSL {
		pos.Active = false
		return SLUpdate{
			Action:    SLTriggered,
			Symbol:    pos.Symbol,
			OldSL:     pos.CurrentSL,
			ExitPrice: ltp,
		}
	}

	// Update highest price since entry
	if ltp <= pos.HighSinceEntry {
		return SLUpdate{Action: SLNoAction}
	}
	pos.HighSinceEntry = ltp

	// Check if price crossed the next 2% trail level
	nextTrailLevel := pos.LastTrailLevel * (1 + trailStepPct/100)
	if ltp < nextTrailLevel {
		return SLUpdate{Action: SLNoAction}
	}

	// Price crossed 2% increment — recalculate SL
	oldSL := pos.CurrentSL
	newSL := pos.HighSinceEntry * (1 - slPct/100) // 20% below new high

	// SL only moves up
	if newSL <= pos.CurrentSL {
		return SLUpdate{Action: SLNoAction}
	}

	pos.CurrentSL = newSL
	pos.LastTrailLevel = ltp

	m.logger.Info("Trailing SL updated",
		zap.String("symbol", pos.Symbol),
		zap.Float64("ltp", ltp),
		zap.Float64("old_sl", oldSL),
		zap.Float64("new_sl", newSL),
		zap.Float64("high", pos.HighSinceEntry),
	)

	return SLUpdate{
		Action:  SLModify,
		Symbol:  pos.Symbol,
		OldSL:   oldSL,
		NewSL:   newSL,
		NewHigh: pos.HighSinceEntry,
	}
}

// InitPosition sets up the initial SL state for a new entry.
func (m *TrailingSLManager) InitPosition(pos *types.Position, slPct float64) {
	pos.HighSinceEntry = pos.EntryPrice
	pos.CurrentSL = pos.EntryPrice * (1 - slPct/100)
	pos.LastTrailLevel = pos.EntryPrice
	pos.Active = true
}
