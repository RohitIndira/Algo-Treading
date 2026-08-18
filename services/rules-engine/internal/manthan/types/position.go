package types

import (
	"sync"
	"time"
)

// PositionState tracks the lifecycle of a position.
type PositionState string

const (
	StatePendingEntry    PositionState = "PENDING_ENTRY"    // order placed, awaiting fill
	StatePartiallyFilled PositionState = "PARTIALLY_FILLED" // partial fill received, waiting for rest
	StateActive          PositionState = "ACTIVE"           // fill confirmed, SL + trailing enabled
	StateExitPending     PositionState = "EXIT_PENDING"     // trail crossed, exit ORDERED, awaiting broker confirmation
	StateExited          PositionState = "EXITED"           // broker-confirmed exit
)

// Position tracks a live holding for a user.
type Position struct {
	Symbol         string
	ISIN           string
	Industry       string
	MCapBucket     string
	IndexName      string
	EntryPrice     float64 // initially from LTP, overwritten by actual fill price
	EntryTime      time.Time
	Quantity       int32
	InvestedAmt    float64       // actual ₹ deployed (PerStockBase × EMA%)
	HighSinceEntry float64       // post-entry highest price (for trailing SL)
	CurrentSL      float64       // 20% below HighSinceEntry
	LastTrailLevel float64       // last 2% increment that triggered trail
	State          PositionState // PENDING_ENTRY → ACTIVE → EXITED
	// ExitPendingSince is when MarkExitPending froze the position (zero when
	// not EXIT_PENDING). The tick loop reverts a stale EXIT_PENDING to
	// ACTIVE after ExitPendingTTL so a position whose exit command died
	// downstream (DLQ, dead auth) is not frozen forever — trailing resumes
	// and the SL_EXIT re-fires under the next day's signal_id.
	ExitPendingSince time.Time
	Active           bool   // kept for backward compat (true when State == ACTIVE)
	BrokerOrderID    string // broker order ID for the entry order
}

// Portfolio tracks a user's full MANTHAN portfolio state.
//
// Concurrency: every read or write of Positions / Cooldown / the capital
// fields must be done under Mu. PortfolioManager methods take this lock
// internally when they mutate; external callers iterating portfolio.Positions
// (LTPFeed poll, allocator cap-check, tick handler, publisher snapshot,
// fill consumer position lookup) must take Mu.RLock around their read.
//
// PortfolioManager.mu (outer map lock) does NOT cover the inner maps once
// a *Portfolio pointer has escaped via AllPortfolios / Get / GetOrCreate.
// Without this per-Portfolio mutex, the LTP poll iterating Positions while
// the fill consumer mutated the same map would eventually trip Go's
// Occupies reports whether the position holds a book slot and counts toward
// the sector / mcap-bucket caps: everything from dispatch (PENDING_ENTRY,
// PARTIALLY_FILLED) through ACTIVE and EXIT_PENDING, until the broker
// confirms the exit. Capital and slot are reserved at DISPATCH — counting
// only Active (fill-confirmed) positions let every signal in a morning
// batch see the same free slot while earlier dispatches were still
// unfilled: 2026-08-17 FIV99 dispatched 26 entries on a 25-slot book.
func (p *Position) Occupies() bool {
	if p == nil {
		return false
	}
	if p.Active {
		return true
	}
	switch p.State {
	case StatePendingEntry, StatePartiallyFilled, StateActive, StateExitPending:
		return true
	}
	return false
}

// "concurrent map read and map write" panic.
type Portfolio struct {
	Mu             sync.RWMutex
	UserID         string
	StrategyID     string
	InitialCapital float64
	CurrentCapital float64 // adjusted for realized P&L
	MaxPositions   int32   // 25 or 50
	PerStockBase   float64 // CurrentCapital / MaxPositions (recalc on rebalance)
	// StopLossPct is the strategy's initial/trailing stop distance in percent
	// (20 for Manthan). The allocator sizes every NEW position's InitialSL
	// from this — never from a literal. 2026-08-18: a "TEST" literal of 0.98
	// (2% stop) had shipped to production; every position was born with a
	// 2% stop the 20% trail could not ratchet below, and every restart
	// rehydrated it → phantom TSL exits (SHANTIGOLD, FILATEX, GNA,
	// NRBBEARING) and a live near-miss (ALIVUS).
	StopLossPct float64
	Positions   map[string]*Position
	Cooldown    map[string]*CooldownEntry
}

// CooldownEntry tracks a stock in re-entry cooldown after SL exit.
type CooldownEntry struct {
	Symbol       string
	ATHAtExit    float64
	ExitPrice    float64
	ExitTime     time.Time
	ReentryBelow float64 // ATHAtExit × 0.80 — stock must drop below this to re-qualify
}
