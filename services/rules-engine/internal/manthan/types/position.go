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
	StateExited          PositionState = "EXITED"           // SL hit or manual exit
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
	Active         bool          // kept for backward compat (true when State == ACTIVE)
	BrokerOrderID  string        // broker order ID for the entry order
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
// "concurrent map read and map write" panic.
type Portfolio struct {
	Mu             sync.RWMutex
	UserID         string
	StrategyID     string
	InitialCapital float64
	CurrentCapital float64 // adjusted for realized P&L
	MaxPositions   int32   // 25 or 50
	PerStockBase   float64 // CurrentCapital / MaxPositions (recalc on rebalance)
	Positions      map[string]*Position
	Cooldown       map[string]*CooldownEntry
}

// CooldownEntry tracks a stock in re-entry cooldown after SL exit.
type CooldownEntry struct {
	Symbol       string
	ATHAtExit    float64
	ExitPrice    float64
	ExitTime     time.Time
	ReentryBelow float64 // ATHAtExit × 0.80 — stock must drop below this to re-qualify
}
