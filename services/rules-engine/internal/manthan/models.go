package manthan

import (
	"sync"
	"time"
)

// ManthanSignal represents one eligible stock from the data-ingestion pipeline
// (consumed from manthan.signals Kafka topic).
type ManthanSignal struct {
	RunDate     string  `json:"run_date"`
	Symbol      string  `json:"symbol"`
	ISIN        string  `json:"isin"`
	Industry    string  `json:"industry"`
	MCapBucket  string  `json:"mcap_bucket"`  // LARGE, MID, SMALL
	IndexName   string  `json:"index_name"`   // NIFTY50, NFTYMCP150, NTYSLCP250
	MarketCap   float64 `json:"market_cap"`
	PE          float64 `json:"pe"`
	FScore      float64 `json:"fscore"`
	PAT         float64 `json:"pat"`
	LatestPrice float64 `json:"latest_price"`
	ATHClose    float64 `json:"ath_close"`
	Week52High  float64 `json:"week52_high"`
	EmittedAt   string  `json:"emitted_at"`
}

// UserStrategy holds a user's MANTHAN strategy config (from user-config-events).
//
// Auth fields (BearerToken/AppId/Source) were removed 2026-06-25 — broker
// credentials are now fetched at-edge by trade-execution via user-config gRPC
// instead of flowing through this in-memory model and onto the Kafka wire.
type UserStrategy struct {
	StrategyID    string
	UserID        string
	StrategyName  string
	TradingMode   string  // PAPER or LIVE
	TotalCapital  float64
	MaxPositions  int32
	PerStockBase  float64 // TotalCapital / MaxPositions
	StopLossPct   float64 // 20
	TrailingSLPct float64 // 2
}

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
	EntryPrice     float64       // initially from LTP, overwritten by actual fill price
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
	Symbol        string
	ATHAtExit     float64
	ExitPrice     float64
	ExitTime      time.Time
	ReentryBelow  float64 // ATHAtExit × 0.80 — stock must drop below this to re-qualify
}

// AllocationResult is what the allocator produces for one stock for one user.
type AllocationResult struct {
	Symbol        string
	Industry      string
	MCapBucket    string
	IndexName     string
	EMAAllocPct   float64 // 0.0–1.0 from IndicesGradeRange Allocations
	PerCallBase   float64 // CurrentCapital / MaxPositions
	PerCallActual float64 // PerCallBase × EMAAllocPct
	EntryPrice    float64
	Quantity      int32   // floor(PerCallActual / (EntryPrice × (1 + txn cost)))
	InitialSL     float64 // EntryPrice × 0.80
	ATHClose      float64
	Week52High    float64
	ISIN          string
}

// FillEvent + ResolvedSignalID/ResolvedEventSeq/ResolvedFillPrice/
// ResolvedFillQty moved to internal/manthan/projector/event.go on 2026-06-25
// (Finding #3 split). The projector now owns them so it can be unit-tested
// standalone; callers in this package use projector.FillEvent.

// CapCheck holds the cap counters for sector and MCap allocation.
type CapCheck struct {
	SectorCount  map[string]int // industry → count of positions
	BucketCount  map[string]int // LARGE/MID/SMALL → count
	MaxPerSector int            // 25% of max_positions
	MaxPerBucket int            // 50% of max_positions
}

// NewCapCheck creates a fresh cap checker for a user's portfolio.
func NewCapCheck(maxPositions int32, existing map[string]*Position) *CapCheck {
	c := &CapCheck{
		SectorCount:  make(map[string]int),
		BucketCount:  make(map[string]int),
		MaxPerSector: int(float64(maxPositions)*0.25 + 0.9999),
		MaxPerBucket: int(float64(maxPositions)*0.50 + 0.9999),
	}
	if c.MaxPerSector < 1 {
		c.MaxPerSector = 1
	}
	if c.MaxPerBucket < 1 {
		c.MaxPerBucket = 1
	}
	for _, p := range existing {
		if p.Active {
			c.SectorCount[p.Industry]++
			c.BucketCount[p.MCapBucket]++
		}
	}
	return c
}

// CanAdd checks if adding a stock would breach sector or MCap caps.
func (c *CapCheck) CanAdd(industry, bucket string) (bool, string) {
	if c.SectorCount[industry] >= c.MaxPerSector {
		return false, "sector cap 25% reached for " + industry
	}
	if c.BucketCount[bucket] >= c.MaxPerBucket {
		return false, "mcap bucket cap 50% reached for " + bucket
	}
	return true, ""
}

// Add records a new position in the cap counters.
func (c *CapCheck) Add(industry, bucket string) {
	c.SectorCount[industry]++
	c.BucketCount[bucket]++
}

// TransactionCost represents the cost model per trade.
var TxnCost = struct {
	SlippagePct  float64
	BrokeragePct float64
}{
	SlippagePct:  0.0005, // 0.05%
	BrokeragePct: 0.0028, // 0.28%
}

// TotalTxnCostPct returns the combined one-way transaction cost.
func TotalTxnCostPct() float64 {
	return TxnCost.SlippagePct + TxnCost.BrokeragePct
}
