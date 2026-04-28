package manthan

import "time"

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
	BearerToken   string
	AppId         string
	Source        string
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
type Portfolio struct {
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

// FillEvent is the (now superset) JSON shape on manthan.execution.events.
// Originally just FILL_CONFIRMED/FILL_UPDATE; extended in Step 3 to carry the
// canonical CQRS event types ENTRY_FILLED/ENTRY_REJECTED/SL_*/EXIT_*/etc.
//
// Backwards compat: Type can hold either a legacy or a new event-type string;
// OrderID was renamed conceptually to SignalID but the JSON tag stays
// "order_id" for back-compat. SignalID/EventSeq/Source/RejectionReason fields
// are populated by the new ManthanEventPublisher in trade-execution.
type FillEvent struct {
	Type          string  `json:"type"`
	EventID       string  `json:"event_id"`
	OrderID       string  `json:"order_id"` // == SignalID; kept for back-compat
	StrategyID    string  `json:"strategy_id"`
	UserID        string  `json:"user_id"`
	Symbol        string  `json:"symbol"`
	ISIN          string  `json:"isin"`
	AvgFillPrice  float64 `json:"avg_fill_price"`
	FilledQty     int32   `json:"filled_qty"`
	ExpectedQty   int32   `json:"expected_qty"`
	BrokerOrderID string  `json:"broker_order_id"`
	SLTrigger     float64 `json:"sl_trigger"`
	SLLimit       float64 `json:"sl_limit"`
	SLBrokerID    string  `json:"sl_broker_id"`
	TradingMode   string  `json:"trading_mode"`
	Timestamp     string  `json:"timestamp"`
	Sequence      int64   `json:"sequence"`

	// New canonical fields (Step 3+)
	SignalID        string  `json:"signal_id,omitempty"`
	EventSeq        int64   `json:"event_seq,omitempty"`
	Source          string  `json:"source,omitempty"`
	FillPrice       float64 `json:"fill_price,omitempty"`
	FillQty         int32   `json:"fill_qty,omitempty"`
	RejectionReason string  `json:"rejection_reason,omitempty"`
	// ParentSignalID — non-empty when this fill is from a TOP-UP order. The
	// projector adds the new fill onto the parent's manthan_positions row
	// instead of creating a new row.
	ParentSignalID  string  `json:"parent_signal_id,omitempty"`
}

// ResolvedSignalID returns SignalID if set, otherwise OrderID (legacy fallback).
func (e *FillEvent) ResolvedSignalID() string {
	if e.SignalID != "" {
		return e.SignalID
	}
	return e.OrderID
}

// ResolvedEventSeq returns EventSeq if set, otherwise the legacy Sequence.
func (e *FillEvent) ResolvedEventSeq() int64 {
	if e.EventSeq != 0 {
		return e.EventSeq
	}
	return e.Sequence
}

// ResolvedFillPrice returns FillPrice if set, else legacy AvgFillPrice.
func (e *FillEvent) ResolvedFillPrice() float64 {
	if e.FillPrice != 0 {
		return e.FillPrice
	}
	return e.AvgFillPrice
}

// ResolvedFillQty returns FillQty if set, else legacy FilledQty.
func (e *FillEvent) ResolvedFillQty() int32 {
	if e.FillQty != 0 {
		return e.FillQty
	}
	return e.FilledQty
}

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
