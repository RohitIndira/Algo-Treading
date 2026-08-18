package types

// AllocationResult is what the allocator produces for one stock for one user.
//
// RunDate is the SEMANTIC trading day this allocation was decided for
// (from manthan_signals.run_date, e.g. "2026-07-15"). It's used to derive
// a DETERMINISTIC signal_id in OrderGenerator — same (strategy, symbol,
// runDate) always produces the same UUID, which makes rules-engine
// idempotent across restarts / Kafka replays / manthan-live re-fires.
// Never derive this from wall-clock in the generator — a signal being
// processed 5 seconds after midnight would compute a DIFFERENT id from
// the same signal processed 5 seconds before, defeating dedup.
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
	RunDate       string // YYYY-MM-DD in IST — source signal's run_date; anchors the deterministic OrderID
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
	// Caps are HARD ceilings on the COMPLETE book (max_positions slots):
	// sector ≤ 25%, mcap bucket ≤ 50% — so the per-sector/bucket limits are
	// the FLOOR of the percentage. The previous ceiling math (+0.9999)
	// allowed 7/25 sector positions (28%) and 13/25 bucket positions (52%),
	// silently breaching the stated ≤25%/≤50% rule (corrected 2026-08-18).
	c := &CapCheck{
		SectorCount:  make(map[string]int),
		BucketCount:  make(map[string]int),
		MaxPerSector: int(float64(maxPositions) * 0.25),
		MaxPerBucket: int(float64(maxPositions) * 0.50),
	}
	if c.MaxPerSector < 1 {
		c.MaxPerSector = 1
	}
	if c.MaxPerBucket < 1 {
		c.MaxPerBucket = 1
	}
	// Occupies(): dispatched-but-unfilled positions reserve their sector /
	// bucket slot too (see Position.Occupies).
	for _, p := range existing {
		if p.Occupies() {
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

// TxnCost represents the cost model per trade.
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
