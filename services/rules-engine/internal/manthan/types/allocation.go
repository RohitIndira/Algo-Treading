package types

import "fmt"

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
	// Flexi is set ONLY in flexi on-mode when this entry took a slot above
	// its bucket's base cap (audit: manthan_signal_decisions.flexi_grant).
	// nil for every base-cap entry and in every other mode. MCapBucket above
	// stays the STOCK's bucket regardless — the stop-loss rule follows the
	// stock, never the seat.
	Flexi *FlexiGrant
}

// CapCheck holds the cap counters for sector and MCap allocation.
type CapCheck struct {
	SectorCount  map[string]int // industry → count of positions
	BucketCount  map[string]int // LARGE/MID/SMALL → count
	MaxPerSector int            // 25% of max_positions
	MaxPerBucket int            // 50% of max_positions

	// BucketCeiling is the flexi-caps override per bucket: nil unless
	// MANTHAN_FLEXI_CAPS_MODE=on AND a plan was applied on this call. A nil
	// map makes CanAdd byte-identical to the pre-flexi behaviour. A ceiling
	// is only ever honoured when it is ABOVE MaxPerBucket (see bucketLimit).
	BucketCeiling map[string]int
	// FlexiDonors is audit-only — the void buckets the ceiling was lent from.
	FlexiDonors []string
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

// bucketLimit is the effective cap for a bucket: the flexi ceiling when one
// was applied AND it is above base, else MaxPerBucket. Never below base.
func (c *CapCheck) bucketLimit(bucket string) int {
	if v, ok := c.BucketCeiling[bucket]; ok && v > c.MaxPerBucket {
		return v
	}
	return c.MaxPerBucket
}

// BucketLimit exports bucketLimit for the allocator's flexi evaluation.
func (c *CapCheck) BucketLimit(bucket string) int { return c.bucketLimit(bucket) }

// CanAdd checks if adding a stock would breach sector or MCap caps.
//
// The sector branch is FIRST and unchanged by flexi. The bucket branch
// emits the legacy string byte-for-byte whenever the effective limit is the
// base cap; only a block AT a flexi ceiling (on-mode, plan applied) gets
// the explanatory string.
func (c *CapCheck) CanAdd(industry, bucket string) (bool, string) {
	if c.SectorCount[industry] >= c.MaxPerSector {
		return false, "sector cap 25% reached for " + industry
	}
	limit := c.bucketLimit(bucket)
	if c.BucketCount[bucket] >= limit {
		if limit == c.MaxPerBucket {
			return false, "mcap bucket cap 50% reached for " + bucket
		}
		return false, fmt.Sprintf("mcap bucket cap reached for %s (flexi ceiling %d = base %d + %d borrowed from %v)",
			bucket, limit, c.MaxPerBucket, limit-c.MaxPerBucket, c.FlexiDonors)
	}
	return true, ""
}

// Clone returns an independent copy (counters, limits, ceiling, donors).
// Used by dry-run to overlay the shadow book without touching the live caps.
func (c *CapCheck) Clone() *CapCheck {
	out := &CapCheck{
		SectorCount:  make(map[string]int, len(c.SectorCount)),
		BucketCount:  make(map[string]int, len(c.BucketCount)),
		MaxPerSector: c.MaxPerSector,
		MaxPerBucket: c.MaxPerBucket,
	}
	for k, v := range c.SectorCount {
		out.SectorCount[k] = v
	}
	for k, v := range c.BucketCount {
		out.BucketCount[k] = v
	}
	if c.BucketCeiling != nil {
		out.BucketCeiling = make(map[string]int, len(c.BucketCeiling))
		for k, v := range c.BucketCeiling {
			out.BucketCeiling[k] = v
		}
	}
	if c.FlexiDonors != nil {
		out.FlexiDonors = append([]string(nil), c.FlexiDonors...)
	}
	return out
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
