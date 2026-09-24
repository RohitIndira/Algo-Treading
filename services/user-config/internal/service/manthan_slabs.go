package service

// Manthan investment slabs (product revision 2026-09-24).
//
//	₹50,000  – ₹2,50,000   → 10 positions
//	₹2,50,000 – ₹25,00,000 → 25 positions
//	> ₹25,00,000           → 50 positions
//
// Slab boundaries land on the LOWER slab (≤2.5L → 10; ≤25L → 25), matching
// the previous behaviour at the 25L boundary. Applied at CREATE only —
// the update path cannot change total_capital (trade_configs UPDATE never
// touches capital/max_positions), so existing strategies keep the slab
// they were created with.
//
// Everything downstream derives from MaxPositions: per_stock_amount here,
// and in rules-engine the slot count, per-call sizing, sector cap
// (floor 25%, min 1) and mcap-bucket cap (floor 50%). With 10 positions
// those caps are 2/sector and 5/bucket. Small capital means small
// per-call bases (₹50k → ₹5k): stocks priced above the per-call base are
// skipped with an auditable "quantity = 0" decision row + Kafka event —
// small accounts hold fewer than their slab's max names by design.
const manthanMinCapital = 50_000

func manthanSlabPositions(totalCapital float64) int32 {
	switch {
	case totalCapital <= 250_000:
		return 10
	case totalCapital <= 2_500_000:
		return 25
	default:
		return 50
	}
}
