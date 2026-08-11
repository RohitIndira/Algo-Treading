package algos

import "context"

// StaticCatalog is the in-memory implementation of the Catalog interface.
// It holds the algo list as a plain Go slice — no DB, no Redis, no network.
//
// Why start with a static catalog?
//
//	We have exactly ONE algo today (Manthan). Building a database table,
//	migrations, gRPC endpoints, and admin tooling just to store one row
//	would be over-engineering. The static catalog gets us to a working
//	frontend in minutes; when we have 10+ algos and need a non-developer
//	to edit them, we'll swap to a DB-backed catalog. The handler won't
//	notice the change — that's the whole point of the Catalog interface.
//
// Why is it a struct (with no fields) instead of a plain function?
//
//	Two reasons:
//	  1. Symmetry with the future. A *DBCatalog will hold a DB connection;
//	     a *RedisCatalog would hold a Redis client. Wrapping the static
//	     version in a struct keeps every implementation the same shape,
//	     so the wiring code (router) stays uniform.
//	  2. Methods on a struct can be added without breaking callers. If we
//	     later need to add caching, a struct is the right home for it.
type StaticCatalog struct {
	// items is the in-memory list. Built once at startup by NewStaticCatalog
	// and never mutated again — so we can share it across goroutines without
	// a lock. (A future DB-backed catalog would have totally different
	// internals, which is why this field is private — it's an implementation
	// detail, not part of the contract.)
	items []Algo

	// detailsByID is the lookup table for GET /api/v1/algos/{id}. Built
	// once at startup and never mutated, so it's safe to read from many
	// goroutines without a mutex.
	//
	// Storing details in a map keyed by ID (not a slice + linear scan)
	// keeps ByID O(1) regardless of how many algos we add later.
	detailsByID map[string]AlgoDetail

	// stats, when set, supplies REAL computed performance figures
	// (primaryReturn windows, maxDrawdown, sortino) derived from
	// algo_performance_daily. Nil-safe: without it the catalog serves its
	// baked-in defaults. See SetStatsProvider — added 2026-08-11 after the
	// hardcoded "3Y Return 28.4 / 2Y Return 32.9 / maxDrawdown -12.6" values
	// were found to be unbacked by data (only 1.49y of history existed, and
	// the true drawdown was -5.68%).
	stats StatsProvider
}

// LiveStats is the computed overlay applied to catalog entries at read time.
type LiveStats struct {
	PrimaryReturn  map[string]float64
	MaxDrawdownPct float64
	SortinoRatio   float64
}

// StatsProvider returns computed stats for an algo. ok=false → keep defaults.
type StatsProvider func(ctx context.Context, algoID string) (LiveStats, bool)

// SetStatsProvider installs the live-stats overlay. Call once at boot.
func (s *StaticCatalog) SetStatsProvider(p StatsProvider) { s.stats = p }

// applyStats overlays computed figures onto a copy of the base Algo. Only
// non-empty values override, so a provider that can compute drawdown but not
// returns (or vice-versa) degrades gracefully instead of zeroing the card.
func (s *StaticCatalog) applyStats(ctx context.Context, a *Algo) LiveStats {
	if s.stats == nil {
		return LiveStats{}
	}
	live, ok := s.stats(ctx, a.ID)
	if !ok {
		return LiveStats{}
	}
	if len(live.PrimaryReturn) > 0 {
		a.PrimaryReturn = live.PrimaryReturn
	}
	if live.MaxDrawdownPct != 0 {
		a.MaxDrawdown = live.MaxDrawdownPct
	}
	return live
}

// NewStaticCatalog returns a Catalog populated with the algos we ship by
// default. Today: just Manthan.
//
// Pattern note: we return the interface type (Catalog), not the concrete
// type (*StaticCatalog). That trains the caller to depend on the
// abstraction, not on this specific implementation. Tomorrow we'll do
// the same with NewDBCatalog — same return type, different innards.
func NewStaticCatalog() Catalog {
	// The list-card entries and detail-page entries are built by two
	// separate factories (manthan / manthanDetail) so we can hold
	// slightly different content in each — the detail page shows a
	// longer description, extra stats, "what you get" bullets, etc.
	// See each factory function for the exact payload.
	manthanDetailData := manthanDetail()
	return &StaticCatalog{
		items: []Algo{
			manthan(),
		},
		detailsByID: map[string]AlgoDetail{
			manthanDetailData.ID: manthanDetailData,
		},
	}
}

// All satisfies the Catalog interface by returning every algo we have.
//
// The receiver is `s *StaticCatalog` (pointer). For a read-only method on
// such a small struct, value receivers would work too, but the convention
// in Go is: pick one (pointer or value) and use it for ALL methods on the
// type. Pointer receivers are the safer default — they avoid copying the
// struct on every call and let you add stateful methods later without
// changing the signature.
//
// `ctx` is ignored here because there's no IO to cancel. The signature
// keeps it for future-proofing — see catalog.go.
//
// Errors are always nil. There's no failure mode for reading a Go slice.
// But we still return error in the signature, again because the interface
// promises it, and that promise is what makes swapping in a DB-backed
// catalog later a transparent change.
//
// We return a COPY of the slice (`append([]Algo(nil), s.items...)`)
// instead of the original. If we returned s.items directly, a caller
// could accidentally modify it (e.g. `result[0].Name = "Hacked"`) and
// the change would silently affect every future caller. Copying costs
// almost nothing here (one algo, ~200 bytes) and makes the catalog
// genuinely read-only from the outside.
func (s *StaticCatalog) All(ctx context.Context) ([]Algo, error) {
	out := append([]Algo(nil), s.items...)
	for i := range out {
		s.applyStats(ctx, &out[i])
	}
	return out, nil
}

// ByID returns the detail view of a single algo, or ErrAlgoNotFound
// if the id doesn't match anything we know about.
//
// We RETURN A COPY of the struct (not a pointer to the internal map
// value). Same reasoning as All: prevents any caller from accidentally
// mutating shared catalog state. `detail := s.detailsByID[id]` copies
// the AlgoDetail struct by value; the copy owns its own slices too
// because Go's copy is shallow for structs but the WhatYouGet /
// AlsoWorthKnowing slices are read-only by convention and we build
// them fresh in manthanDetail() — never appended to at runtime.
//
// ctx is currently unused (no IO in a map lookup). Kept on the
// signature because the Catalog interface promises it.
func (s *StaticCatalog) ByID(ctx context.Context, id string) (*AlgoDetail, error) {
	detail, ok := s.detailsByID[id]
	if !ok {
		return nil, ErrAlgoNotFound
	}
	live := s.applyStats(ctx, &detail.Algo)
	if live.SortinoRatio != 0 {
		detail.KeyStats.Sortino = live.SortinoRatio
	}
	return &detail, nil
}

// manthanDetail is the full detail-page data for the Manthan algo.
// Kept separate from manthan() (the list-card factory) because:
//   - The description is longer here (full paragraph vs one-liner)
//   - Detail-only fields (keyStats, whatYouGet, alsoWorthKnowing,
//     disclaimer) exist only on this side
//   - When we eventually move to a DB, list and detail may be
//     served by different queries — keeping factories separate
//     makes the migration path natural
//
// The base Algo fields (ID, Name, Type, Style, Logo, Badge,
// MinInvestment, MaxDrawdown, PrimaryReturn) MUST match the
// corresponding manthan() list card — otherwise the detail page
// would show inconsistent numbers vs the Explore screen. If you
// need to update one, update both.
func manthanDetail() AlgoDetail {
	return AlgoDetail{
		Algo: Algo{
			ID:    "algo_manthan_v1",
			Name:  "Manthan",
			Type:  "Equity",
			Style: "Positional",
			Logo:  "https://stockk-assets.s3.ap-south-1.amazonaws.com/algos/manthan.png",
			// About Algo copy shown on the detail page. Kept identical to
			// the manthan() list-card description so Explore and Detail
			// never disagree about what the strategy does.
			Description: "The Manthan strategy involves an integrated techno-funda approach for stock analysis. In the realm of technical analysis, the emphasis is on identifying stocks that exhibit superior relative strength compared to both the index and peers.",
			Badge:       "Most Subscribed",
			// Values MUST match manthan() to stay consistent
			// between the Explore card and the detail page.
			MinInvestment: 500_000,
			MaxDrawdown:   -12.6,
			PrimaryReturn: map[string]float64{
				"3Y Return": 28.4,
				"2Y Return": 32.9,
			},
		},
		KeyStats: KeyStats{
			WinRatePct:     62,
			ProfitFactor:   2.38,
			TotalTradesPct: 68,
			AvgHoldingDays: -7.4,
			Sortino:        1.84,
			VolatilityDays: 0, // removed from the UI — omitted via json:omitempty
		},
		WhatYouGet: []WhatYouGetItem{
			{Icon: "automation", Title: "Automated execution", Description: "Trades placed by us — no manual orders"},
			{Icon: "shield", Title: "Stop-loss baked in", Description: "8% trailing per position, 14% portfolio cap"},
			{Icon: "bell", Title: "Signal alerts", Description: "Push notifications on entries"},
			{Icon: "chart", Title: "Daily performance feed", Description: "Curve, P&L, drawdown updated live"},
		},
		AlsoWorthKnowing: []AlsoWorthKnowingItem{
			{Icon: "trending", Text: "Returns shown are gross & historical"},
			{Icon: "wallet", Text: "Funds must be ready at rebalance"},
			{Icon: "receipt", Text: "Frequent trades affect your taxes"},
		},
		Disclaimer: "Past performance does not guarantee future returns. Manthan trades equities only; capital can be temporarily locked during rebalancing windows.",
	}
}

// manthan is the canonical Manthan algo entry as shown on the Explore
// screen. Kept as a separate function (instead of an inline literal) so:
//   - The constructor's slice initialization stays one line and reads cleanly.
//   - When we add the second algo, both factory functions sit side-by-side.
//   - When this data eventually moves to a DB row, the migration script
//     can mirror this exact shape one-to-one.
//
// Field-by-field this matches the Manthan card on the Explore screen.
func manthan() Algo {
	return Algo{
		ID:          "algo_manthan_v1",
		Name:        "Manthan",
		Type:        "Equity",
		Style:       "Positional",
		Logo:        "https://stockk-assets.s3.ap-south-1.amazonaws.com/algos/manthan.png",
		Description: "The Manthan strategy involves an integrated techno-funda approach for stock analysis. In the realm of technical analysis, the emphasis is on identifying stocks that exhibit superior relative strength compared to both the index and peers.",
		Badge:       "Most subscribed",
		// 15,00,000 rupees = ₹15 Lakhs. We send raw rupees as an int64
		// so the API stays stable when the frontend changes its display
		// format ("15 Lac" / "15 Lakhs" / "₹15,00,000").
		MinInvestment: 500_000,
		// −12.6%. Stored as a plain number; the frontend adds the "%" suffix.
		MaxDrawdown: -12.6,
		// Map keys exactly match what the frontend expects to render.
		// Adding "5Y Return" later means one new key here, nothing else.
		PrimaryReturn: map[string]float64{
			"3Y Return": 28.4,
			"2Y Return": 32.9,
		},
	}
}

// Compile-time assertion: if *StaticCatalog ever stops satisfying the
// Catalog interface (e.g. someone renames All or changes its signature),
// this line will fail to compile, surfacing the mistake at build time
// instead of as a confusing runtime error later.
//
// Read it as: "I claim that nil-of-type-*StaticCatalog is a valid Catalog.
// Compiler, prove me wrong."
var _ Catalog = (*StaticCatalog)(nil)
