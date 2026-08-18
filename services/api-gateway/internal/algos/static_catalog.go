package algos

import "context"

type StaticCatalog struct {
	items []Algo

	detailsByID map[string]AlgoDetail

	stats StatsProvider
}

type LiveStats struct {
	PrimaryReturn  map[string]float64
	MaxDrawdownPct float64
	SortinoRatio   float64
}

// StatsProvider returns computed stats for an algo. ok=false → keep defaults.
type StatsProvider func(ctx context.Context, algoID string) (LiveStats, bool)

// SetStatsProvider installs the live-stats overlay. Call once at boot.
func (s *StaticCatalog) SetStatsProvider(p StatsProvider) { s.stats = p }

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

	return live
}

func NewStaticCatalog() Catalog {

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
func (s *StaticCatalog) All(ctx context.Context) ([]Algo, error) {
	out := append([]Algo(nil), s.items...)
	for i := range out {
		s.applyStats(ctx, &out[i])
	}
	return out, nil
}

func (s *StaticCatalog) ByID(ctx context.Context, id string) (*AlgoDetail, error) {
	detail, ok := s.detailsByID[id]
	if !ok {
		return nil, ErrAlgoNotFound
	}
	s.applyStats(ctx, &detail.Algo) // primaryReturn only — see applyStats
	return &detail, nil
}

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
			MaxDrawdown:   -17, // operator-supplied track-record drawdown
			PrimaryReturn: map[string]float64{
				"3Y Return": 28.4,
				"2Y Return": 32.9,
			},
		},

		KeyStats: KeyStats{
			WinRatePct:     54.63,
			ProfitFactor:   2.60,
			TotalTradesPct: 205, // a COUNT despite the field name — see types.go
			AvgHoldingDays: 87,
			Sortino:        2.12,
			VolatilityDays: 0, // removed from the UI — omitted via json:omitempty
		},
		WhatYouGet: []WhatYouGetItem{
			{Icon: "automation", Title: "Automated execution", Description: "Trades placed by us — no manual orders"},
			{Icon: "shield", Title: "Risk guardrails baked in", Description: "20% trailing stop-loss per position (2% steps); max 25% per sector, 50% per market-cap bucket"},
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

func manthan() Algo {
	return Algo{
		ID:          "algo_manthan_v1",
		Name:        "Manthan",
		Type:        "Equity",
		Style:       "Positional",
		Logo:        "https://stockk-assets.s3.ap-south-1.amazonaws.com/algos/manthan.png",
		Description: "The Manthan strategy involves an integrated techno-funda approach for stock analysis. In the realm of technical analysis, the emphasis is on identifying stocks that exhibit superior relative strength compared to both the index and peers.",
		Badge:       "Most subscribed",
		// 5,00,000 rupees = ₹5 Lakhs. We send raw rupees as an int64
		// so the API stays stable when the frontend changes its display
		// format ("5 Lac" / "5 Lakhs" / "₹5,00,000").
		MinInvestment: 500_000,
		// Stored as a plain number; the frontend adds the "%" suffix.
		MaxDrawdown: -17, // operator-supplied track-record drawdown
		// Map keys exactly match what the frontend expects to render.
		// Adding "5Y Return" later means one new key here, nothing else.
		PrimaryReturn: map[string]float64{
			"3Y Return": 28.4,
			"2Y Return": 32.9,
		},
	}
}

var _ Catalog = (*StaticCatalog)(nil)
