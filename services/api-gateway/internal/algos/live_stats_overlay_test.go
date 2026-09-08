package algos

// Catalog stats contract — updated 2026-09-08 (operator decision):
// the Key Stats grid (win rate, profit factor, total trades, avg holding,
// Sortino) and MaxDrawdown are STATIC track-record figures from the
// strategy writeup sheet (rows 241-246) and must NEVER be replaced by
// live-computed values. Only the additive series fields (Sharpe /
// TotalReturnPct / CAGRPct) and the PrimaryReturn headline overlay from
// the real daily series. These tests pin both directions.

import (
	"context"
	"testing"
)

// Sheet-of-record values (strategy writeup rows 241-246).
const (
	sheetWinRate    = 48.78
	sheetPF         = 2.42
	sheetTrades     = 205
	sheetAvgHolding = 96
	sheetSortino    = 2.25
	sheetMaxDD      = -17
)

func overlayCatalog(live LiveStats, ok bool) Catalog {
	c := NewStaticCatalog()
	c.(*StaticCatalog).SetStatsProvider(func(context.Context, string) (LiveStats, bool) {
		return live, ok
	})
	return c
}

// Live series present → statics must still stand; only additive fields +
// the PrimaryReturn headline change.
func TestStatics_SurviveLiveSeries(t *testing.T) {
	live := LiveStats{
		PrimaryReturn:  map[string]float64{"1Y Return": 44.78},
		MaxDrawdownPct: -5.68, SortinoRatio: 4.23, SharpeRatio: 1.4,
		TotalReturnPct: 47.23, CAGRPct: 29.1,
	}
	d, err := overlayCatalog(live, true).ByID(context.Background(), "algo_manthan_v1")
	if err != nil {
		t.Fatal(err)
	}
	if d.MaxDrawdown != sheetMaxDD {
		t.Errorf("MaxDrawdown = %v, want the STATIC sheet value %v", d.MaxDrawdown, sheetMaxDD)
	}
	if d.KeyStats.Sortino != sheetSortino {
		t.Errorf("Sortino = %v, want the STATIC sheet value %v", d.KeyStats.Sortino, sheetSortino)
	}
	if d.PrimaryReturn["1Y Return"] != 44.78 {
		t.Errorf("PrimaryReturn headline must stay LIVE: %+v", d.PrimaryReturn)
	}
	// Additive series fields still overlay.
	if d.KeyStats.Sharpe != 1.4 || d.KeyStats.TotalReturnPct != 47.23 || d.KeyStats.CAGRPct != 29.1 {
		t.Errorf("additive series fields not applied: %+v", d.KeyStats)
	}
}

// Even a meaningful live closed-lot sample must NOT replace the sheet's
// trade stats any more.
func TestStatics_SurviveLiveTradeSample(t *testing.T) {
	live := LiveStats{
		TradeStatsLive: true, WinRatePct: 61.9, ProfitFactor: 2.1,
		TotalTrades: 42, AvgHoldingDays: 34.5,
	}
	d, err := overlayCatalog(live, true).ByID(context.Background(), "algo_manthan_v1")
	if err != nil {
		t.Fatal(err)
	}
	ks := d.KeyStats
	if ks.WinRatePct != sheetWinRate || ks.ProfitFactor != sheetPF ||
		ks.TotalTradesPct != sheetTrades || ks.AvgHoldingDays != sheetAvgHolding {
		t.Errorf("trade stats must stay STATIC per the sheet: %+v", ks)
	}
}

// Stats provider absent/failing → full statics, no zeroes.
func TestStatics_NoDataKeepsDefaults(t *testing.T) {
	d, err := overlayCatalog(LiveStats{}, false).ByID(context.Background(), "algo_manthan_v1")
	if err != nil {
		t.Fatal(err)
	}
	ks := d.KeyStats
	if d.MaxDrawdown != sheetMaxDD || ks.WinRatePct != sheetWinRate || ks.ProfitFactor != sheetPF ||
		ks.TotalTradesPct != sheetTrades || ks.AvgHoldingDays != sheetAvgHolding || ks.Sortino != sheetSortino {
		t.Errorf("catalog statics must survive a stats outage: MaxDD=%v %+v", d.MaxDrawdown, ks)
	}
}
