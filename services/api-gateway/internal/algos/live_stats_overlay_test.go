package algos

// The 2026-08-20 overlay contract: series-derived stats (drawdown, Sortino,
// Sharpe, total return, CAGR) apply whenever computed; trade-derived stats
// (win rate, profit factor, trades, holding) apply ONLY when the live
// closed-lot sample passed the threshold (TradeStatsLive) — otherwise the
// operator's track-record figures stand. Regression: MaxDrawdown was
// computed but never applied (card kept −17 while the data said −5.68).

import (
	"context"
	"testing"
)

func overlayCatalog(live LiveStats, ok bool) *StaticCatalog {
	c := NewStaticCatalog().(*StaticCatalog)
	c.SetStatsProvider(func(ctx context.Context, algoID string) (LiveStats, bool) { return live, ok })
	return c
}

func TestOverlay_SeriesStatsAlwaysApply(t *testing.T) {
	live := LiveStats{
		PrimaryReturn:  map[string]float64{"1Y Return": 41.5, "Since Inception": 47.23},
		MaxDrawdownPct: -5.68, SortinoRatio: 1.9, SharpeRatio: 1.4,
		TotalReturnPct: 47.23, CAGRPct: 29.1,
	}
	d, err := overlayCatalog(live, true).ByID(context.Background(), "algo_manthan_v1")
	if err != nil {
		t.Fatal(err)
	}
	if d.MaxDrawdown != -5.68 {
		t.Errorf("MaxDrawdown = %v, want −5.68 (the never-applied overlay bug)", d.MaxDrawdown)
	}
	if d.KeyStats.Sortino != 1.9 || d.KeyStats.Sharpe != 1.4 ||
		d.KeyStats.TotalReturnPct != 47.23 || d.KeyStats.CAGRPct != 29.1 {
		t.Errorf("series key stats not applied: %+v", d.KeyStats)
	}
	// Trade stats NOT live → operator track-record figures must remain.
	if d.KeyStats.WinRatePct != 54.63 || d.KeyStats.TotalTradesPct != 205 {
		t.Errorf("track-record stats must stand below threshold: %+v", d.KeyStats)
	}
}

func TestOverlay_TradeStatsApplyOnlyWhenLive(t *testing.T) {
	live := LiveStats{
		PrimaryReturn:  map[string]float64{"Since Inception": 47.23},
		TradeStatsLive: true, WinRatePct: 61.9, ProfitFactor: 2.1,
		TotalTrades: 42, AvgHoldingDays: 34.5,
	}
	d, err := overlayCatalog(live, true).ByID(context.Background(), "algo_manthan_v1")
	if err != nil {
		t.Fatal(err)
	}
	ks := d.KeyStats
	if ks.WinRatePct != 61.9 || ks.ProfitFactor != 2.1 || ks.TotalTradesPct != 42 || ks.AvgHoldingDays != 34.5 {
		t.Errorf("live trade stats not applied: %+v", ks)
	}
}

func TestOverlay_NoDataKeepsDefaults(t *testing.T) {
	d, err := overlayCatalog(LiveStats{}, false).ByID(context.Background(), "algo_manthan_v1")
	if err != nil {
		t.Fatal(err)
	}
	if d.MaxDrawdown != -17 || d.KeyStats.WinRatePct != 54.63 || d.KeyStats.Sortino != 2.12 {
		t.Errorf("catalog defaults must survive a stats outage: %+v", d.KeyStats)
	}
}
