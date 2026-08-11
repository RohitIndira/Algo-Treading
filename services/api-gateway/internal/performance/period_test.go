package performance

import (
	"math"
	"testing"
	"time"
)

// 2026-08-11 regression: the endpoint ignored ?period=, so toggling the
// timeframe left the "vs NIFTY 50" benchmark number frozen while the algo
// line appeared to move. BOTH series must be sliced to the window and
// re-indexed to the same baseline.
func TestBuild_PeriodTogglesBothSeries(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	var daily []DailyRow
	var bench []BenchmarkRow
	// 400 days: algo cumulative 0→40; nifty 100→140 (both +40% over the span).
	for i := -400; i <= 0; i++ {
		frac := float64(400+i) / 400
		daily = append(daily, DailyRow{
			Date: base.AddDate(0, 0, i), ReturnPct: 40 * frac, InvestmentAmount: 500000,
		})
		bench = append(bench, BenchmarkRow{
			Date: base.AddDate(0, 0, i), CloseValue: 100 * (1 + 0.40*frac),
		})
	}

	all := Build("a", "A844", "NIFTY 50", daily, bench, "All")
	m1 := Build("a", "A844", "NIFTY 50", daily, bench, "1M")

	if all.Period != "All" || m1.Period != "1M" {
		t.Fatalf("period not echoed: all=%q m1=%q", all.Period, m1.Period)
	}

	// Full span: algo ≈ +40%, benchmark ≈ +40%.
	if math.Abs(all.VsBenchmark.Algo.ReturnPct-40) > 0.2 {
		t.Errorf("All algo = %v, want ~40", all.VsBenchmark.Algo.ReturnPct)
	}
	if math.Abs(all.VsBenchmark.Benchmark.ReturnPct-40) > 0.2 {
		t.Errorf("All benchmark = %v, want ~40", all.VsBenchmark.Benchmark.ReturnPct)
	}

	// 1M window: both must SHRINK to roughly one month of the move (~3%),
	// and critically the benchmark must NOT stay at its full-span value.
	if m1.VsBenchmark.Benchmark.ReturnPct >= all.VsBenchmark.Benchmark.ReturnPct-1 {
		t.Errorf("benchmark did not shrink with the 1M toggle: all=%v 1M=%v (the reported bug)",
			all.VsBenchmark.Benchmark.ReturnPct, m1.VsBenchmark.Benchmark.ReturnPct)
	}
	if m1.VsBenchmark.Algo.ReturnPct >= all.VsBenchmark.Algo.ReturnPct-1 {
		t.Errorf("algo did not shrink with the 1M toggle: all=%v 1M=%v",
			all.VsBenchmark.Algo.ReturnPct, m1.VsBenchmark.Algo.ReturnPct)
	}

	// Chart must be re-indexed to 100 at the WINDOW start for both series.
	p0 := m1.Chart.Points[0]
	if p0.AlgoPct == nil || math.Abs(*p0.AlgoPct-100) > 0.01 {
		t.Errorf("1M chart algo baseline = %v, want 100", p0.AlgoPct)
	}
	if p0.BenchmarkPct == nil || math.Abs(*p0.BenchmarkPct-100) > 0.01 {
		t.Errorf("1M chart benchmark baseline = %v, want 100", p0.BenchmarkPct)
	}
	// 1M chart must be shorter than the full chart.
	if len(m1.Chart.Points) >= len(all.Chart.Points) {
		t.Errorf("1M chart not sliced: %d points vs all %d", len(m1.Chart.Points), len(all.Chart.Points))
	}
}

// Unknown / empty period falls back to the full series rather than erroring.
func TestBuild_UnknownPeriodFallsBackToAll(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	daily := []DailyRow{
		{Date: base.AddDate(0, 0, -10), ReturnPct: 0, InvestmentAmount: 500000},
		{Date: base, ReturnPct: 9, InvestmentAmount: 500000},
	}
	for _, p := range []string{"", "banana", "all", "ALL"} {
		got := Build("a", "A844", "NIFTY 50", daily, nil, p)
		if got.Period != "All" {
			t.Errorf("period %q → %q, want All", p, got.Period)
		}
		if len(got.Chart.Points) != 2 {
			t.Errorf("period %q sliced unexpectedly: %d points", p, len(got.Chart.Points))
		}
	}
}

// The returns tiles (1M/3M/6M/1Y/since) are FIXED windows shown as a card row.
// They must be identical no matter which chart period is selected. Regression:
// slicing leaked into them, so ?period=1M returned "1M": 42.37 (the since-
// inception value) instead of the real 1M figure.
func TestBuild_ReturnsTilesAreIndependentOfPeriod(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	var daily []DailyRow
	var bench []BenchmarkRow
	for i := -400; i <= 0; i++ {
		frac := float64(400+i) / 400
		daily = append(daily, DailyRow{Date: base.AddDate(0, 0, i), ReturnPct: 40 * frac, InvestmentAmount: 500000})
		bench = append(bench, BenchmarkRow{Date: base.AddDate(0, 0, i), CloseValue: 100 * (1 + 0.40*frac)})
	}

	all := Build("a", "A844", "NIFTY 50", daily, bench, "All")
	for _, p := range []string{"1M", "3M", "6M", "1Y"} {
		got := Build("a", "A844", "NIFTY 50", daily, bench, p)
		if got.Returns != all.Returns {
			t.Errorf("period=%s changed the returns tiles:\n  got %+v\n  want %+v", p, got.Returns, all.Returns)
		}
		// Calendars must stay full-span too.
		if len(got.DailyPnL) != len(all.DailyPnL) {
			t.Errorf("period=%s sliced dailyPnL: %d vs %d", p, len(got.DailyPnL), len(all.DailyPnL))
		}
		if len(got.MonthlyPnL) != len(all.MonthlyPnL) {
			t.Errorf("period=%s sliced monthlyPnL: %d vs %d", p, len(got.MonthlyPnL), len(all.MonthlyPnL))
		}
	}
	// And the 1M tile must be a real 1M number, not the since-inception total.
	if all.Returns.Month1.Percent >= all.Returns.SinceDeployment.Percent {
		t.Errorf("1M tile (%v) should be well below since-inception (%v)",
			all.Returns.Month1.Percent, all.Returns.SinceDeployment.Percent)
	}
}

// Day-wise P&L cells must be that DAY's change, not the running total, and
// must telescope to the since-inception figure. Regression 2026-08-11: the
// August cells each read ~₹1.7-2.1 lakh (the cumulative book P&L).
func TestBuild_DailyPnLIsPerDayNotCumulative(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	// Cumulative series mirroring the real Aug rows.
	daily := []DailyRow{
		{Date: base.AddDate(0, 0, -10), ReturnPct: 31.87, PnLAmount: 159360, InvestmentAmount: 500000},
		{Date: base.AddDate(0, 0, -7), ReturnPct: 34.06, PnLAmount: 170321, InvestmentAmount: 500000},
		{Date: base.AddDate(0, 0, -6), ReturnPct: 34.88, PnLAmount: 174409, InvestmentAmount: 500000},
		{Date: base, ReturnPct: 42.37, PnLAmount: 211846, InvestmentAmount: 500000},
	}
	got := Build("a", "A844", "NIFTY 50", daily, nil, "All")

	wantAmt := []int64{159360, 10961, 4088, 37437} // first = from deployment, then deltas
	var sum int64
	for i, p := range got.DailyPnL {
		if p.Amount != wantAmt[i] {
			t.Errorf("day %s amount = %d, want %d", p.Date, p.Amount, wantAmt[i])
		}
		sum += p.Amount
		if p.Amount > 100000 && i > 0 {
			t.Errorf("day %s amount %d looks cumulative (lakh-scale), not a daily change", p.Date, p.Amount)
		}
	}
	// Telescoping: the cells must sum to the since-inception total.
	if sum != 211846 {
		t.Errorf("daily cells sum to %d, want 211846 (must reconcile to since-inception)", sum)
	}
	// And the last day's % must be the day's move, not the running 42.37.
	last := got.DailyPnL[len(got.DailyPnL)-1]
	if last.Percent != round2(42.37-34.88) {
		t.Errorf("last day percent = %v, want %v", last.Percent, round2(42.37-34.88))
	}
}
