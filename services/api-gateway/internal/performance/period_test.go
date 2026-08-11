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
