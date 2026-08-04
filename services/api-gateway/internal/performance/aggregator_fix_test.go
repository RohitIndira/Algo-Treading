package performance

// Regression coverage for the 2026-08-04 performance-tile fixes:
//   1. Month-wise P&L must be the CHANGE in cumulative return per month (delta),
//      NOT the sum of daily cumulative values — and must reconcile to the
//      since-deployment total.
//   2. Benchmark (NIFTY) return must be computed PER window so it differs across
//      1M/3M/6M/1Y instead of showing one static number.

import (
	"testing"
	"time"
)

func d(s string) time.Time { t, _ := time.Parse("2006-01-02", s); return t }

func TestBuildMonthlyPnL_DeltaReconcilesToSinceDeployment(t *testing.T) {
	// return_pct is cumulative "as of" each date.
	daily := []DailyRow{
		{Date: d("2026-01-10"), ReturnPct: 3, InvestmentAmount: 500000},
		{Date: d("2026-01-31"), ReturnPct: 5, InvestmentAmount: 500000},  // Jan end 5   (+5)
		{Date: d("2026-02-28"), ReturnPct: 12, InvestmentAmount: 500000}, // Feb end 12  (+7)
		{Date: d("2026-03-31"), ReturnPct: 10, InvestmentAmount: 500000}, // Mar end 10  (-2, drawdown)
		{Date: d("2026-04-30"), ReturnPct: 20, InvestmentAmount: 500000}, // Apr end 20  (+10)
	}
	mp := buildMonthlyPnL(daily, 500000)
	if len(mp) != 4 {
		t.Fatalf("want 4 months, got %d", len(mp))
	}
	wantPct := []float64{5, 7, -2, 10}
	sum := 0.0
	for i, m := range mp {
		if m.Percent != wantPct[i] {
			t.Errorf("month %d pct = %v, want %v", i, m.Percent, wantPct[i])
		}
		sum += m.Percent
	}
	// Months must sum to the since-deployment total (latest cumulative = 20).
	if round2(sum) != 20 {
		t.Errorf("months sum = %v, want 20 (== since-deployment)", sum)
	}
	// Amount uses capital * delta/100 → Feb +7% of 5,00,000 = 35,000.
	if mp[1].Amount != 35000 {
		t.Errorf("Feb amount = %d, want 35000", mp[1].Amount)
	}
	// The OLD summed-cumulative bug would have made Jan alone = 3+5 = 8%; assert
	// we're NOT doing that.
	if mp[0].Percent == 8 {
		t.Errorf("Jan pct = 8 → still summing cumulative (bug not fixed)")
	}
}

func TestBenchmarkReturnPct_VariesPerWindow(t *testing.T) {
	bench := []BenchmarkRow{
		{Date: d("2026-01-01"), CloseValue: 100},
		{Date: d("2026-02-01"), CloseValue: 110}, // +10% vs Jan
		{Date: d("2026-03-01"), CloseValue: 121}, // +21% vs Jan, +10% vs Feb
	}
	full := benchmarkReturnPct(bench, d("2026-01-01"), d("2026-03-01"))
	feb := benchmarkReturnPct(bench, d("2026-02-01"), d("2026-03-01"))
	if full != 21 {
		t.Errorf("full window = %v, want 21", full)
	}
	if feb != 10 {
		t.Errorf("feb→mar window = %v, want 10", feb)
	}
	if full == feb {
		t.Errorf("benchmark must differ per window; both = %v (the static-value bug)", full)
	}
	// Unbracketable window → 0 (frontend hides benchmark line).
	if got := benchmarkReturnPct(nil, d("2026-01-01"), d("2026-03-01")); got != 0 {
		t.Errorf("no bench data → %v, want 0", got)
	}
}
