package performance

import (
	"math"
	"testing"
	"time"
)

func day(base time.Time, off int) time.Time { return base.AddDate(0, 0, off) }

// The honesty rule: a trailing window is reported ONLY when the series covers
// it. This is the 2026-08-11 regression — 1.49y of data was being presented as
// "3Y Return 28.4" / "2Y Return 32.9".
func TestComputeAlgoStats_NeverFabricatesUncoveredWindows(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	var rows []DailyRow
	for i := -400; i <= 0; i += 10 {
		rows = append(rows, DailyRow{Date: day(base, i), ReturnPct: float64(400+i) / 10}) // 0 → 40
	}

	st := ComputeAlgoStats(rows)
	if !st.Available {
		t.Fatal("series present but Available=false")
	}
	if _, ok := st.PrimaryReturn["2Y Return"]; ok {
		t.Error("2Y Return reported from ~1.1y of data — fabrication")
	}
	if _, ok := st.PrimaryReturn["3Y Return"]; ok {
		t.Error("3Y Return reported from ~1.1y of data — fabrication")
	}
	// 1Y IS covered: last=40, base row at −370 → 3.0, so 37.0
	if got := st.PrimaryReturn["1Y Return"]; math.Abs(got-37) > 0.01 {
		t.Errorf("1Y Return = %v, want 37", got)
	}
	if got := st.PrimaryReturn["Since Inception"]; math.Abs(got-40) > 0.01 {
		t.Errorf("Since Inception = %v, want 40 (latest cumulative)", got)
	}
	// Only the longest covered window + since-inception — the card has 2 slots.
	if len(st.PrimaryReturn) != 2 {
		t.Errorf("PrimaryReturn = %v, want exactly {longest covered window, Since Inception}", st.PrimaryReturn)
	}
}

// Short history: not even 1Y covered → falls back to a shorter window.
func TestComputeAlgoStats_ShortHistoryUsesShorterWindow(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	rows := []DailyRow{
		{Date: day(base, -100), ReturnPct: 0},
		{Date: day(base, -50), ReturnPct: 4},
		{Date: base, ReturnPct: 9},
	}
	st := ComputeAlgoStats(rows)
	if _, ok := st.PrimaryReturn["1Y Return"]; ok {
		t.Error("1Y reported from 100 days of data")
	}
	// 3M (90d) is covered: base row at −100 → 0, so 9 − 0 = 9
	if got := st.PrimaryReturn["3M Return"]; math.Abs(got-9) > 0.01 {
		t.Errorf("3M Return = %v, want 9 (got map %v)", got, st.PrimaryReturn)
	}
}

func TestComputeAlgoStats_MaxDrawdown(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	rows := []DailyRow{
		{Date: day(base, -20), ReturnPct: 0},   // equity 100
		{Date: day(base, -10), ReturnPct: 10},  // equity 110 (peak)
		{Date: day(base, -5), ReturnPct: -5},   // equity 95  → DD = (95-110)/110
		{Date: base, ReturnPct: 2},             // equity 102 (recovery)
	}
	st := ComputeAlgoStats(rows)
	want := round2((95.0 - 110.0) / 110.0 * 100) // -13.64
	if st.MaxDrawdownPct != want {
		t.Errorf("MaxDrawdownPct = %v, want %v", st.MaxDrawdownPct, want)
	}
	if st.MaxDrawdownPct > 0 {
		t.Error("drawdown must be negative or zero")
	}
}

func TestComputeAlgoStats_EmptyAndMonotonic(t *testing.T) {
	if st := ComputeAlgoStats(nil); st.Available || len(st.PrimaryReturn) != 0 {
		t.Errorf("empty series must yield Available=false and no returns, got %+v", st)
	}
	// Monotonic rise → no downside → Sortino reported as 0, never +Inf/NaN.
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	rows := []DailyRow{
		{Date: day(base, -2), ReturnPct: 0},
		{Date: day(base, -1), ReturnPct: 5},
		{Date: base, ReturnPct: 9},
	}
	st := ComputeAlgoStats(rows)
	if math.IsInf(st.SortinoRatio, 0) || math.IsNaN(st.SortinoRatio) {
		t.Errorf("Sortino must be finite, got %v", st.SortinoRatio)
	}
	if st.MaxDrawdownPct != 0 {
		t.Errorf("monotonic rise has no drawdown, got %v", st.MaxDrawdownPct)
	}
}

// Input slice must not be reordered by the computation.
func TestComputeAlgoStats_DoesNotMutateInput(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	rows := []DailyRow{
		{Date: base, ReturnPct: 9},
		{Date: day(base, -10), ReturnPct: 0},
	}
	_ = ComputeAlgoStats(rows)
	if !rows[0].Date.Equal(base) {
		t.Error("input slice was reordered")
	}
}

// ═══ 2026-08-20: production metrics — CAGR / Sharpe / TotalReturn ═══

// CAGR is only reported when the series covers ≥ 1 year — annualising a
// shorter span inflates the figure (the same fabrication rule as windows).
func TestComputeAlgoStats_CAGRHonesty(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	short := []DailyRow{
		{Date: day(base, -200), ReturnPct: 0},
		{Date: base, ReturnPct: 30},
	}
	if st := ComputeAlgoStats(short); st.CAGRPct != 0 {
		t.Errorf("CAGR from 200 days = %v, want 0 (never annualise <1y)", st.CAGRPct)
	}
	// Exactly 2 years, 0 → 44%: CAGR = sqrt(1.44)−1 = 20%.
	twoYears := []DailyRow{
		{Date: day(base, -730), ReturnPct: 0},
		{Date: base, ReturnPct: 44},
	}
	st := ComputeAlgoStats(twoYears)
	if math.Abs(st.CAGRPct-20) > 0.15 {
		t.Errorf("CAGR = %v, want ≈20 (sqrt(1.44)−1)", st.CAGRPct)
	}
	if st.TotalReturnPct != 44 {
		t.Errorf("TotalReturnPct = %v, want 44", st.TotalReturnPct)
	}
}

func TestComputeAlgoStats_SharpeFiniteAndSigned(t *testing.T) {
	base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	// Steadily rising with mild noise → positive, finite Sharpe.
	var rows []DailyRow
	cum := 0.0
	for i := -260; i <= 0; i++ {
		if i%2 == 0 {
			cum += 0.20
		} else {
			cum -= 0.05
		}
		rows = append(rows, DailyRow{Date: day(base, i), ReturnPct: cum})
	}
	st := ComputeAlgoStats(rows)
	if st.SharpeRatio <= 0 || math.IsInf(st.SharpeRatio, 0) || math.IsNaN(st.SharpeRatio) {
		t.Errorf("Sharpe = %v, want positive finite", st.SharpeRatio)
	}
	// Flat series → zero volatility → 0, never NaN.
	flat := []DailyRow{{Date: day(base, -2), ReturnPct: 5}, {Date: day(base, -1), ReturnPct: 5}, {Date: base, ReturnPct: 5}}
	if st := ComputeAlgoStats(flat); st.SharpeRatio != 0 {
		t.Errorf("flat Sharpe = %v, want 0", st.SharpeRatio)
	}
}
