package performance

import (
	"time"
)

// Build assembles the final Response for the /algos/{id}/performance
// endpoint from raw DailyRow + BenchmarkRow data.
//
// Semantics of algo_performance_daily.return_pct and pnl_amount:
//
//	Empirically, pnl_amount = investment_amount * (return_pct / 100)
//	so return_pct IS the "position return to date" as-of that day —
//	NOT a per-day change. Interpretation:
//	- On day N, return_pct = current unrealized P&L / investment
//	- The value can swing (positions close and reopen)
//	- The last row's return_pct IS the "current" return the user cares about
//
// For the chart line, we use return_pct directly as an "indexed at
// 100" cumulative curve — i.e., 100 + return_pct. That gives the
// mockup's growth-curve look.
//
// For the 1M / 6M / 1Y / Since Deployment tiles, we take the return_pct
// AS OF (latest date) minus AS OF (window start date). Same for the
// benchmark. This matches how the mobile app will visually compare
// them.
func Build(
	algoID, referenceClientID, benchmarkLabel string,
	daily []DailyRow,
	bench []BenchmarkRow,
) Response {
	if len(daily) == 0 {
		return Response{
			AlgoID:            algoID,
			ReferenceClientID: referenceClientID,
			Disclaimer:        defaultDisclaimer(),
		}
	}

	latest := daily[len(daily)-1]

	// Capital base is the reference client's actual investment. Displayed
	// on-screen as a footnote — the user chose "show A844's actual
	// numbers as-is" so this shows ₹5 Lac for A844.
	capitalBase := int64(latest.InvestmentAmount)

	// ── Chart ─────────────────────────────────────────────────────────
	// Overlay algo + benchmark on the same date range. We index BOTH
	// series at 100 as of the algo's first date so the "vs benchmark"
	// visual comparison starts from the same baseline.
	chartPoints := buildChartPoints(daily, bench)

	// ── VsBenchmark headline (latest day) ─────────────────────────────
	// The +8.26% vs +1.37% row above the chart. Uses the last available
	// value for each series over the algo's date span.
	algoLatestPct := latest.ReturnPct
	benchmarkLatestPct := latestBenchmarkPct(chartPoints)

	vsBenchmark := VsBenchmark{
		Algo:      Series{Label: "Manthan", ReturnPct: round2(algoLatestPct)},
		Benchmark: Series{Label: benchmarkLabel, ReturnPct: round2(benchmarkLatestPct)},
	}

	// ── Returns tiles (1M / 3M / 6M / 1Y / since deployment) ─────────
	// Each tile carries the benchmark's return over the SAME window, so the
	// "Nifty" number differs per timeframe instead of showing one static value.
	returns := Returns{
		Month1:          buildReturn(daily, bench, 30, capitalBase),
		Month3:          buildReturn(daily, bench, 90, capitalBase),
		Month6:          buildReturn(daily, bench, 180, capitalBase),
		Year1:           buildReturn(daily, bench, 365, capitalBase),
		SinceDeployment: sinceDeployment(daily, bench, capitalBase),
	}

	// ── DailyPnL (heat-map calendar) ─────────────────────────────────
	// Pass the raw daily rows through with %/₹.
	dailyPnL := make([]DailyPoint, 0, len(daily))
	for _, r := range daily {
		dailyPnL = append(dailyPnL, DailyPoint{
			Date:    r.Date.Format("2006-01-02"),
			Amount:  int64(r.PnLAmount),
			Percent: round2(r.ReturnPct),
		})
	}

	// ── MonthlyPnL — cumulative-return delta per month ──────────────
	monthlyPnL := buildMonthlyPnL(daily, capitalBase)

	return Response{
		AlgoID:            algoID,
		ReferenceClientID: referenceClientID,
		AsOf:              latest.Date.Format("2006-01-02"),
		CapitalBase:       capitalBase,
		VsBenchmark:       vsBenchmark,
		Chart:             Chart{Points: chartPoints},
		Returns:           returns,
		DailyPnL:          dailyPnL,
		MonthlyPnL:        monthlyPnL,
		Disclaimer:        defaultDisclaimer(),
	}
}

// buildChartPoints creates one ChartPoint per algo date, overlaying the
// nearest benchmark close from `bench`. Both series are indexed at 100
// at the algo's first date — the natural "since deployment" baseline.
func buildChartPoints(daily []DailyRow, bench []BenchmarkRow) []ChartPoint {
	if len(daily) == 0 {
		return nil
	}
	// Map benchmark date → close for O(1) lookup, and remember first
	// benchmark close ON OR AFTER the algo's first date so we can
	// re-index at 100.
	firstAlgoDate := daily[0].Date
	benchByDate := make(map[string]float64, len(bench))
	var benchmarkBaseline float64
	for _, b := range bench {
		if b.Date.Before(firstAlgoDate) {
			continue
		}
		if benchmarkBaseline == 0 {
			benchmarkBaseline = b.CloseValue
		}
		benchByDate[b.Date.Format("2006-01-02")] = b.CloseValue
	}

	pts := make([]ChartPoint, 0, len(daily))
	for _, d := range daily {
		p := ChartPoint{
			Date: d.Date.Format("2006-01-02"),
		}
		// Algo: 100 + return_pct — the "growth from ₹100 base" curve.
		algoIdx := round2(100 + d.ReturnPct)
		p.AlgoPct = &algoIdx

		// Benchmark: index against the first close on/after algo start.
		if benchmarkBaseline > 0 {
			if close, ok := benchByDate[d.Date.Format("2006-01-02")]; ok {
				bIdx := round2(close / benchmarkBaseline * 100)
				p.BenchmarkPct = &bIdx
			}
		}
		pts = append(pts, p)
	}
	return pts
}

// latestBenchmarkPct returns the last non-nil BenchmarkPct across the
// chart points, expressed as % change from baseline (e.g., 108.5 →
// +8.5%). If no benchmark data is available, returns 0 (frontend can
// hide the vs-benchmark row).
func latestBenchmarkPct(pts []ChartPoint) float64 {
	for i := len(pts) - 1; i >= 0; i-- {
		if pts[i].BenchmarkPct != nil {
			return round2(*pts[i].BenchmarkPct - 100)
		}
	}
	return 0
}

// buildReturn computes the return over the trailing `days`-day window
// using return_pct AS OF (last) minus AS OF (~days ago). If we don't
// have `days` days of data, returns the same as sinceDeployment.
func buildReturn(daily []DailyRow, bench []BenchmarkRow, days int, capital int64) ReturnEntry {
	if len(daily) == 0 {
		return ReturnEntry{}
	}
	latest := daily[len(daily)-1]
	// Find the row closest to (latest.Date - days days), NOT going past.
	target := latest.Date.AddDate(0, 0, -days)
	if target.Before(daily[0].Date) {
		return sinceDeployment(daily, bench, capital)
	}
	// Binary-ish search: linear is fine for ~150 rows.
	startIdx := 0
	for i, r := range daily {
		if r.Date.After(target) {
			break
		}
		startIdx = i
	}
	windowStart := daily[startIdx]
	pct := latest.ReturnPct - windowStart.ReturnPct
	amount := int64(float64(capital) * pct / 100.0)
	return ReturnEntry{
		Amount:           amount,
		Percent:          round2(pct),
		BenchmarkPercent: benchmarkReturnPct(bench, windowStart.Date, latest.Date),
	}
}

// sinceDeployment is the return from first row to last row.
func sinceDeployment(daily []DailyRow, bench []BenchmarkRow, capital int64) ReturnEntry {
	if len(daily) == 0 {
		return ReturnEntry{}
	}
	latest := daily[len(daily)-1]
	// Since deployment, the return AS OF latest date IS the total return.
	pct := latest.ReturnPct
	amount := int64(float64(capital) * pct / 100.0)
	return ReturnEntry{
		Amount:           amount,
		Percent:          round2(pct),
		BenchmarkPercent: benchmarkReturnPct(bench, daily[0].Date, latest.Date),
	}
}

// benchmarkReturnPct is the benchmark's % change over [from, to], using the
// last close on/before each bound (bench is date-ascending). Returns 0 when the
// window can't be bracketed — the frontend then just shows the algo line. This
// is what makes the "Nifty" number differ per 1M/3M/6M/1Y window instead of a
// single static value.
func benchmarkReturnPct(bench []BenchmarkRow, from, to time.Time) float64 {
	var fromClose, toClose float64
	for _, b := range bench {
		if !b.Date.After(from) {
			fromClose = b.CloseValue // last close on/before `from`
		}
		if !b.Date.After(to) {
			toClose = b.CloseValue // last close on/before `to`
		}
	}
	if fromClose <= 0 || toClose <= 0 {
		return 0
	}
	return round2((toClose/fromClose - 1) * 100)
}

// buildMonthlyPnL computes each month's P&L as the CHANGE in cumulative return
// over that month — (last row of the month) minus (last row of the previous
// month). return_pct is a cumulative "as of" value (see Build's doc), so the
// old code that SUMMED daily pnl_amount / return_pct double-counted massively
// and produced nonsense month totals. With the delta approach the months sum
// exactly to the since-deployment return (e.g. months add up to +34.9%).
func buildMonthlyPnL(daily []DailyRow, capital int64) []MonthlyPoint {
	type key struct{ y, m int }
	lastOfMonth := make(map[key]DailyRow)
	order := make([]key, 0)
	for _, r := range daily {
		k := key{r.Date.Year(), int(r.Date.Month())}
		if _, seen := lastOfMonth[k]; !seen {
			order = append(order, k)
		}
		lastOfMonth[k] = r // daily is date-ascending → last write is the month-end row
	}
	out := make([]MonthlyPoint, 0, len(order))
	prevCum := 0.0 // cumulative return at the end of the previous month
	for _, k := range order {
		cum := lastOfMonth[k].ReturnPct
		delta := cum - prevCum
		prevCum = cum
		out = append(out, MonthlyPoint{
			Year:    k.y,
			Month:   k.m,
			Amount:  int64(float64(capital) * delta / 100.0),
			Percent: round2(delta),
		})
	}
	return out
}

// round2 rounds to 2 decimal places (money / percentages don't need more).
func round2(f float64) float64 {
	shifted := f * 100
	if shifted >= 0 {
		return float64(int64(shifted+0.5)) / 100
	}
	return float64(int64(shifted-0.5)) / 100
}

func defaultDisclaimer() string {
	return "Performance shown is based on reference account A844's actual returns. Past performance does not guarantee future returns. Manthan trades equities only; capital can be temporarily locked during rebalancing windows."
}

// helper for future-proofing — currently unused
var _ = time.Duration(0)
