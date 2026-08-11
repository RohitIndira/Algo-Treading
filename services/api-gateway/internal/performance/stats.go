package performance

// Headline algo statistics computed from the REAL daily performance series
// (stockk_market.algo_performance_daily, sourced from the Manthan sheet).
//
// WHY THIS EXISTS
//   The Explore card + algo detail page used to serve hardcoded constants
//   ("3Y Return: 28.4", "2Y Return: 32.9", maxDrawdown −12.6). On 2026-08-11
//   the reference client A844 had only 1.49 years of history (2025-02-11 →
//   2026-08-10), so the 2Y/3Y figures could not have come from data at all,
//   and the real max drawdown was −5.68% (not −12.6%). Real users were being
//   shown invented performance numbers. Everything here is derived; nothing
//   is assumed.
//
// SEMANTICS
//   DailyRow.ReturnPct is CUMULATIVE return since algo inception as of that
//   date (verified: pnl_amount = investment_amount × return_pct/100). So:
//     since-inception = last.ReturnPct
//     window return   = last.ReturnPct − ReturnPct(as of last.Date − window)
//     equity curve    = 100 + ReturnPct
//
// HONESTY RULE
//   A window is only reported when the series actually covers it
//   (first row on/before the window start). We never extrapolate a 2Y or 3Y
//   number out of 1.5Y of data — the window is simply omitted.

import (
	"math"
	"sort"
	"time"
)

// tradingDaysPerYear is the standard annualisation factor for Indian equity
// daily series (NSE trades ~250 sessions/yr; 252 is the conventional value).
const tradingDaysPerYear = 252

// AlgoStats is the computed headline set for one algo.
type AlgoStats struct {
	// PrimaryReturn maps a display label → return %, e.g.
	// {"1Y Return": 41.52, "Since Inception": 42.37}. Only windows the data
	// genuinely covers appear here.
	PrimaryReturn map[string]float64

	// MaxDrawdownPct is the worst peak-to-trough decline on the equity
	// curve, negative (e.g. -5.68).
	MaxDrawdownPct float64

	// SortinoRatio is annualised, risk-free = 0, using downside deviation.
	// Zero when there is no downside volatility (or too few points).
	SortinoRatio float64

	// AsOf is the last date in the series; Days is its span.
	AsOf time.Time
	Days int

	// Available is false when the series is empty — callers should then keep
	// whatever defaults they already had rather than render zeros.
	Available bool
}

// statWindow is a candidate reporting window, longest first.
var statWindows = []struct {
	label string
	days  int
}{
	{"3Y Return", 3 * 365},
	{"2Y Return", 2 * 365},
	{"1Y Return", 365},
	{"6M Return", 180},
	{"3M Return", 90},
	{"1M Return", 30},
}

// ComputeAlgoStats derives the headline stats from a daily series. Pure — no
// IO, no clock — so it is fully unit-testable. Input need not be sorted.
func ComputeAlgoStats(rows []DailyRow) AlgoStats {
	out := AlgoStats{PrimaryReturn: map[string]float64{}}
	if len(rows) == 0 {
		return out
	}

	// Copy + sort ascending so callers' slices are never mutated.
	series := append([]DailyRow(nil), rows...)
	sort.Slice(series, func(i, j int) bool { return series[i].Date.Before(series[j].Date) })

	first, last := series[0], series[len(series)-1]
	out.Available = true
	out.AsOf = last.Date
	out.Days = int(last.Date.Sub(first.Date).Hours() / 24)

	// Since inception — the cumulative return as of the latest row.
	out.PrimaryReturn["Since Inception"] = round2(last.ReturnPct)

	// Longest FULLY-COVERED trailing window. Reporting only the longest
	// keeps the card to its two-slot design ({window, since-inception}) and
	// self-upgrades: once 2 years of history exists, "2Y Return" appears
	// automatically with no code change.
	for _, w := range statWindows {
		start := last.Date.AddDate(0, 0, -w.days)
		if first.Date.After(start) {
			continue // series doesn't reach back far enough — never fabricate
		}
		base, ok := returnAsOf(series, start)
		if !ok {
			continue
		}
		out.PrimaryReturn[w.label] = round2(last.ReturnPct - base)
		break
	}

	out.MaxDrawdownPct = maxDrawdownPct(series)
	out.SortinoRatio = sortinoRatio(series)
	return out
}

// returnAsOf returns the cumulative return on the last row at/before `t`.
func returnAsOf(series []DailyRow, t time.Time) (float64, bool) {
	val, ok := 0.0, false
	for _, r := range series {
		if r.Date.After(t) {
			break
		}
		val, ok = r.ReturnPct, true
	}
	return val, ok
}

// maxDrawdownPct is the worst peak-to-trough decline of the equity curve
// (100 + cumulative return), as a negative percentage.
func maxDrawdownPct(series []DailyRow) float64 {
	peak := math.Inf(-1)
	worst := 0.0
	for _, r := range series {
		equity := 100 + r.ReturnPct
		if equity > peak {
			peak = equity
		}
		if peak > 0 {
			if dd := (equity - peak) / peak * 100; dd < worst {
				worst = dd
			}
		}
	}
	return round2(worst)
}

// sortinoRatio is the annualised Sortino (risk-free = 0) over daily equity
// returns. Downside deviation uses only negative daily returns; when there
// are none (or <2 points) the ratio is reported as 0 rather than +Inf.
func sortinoRatio(series []DailyRow) float64 {
	if len(series) < 2 {
		return 0
	}
	daily := make([]float64, 0, len(series)-1)
	for i := 1; i < len(series); i++ {
		prev := 100 + series[i-1].ReturnPct
		curr := 100 + series[i].ReturnPct
		if prev <= 0 {
			continue
		}
		daily = append(daily, curr/prev-1)
	}
	if len(daily) < 2 {
		return 0
	}
	var sum, downSq float64
	for _, r := range daily {
		sum += r
		if r < 0 {
			downSq += r * r
		}
	}
	mean := sum / float64(len(daily))
	downDev := math.Sqrt(downSq / float64(len(daily)))
	if downDev == 0 {
		return 0 // no downside observed — an "infinite" Sortino is not a number to show
	}
	return round2(mean / downDev * math.Sqrt(tradingDaysPerYear))
}
