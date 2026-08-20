package handlers

// metricsFromChart: the deployed-strategy tiles are derived from the page's
// own chart curve so they can never contradict it. Honesty gates pinned here.

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

func chartOf(start time.Time, pcts ...float64) livealgos.DetailsChart {
	var pts []livealgos.DetailsChartPoint
	for i, p := range pcts {
		pts = append(pts, livealgos.DetailsChartPoint{Date: start.AddDate(0, 0, i).Format("2006-01-02"), Pct: p})
	}
	return livealgos.DetailsChart{Points: pts}
}

func TestMetricsFromChart_DrawdownFromCurve(t *testing.T) {
	start := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	// 100 → 110 (peak) → 99 (dd −10%) → 104
	dd, sharpe, cagr := metricsFromChart(chartOf(start, 100, 110, 99, 104))
	if math.Abs(dd-(-10)) > 0.01 {
		t.Errorf("maxDD = %v, want −10", dd)
	}
	// 4 points: too short for Sharpe (needs 31) and CAGR (needs 1y).
	if sharpe != 0 || cagr != 0 {
		t.Errorf("short deployment must gate sharpe/cagr: sharpe=%v cagr=%v", sharpe, cagr)
	}
}

func TestMetricsFromChart_YearLongDeployment(t *testing.T) {
	start := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)
	pcts := make([]float64, 0, 400)
	v := 100.0
	for i := 0; i < 400; i++ {
		if i%3 == 2 {
			v -= 0.10
		} else {
			v += 0.15
		}
		pcts = append(pcts, v)
	}
	dd, sharpe, cagr := metricsFromChart(chartOf(start, pcts...))
	if dd >= 0 {
		t.Errorf("maxDD = %v, want negative", dd)
	}
	if sharpe <= 0 || math.IsInf(sharpe, 0) || math.IsNaN(sharpe) {
		t.Errorf("sharpe = %v, want positive finite", sharpe)
	}
	// ~400 days, 100 → ~126.6: CAGR slightly under the total return.
	if cagr <= 0 || cagr > 30 {
		t.Errorf("cagr = %v, want (0,30)", cagr)
	}
}

func TestMetricsFromChart_DegenerateInputs(t *testing.T) {
	if dd, sh, cg := metricsFromChart(livealgos.DetailsChart{}); dd != 0 || sh != 0 || cg != 0 {
		t.Error("empty chart must be all zeros")
	}
	start := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	// Flat 40-day curve: no drawdown, zero volatility → sharpe 0 (never NaN).
	flat := make([]float64, 40)
	for i := range flat {
		flat[i] = 100
	}
	dd, sh, cg := metricsFromChart(chartOf(start, flat...))
	if dd != 0 || sh != 0 || cg != 0 {
		t.Errorf("flat: dd=%v sharpe=%v cagr=%v, want zeros", dd, sh, cg)
	}
	_ = fmt.Sprint()
}
