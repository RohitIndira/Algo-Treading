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

// ═══ trueCurve: the TRUE deployment chart from NAV snapshots ═══

func navPt(y, m, d int, pct float64) livealgos.NAVPoint {
	return livealgos.NAVPoint{Date: time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), NetPnLPct: pct}
}

func TestTrueCurve_AnchorsSnapshotsAndLivePoint(t *testing.T) {
	deployed := time.Date(2026, 8, 11, 6, 35, 0, 0, time.UTC)
	now := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	c := trueCurve([]livealgos.NAVPoint{navPt(2026, 8, 20, 0.42), navPt(2026, 8, 21, 0.90)}, deployed, 1.35, now)
	want := []struct {
		date string
		pct  float64
	}{
		{"2026-08-11", 100},    // deploy anchor (precedes first snapshot)
		{"2026-08-20", 100.42}, // settled snapshots
		{"2026-08-21", 100.90},
		{"2026-08-22", 101.35}, // today's LIVE value ends the curve
	}
	if len(c.Points) != len(want) {
		t.Fatalf("points = %v", c.Points)
	}
	for i, w := range want {
		if c.Points[i].Date != w.date || math.Abs(c.Points[i].Pct-w.pct) > 0.001 {
			t.Errorf("point %d = %+v, want %+v", i, c.Points[i], w)
		}
	}
}

func TestTrueCurve_TodaySnapshotReplacedByLive(t *testing.T) {
	// Intraday: today's snapshot row exists (30-min upsert) but the response
	// must show the CURRENT header value, not the stale snapshot.
	deployed := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC)
	c := trueCurve([]livealgos.NAVPoint{navPt(2026, 8, 20, 0.10)}, deployed, 0.55, now)
	last := c.Points[len(c.Points)-1]
	if last.Date != "2026-08-20" || math.Abs(last.Pct-100.55) > 0.001 {
		t.Errorf("today's point = %+v, want live 100.55", last)
	}
	for i, p := range c.Points[:len(c.Points)-1] {
		if p.Date == "2026-08-20" {
			t.Errorf("stale today snapshot survived at %d: %+v", i, p)
		}
	}
}
