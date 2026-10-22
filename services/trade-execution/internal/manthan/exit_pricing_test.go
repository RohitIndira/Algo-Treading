package manthan

import (
	"math"
	"testing"
)

func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// exitLimitFor is the ONLY pricing rule for an immediate exit (marketable
// LIMIT). 2026-10-22: Market / SL-M are rejected for algo-tagged orders, so
// this price decides whether a stopped-out position actually leaves the book.
func TestExitLimitFor(t *testing.T) {
	cases := []struct {
		name                    string
		ltp, tick, lo, hi, slip float64
		want                    float64
		wantErr                 bool
	}{
		{"2pct below ltp", 200, 0.05, 180, 220, 2, 196.00, false},
		{"floored at the lower band", 185, 0.05, 183, 220, 2, 183, false},
		{"no ltp → band floor", 0, 0.05, 183, 220, 2, 183, false},
		{"no ltp, no band → error", 0, 0.05, 0, 0, 2, 0, true},
		{"tick rounding 0.05", 100.37, 0.05, 0, 0, 2, 98.35, false},
		{"tick rounding 0.50 (APARINDS-like)", 15220, 0.5, 14000, 16000, 2, 14915.50, false},
		{"never above the upper band", 300, 0.05, 180, 220, 0.1, 220, false},
		{"zero tick falls back to 0.05", 50, 0, 0, 0, 2, 49.00, false},
	}
	for _, c := range cases {
		got, err := exitLimitFor(c.ltp, c.tick, c.lo, c.hi, c.slip)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: want error, got %.2f", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
			continue
		}
		if !approxEq(got, c.want) {
			t.Errorf("%s: got %.4f want %.4f", c.name, got, c.want)
		}
	}
}

// The limit must always be a price a buyer at the current LTP would hit:
// at or below LTP (when LTP is known) and never below the band floor.
func TestExitLimitFor_IsMarketable(t *testing.T) {
	for _, ltp := range []float64{1.05, 12.3, 99.95, 212.40, 1500, 15220} {
		lo, hi := ltp*0.95, ltp*1.05
		got, err := exitLimitFor(ltp, 0.05, lo, hi, 2)
		if err != nil {
			t.Fatalf("ltp=%v: %v", ltp, err)
		}
		if got > ltp+1e-9 {
			t.Errorf("ltp=%v: limit %.2f above LTP", ltp, got)
		}
		if got < lo-1e-9 {
			t.Errorf("ltp=%v: limit %.2f below band floor %.2f", ltp, got, lo)
		}
	}
}

func TestEnvPct(t *testing.T) {
	const name = "MANTHAN_TEST_PCT_X"
	t.Setenv(name, "")
	if got := envPct(name, 2, 0.1, 10); got != 2 {
		t.Errorf("empty: got %v want default 2", got)
	}
	t.Setenv(name, "3.5")
	if got := envPct(name, 2, 0.1, 10); got != 3.5 {
		t.Errorf("3.5: got %v", got)
	}
	t.Setenv(name, "0") // unfillable → default
	if got := envPct(name, 2, 0.1, 10); got != 2 {
		t.Errorf("0: got %v want default", got)
	}
	t.Setenv(name, "50") // give-away → default
	if got := envPct(name, 2, 0.1, 10); got != 2 {
		t.Errorf("50: got %v want default", got)
	}
	t.Setenv(name, "abc")
	if got := envPct(name, 2, 0.1, 10); got != 2 {
		t.Errorf("abc: got %v want default", got)
	}
}

// SLLimitGap: the limit leg is now MANTHAN_SL_LIMIT_GAP_PCT (default 2%) of
// the trigger, never tighter than 5 ticks. The trigger itself is untouched.
func TestSLLimitGap_DefaultIsTwoPercentFlooredAtFiveTicks(t *testing.T) {
	if slLimitGapPct != defaultSLLimitGapPct {
		t.Skipf("MANTHAN_SL_LIMIT_GAP_PCT overridden in env (%v) — default assertions skipped", slLimitGapPct)
	}
	if got := SLLimitGap(212.40, 0.05); !approxEq(got, 4.248) {
		t.Errorf("212.40 → gap %.4f want 4.2480 (2%%)", got)
	}
	if got := SLLimitGap(1.00, 0.05); !approxEq(got, 0.25) {
		t.Errorf("1.00 → gap %.4f want 0.25 (5 ticks floor)", got)
	}
	if got := SLLimitGap(15220, 0.5); !approxEq(got, 304.4) {
		t.Errorf("15220 → gap %.4f want 304.40", got)
	}
}
