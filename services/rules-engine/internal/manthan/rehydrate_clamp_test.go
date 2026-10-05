package manthan

import (
	"math"
	"testing"
)

// The restart re-clamp (rehydrate.go) must judge a persisted stop against the
// POSITION's bucket-aware stop distance, not the strategy's configured pct.
// Before 2026-09-30 a LARGE-cap position trailing at its legitimate 10% was
// "corrected" to the 20% level (and persisted) on every restart.
func TestRehydrate_ClampUsesBucketStopLoss(t *testing.T) {
	const high = 100.0
	cases := []struct {
		name      string
		bucket    string
		persisted float64
		conf      float64
		wantClamp bool
		wantRule  float64
	}{
		// The bug: a LARGE stop at 10% below the high is exactly the rule level.
		// Under the old strategy-pct rule (20%) it was "too tight" and widened to 80.
		{"LARGE at its 10% level is NOT clamped", "LARGE", 90, 20, false, 90},
		{"LARGE slightly below its level is NOT clamped", "LARGE", 89.5, 20, false, 90},
		// A genuinely corrupt LARGE stop (2% below high) still self-heals — to 90, not 80.
		{"LARGE corrupt 2% stop clamps to the 10% level", "LARGE", 98, 20, true, 90},
		// Non-LARGE keeps the strategy's distance.
		{"SMALL corrupt 2% stop clamps to the 20% level", "SMALL", 98, 20, true, 80},
		{"SMALL at 12% below high is tighter than 20% → clamp", "SMALL", 88, 20, true, 80},
		{"SMALL at its 20% level is NOT clamped", "SMALL", 80, 20, false, 80},
		{"MID with broken config falls back to the 20% default", "MID", 95, 0, true, 80},
		// Lower-case / padded bucket strings are still LARGE.
		{"lower-case large honours 10%", " large ", 90, 20, false, 90},
	}
	for _, c := range cases {
		rule, clamp := rehydrateClampSL(high, c.persisted, c.bucket, c.conf)
		if clamp != c.wantClamp {
			t.Errorf("%s: clamp=%v want %v (rule=%.2f)", c.name, clamp, c.wantClamp, rule)
		}
		if math.Abs(rule-c.wantRule) > 1e-9 {
			t.Errorf("%s: rule level=%.4f want %.4f", c.name, rule, c.wantRule)
		}
	}

	// A zero/unknown high can never justify moving a stop.
	if _, clamp := rehydrateClampSL(0, 98, "SMALL", 20); clamp {
		t.Error("high=0 must never clamp")
	}
}
