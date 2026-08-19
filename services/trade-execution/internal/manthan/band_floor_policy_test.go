package manthan

import "testing"

// HARD RULE (operator, 2026-08-19): the stop is always the exact 20% level.
// Outside the band → not placed (deferred); never moved to the band floor.
func TestResolveBandFloor_NeverClamps(t *testing.T) {
	cases := []struct {
		name          string
		intended, low float64
		wantDefer     bool
	}{
		{"inside band → intended, placed", 468.80, 465.20, false}, // GNA
		{"APARINDS: 3.8% below floor → deferred, NOT clamped", 13337.60, 13772, true},
		{"FILATEX: 1.5% below floor → deferred", 68.70, 69.40, true},
		{"AEGISLOG: 12.7% below → deferred", 1097.68, 1231.00, true},
		{"no DPR data → intended untouched", 100, 0, false},
	}
	for _, c := range cases {
		trig, clamped, why := resolveBandFloor(c.intended, c.low)
		if clamped {
			t.Errorf("%s: must never clamp to the band floor", c.name)
		}
		if (why != "") != c.wantDefer {
			t.Errorf("%s: defer=%v want %v (%s)", c.name, why != "", c.wantDefer, why)
		}
		if !c.wantDefer && trig != c.intended {
			t.Errorf("%s: trigger=%.2f want the exact intended %.2f", c.name, trig, c.intended)
		}
	}
	if BandFloorClampMaxPct != 0 {
		t.Fatal("BandFloorClampMaxPct must stay 0 — hard rule: exact 20% stop only")
	}
}
