package manthan

import "testing"

// Policy chosen 2026-08-19: place at the band floor when it is within 5% of
// the intended stop; defer when the band is narrower than that.
func TestResolveBandFloor(t *testing.T) {
	cases := []struct {
		name          string
		intended, low float64
		wantTrig      float64
		wantClamped   bool
		wantDefer     bool
	}{
		{"inside band → intended", 468.80, 465.20, 468.80, false, false}, // GNA today
		{"APARINDS: floor 3.8% above → clamp to floor", 13337.60, 13772, 13772 * 1.005, true, false},
		{"FILATEX: floor 1.5% above → clamp", 68.70, 69.40, 69.40 * 1.005, true, false},
		{"AEGISLOG: floor 12.7% above → defer", 1097.68, 1231.00, 0, false, true},
		{"MODISONLTD BE 5%-band: 13.7% above → defer", 314.04, 355.25, 0, false, true},
		{"exactly at 5% → clamp", 100, 105 / 1.005, 105, true, false},
		{"just over 5% → defer", 100, 105.2 / 1.005, 0, false, true},
		{"no DPR data → intended untouched", 100, 0, 100, false, false},
	}
	for _, c := range cases {
		trig, clamped, why := resolveBandFloor(c.intended, c.low)
		if clamped != c.wantClamped || (why != "") != c.wantDefer {
			t.Errorf("%s: clamped=%v defer=%v (%s)", c.name, clamped, why != "", why)
			continue
		}
		if !c.wantDefer && (trig < c.wantTrig-0.01 || trig > c.wantTrig+0.01) {
			t.Errorf("%s: trigger=%.2f want %.2f", c.name, trig, c.wantTrig)
		}
	}
}
