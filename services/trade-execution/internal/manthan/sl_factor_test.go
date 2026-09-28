package manthan

import "testing"

// slFactor is the ONLY translation from stop pct to trigger multiplier.
// Legacy rows/signals carry 0 → must behave exactly like the old
// hardcoded 20% so pre-migration positions are re-protected identically.
func TestSLFactor(t *testing.T) {
	cases := []struct {
		pct  float64
		want float64
	}{
		{20, 0.80},
		{10, 0.90}, // LARGE-cap rule
		{0, 0.80},  // legacy row / missing pct
		{-5, 0.80},
		{100, 0.80},
		{150, 0.80},
	}
	for _, c := range cases {
		if got := slFactor(c.pct); got != c.want {
			t.Errorf("slFactor(%v) = %v, want %v", c.pct, got, c.want)
		}
	}
}
