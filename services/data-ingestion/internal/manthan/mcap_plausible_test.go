package manthan

import "testing"

// The MCap plausibility gate (2026-09-09): a fabricated market cap that
// re-labels the bucket (twice observed: 28,000 Cr vs ~1,400 Cr implied)
// must be dropped; genuine rows with normal data noise must pass.
func TestMcapPlausible(t *testing.T) {
	cases := []struct {
		name             string
		claimed, pe, pat float64
		want             bool
	}{
		{"DIFFNKG genuine (1823 vs 26.87x50.41=1354)", 1823, 26.87, 50.41, true},
		{"the observed fabrication (28000 vs ~1354)", 28000, 26.87, 50.41, false},
		{"BODALCHEM-class fabrication (28000 vs 20x80=1600)", 28000, 20, 80, false},
		{"3.9x above implied — inside tolerance", 5270, 26.87, 50.41, true},
		{"4.1x above implied — dropped", 5560, 26.87, 50.41, false},
		{"far below implied — dropped", 300, 26.87, 50.41, false},
		{"implied zero (no PE) — can't judge, pass", 28000, 0, 50, true},
	}
	for _, c := range cases {
		got := mcapPlausible(c.claimed, c.pe*c.pat)
		if got != c.want {
			t.Errorf("%s: mcapPlausible(%.0f, %.0f)=%v want %v", c.name, c.claimed, c.pe*c.pat, got, c.want)
		}
	}
}
