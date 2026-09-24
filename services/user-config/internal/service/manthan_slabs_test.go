package service

import "testing"

func TestManthanSlabPositions(t *testing.T) {
	cases := []struct {
		capital float64
		want    int32
	}{
		{50_000, 10},      // new minimum
		{60_000, 10},
		{249_999, 10},
		{250_000, 10},     // boundary → lower slab
		{250_001, 25},
		{500_000, 25},     // the old minimum, unchanged slab
		{2_500_000, 25},   // boundary → lower slab (pre-existing behaviour)
		{2_500_001, 50},
		{10_000_000, 50},
	}
	for _, c := range cases {
		if got := manthanSlabPositions(c.capital); got != c.want {
			t.Errorf("slab(%.0f) = %d, want %d", c.capital, got, c.want)
		}
	}
	if manthanMinCapital != 50_000 {
		t.Errorf("min capital = %d, want 50000", manthanMinCapital)
	}
}
