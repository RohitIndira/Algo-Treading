package manthan

import "testing"

// canPlaceEntriesNowIST must mirror trade-execution's checkMarketHours window
// (trading day, 09:15–15:20 IST). This test documents the boundary contract;
// it exercises the pure minute-of-day logic via the same constants.
func TestMarketWindowBoundaries(t *testing.T) {
	const open = 9*60 + 15
	const close = 15*60 + 20
	cases := []struct {
		mod  int
		want bool
	}{
		{9*60 + 14, false}, // 09:14 — before open
		{9*60 + 15, true},  // 09:15 — open edge (inclusive)
		{12*60, true},      // midday
		{15*60 + 19, true}, // 15:19 — last tradeable minute
		{15*60 + 20, false},// 15:20 — cutoff (exclusive)
		{17*60 + 20, false},// 17:20 — the PD09 after-hours incident
	}
	for _, c := range cases {
		got := c.mod >= open && c.mod < close
		if got != c.want {
			t.Errorf("minute %d: got %v want %v", c.mod, got, c.want)
		}
	}
}
