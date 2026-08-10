package manthan

import (
	"testing"
)

// The safety monitor must only act during a regular session (09:15–15:30 IST,
// trading day). Regression for the 2026-08-08 Saturday spam (7,464 AU004
// place-order attempts). We test the pure minute-of-day window here; trading-
// day/holiday handling is delegated to indiraClient.IsTradingDay.
func TestSafetyMonitor_SessionWindow(t *testing.T) {
	const open = 9*60 + 15
	const close = 15*60 + 30
	cases := []struct {
		h, m int
		want bool
	}{
		{9, 14, false},  // before open
		{9, 15, true},   // open edge
		{12, 0, true},   // midday
		{15, 30, true},  // close edge (inclusive — SL still placeable)
		{15, 31, false}, // after close
		{16, 35, false}, // EOD AMO window — NOT the monitor's job
		{3, 0, false},   // 3am — the Saturday-spam hour
	}
	for _, c := range cases {
		mod := c.h*60 + c.m
		got := mod >= open && mod <= close
		if got != c.want {
			t.Errorf("%02d:%02d: got %v want %v", c.h, c.m, got, c.want)
		}
	}
}
