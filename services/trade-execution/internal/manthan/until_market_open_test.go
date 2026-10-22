package manthan

import (
	"testing"
	"time"
)

// 2026-10-22 review: an exit held outside the session used to wake every 30 s
// all night. After the close (or on a holiday/weekend) the hold must sleep
// until the NEXT session's 09:15:10, and inside the session it stays 30 s.
func TestUntilMarketOpen(t *testing.T) {
	ist := istLocation()
	at := func(d, h, m int) time.Time { return time.Date(2026, 10, d, h, m, 0, 0, ist) }

	// Thursday 22 Oct 08:00 → today's open.
	if got := untilMarketOpen(at(22, 8, 0)); got != 75*time.Minute+10*time.Second {
		t.Errorf("pre-open: got %v want 1h15m10s", got)
	}
	// Inside the session → 30 s floor.
	if got := untilMarketOpen(at(22, 11, 0)); got != 30*time.Second {
		t.Errorf("in-session: got %v want 30s", got)
	}
	// After the close → next trading day (Fri 23 Oct) 09:15:10.
	want := time.Date(2026, 10, 23, 9, 15, 10, 0, ist).Sub(at(22, 16, 0))
	if got := untilMarketOpen(at(22, 16, 0)); got != want {
		t.Errorf("after-close: got %v want %v", got, want)
	}
	// Saturday → Monday 09:15:10.
	want = time.Date(2026, 10, 26, 9, 15, 10, 0, ist).Sub(at(24, 11, 0))
	if got := untilMarketOpen(at(24, 11, 0)); got != want {
		t.Errorf("weekend: got %v want %v", got, want)
	}
}
