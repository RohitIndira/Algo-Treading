package manthan

import (
	"testing"
	"time"
)

// 2026-10-22 BODALCHEM: the feed row was 23 days old and the exit was priced
// from it. A row not updated during today's session must be judged stale
// once the market has been open a few minutes; before the open (or on a
// closed day) yesterday's last tick is legitimate.
func TestFeedStale(t *testing.T) {
	ist := istLocation()
	// Thursday 2026-10-22 (a trading day).
	at := func(h, m int) time.Time { return time.Date(2026, 10, 22, h, m, 0, 0, ist) }

	cases := []struct {
		name  string
		asOf  time.Time
		now   time.Time
		stale bool
	}{
		{"unstamped row is trusted", time.Time{}, at(11, 0), false},
		{"23-day-old row mid-session", time.Date(2026, 9, 29, 14, 30, 0, 0, ist), at(11, 6), true},
		{"yesterday's close at 09:14 (pre-open) is fine", at(9, 14).AddDate(0, 0, -1), at(9, 14), false},
		{"yesterday's close at 09:19 still fine (grace)", at(15, 30).AddDate(0, 0, -1), at(9, 19), false},
		{"yesterday's close at 09:20 is stale", at(15, 30).AddDate(0, 0, -1), at(9, 20), true},
		{"ticked a minute ago", at(11, 5), at(11, 6), false},
		{"ticked at 09:30, now 15:00 (5.5h) within 6h default", at(9, 30), at(15, 0), false},
		{"ticked at 09:00 pre-open stamp, now 15:30 (6.5h) stale", at(9, 0), at(15, 30), true},
		{"after close nothing is judged stale", time.Date(2026, 9, 29, 14, 30, 0, 0, ist), at(16, 0), false},
		{"weekend nothing is judged stale", time.Date(2026, 9, 29, 14, 30, 0, 0, ist), time.Date(2026, 10, 24, 11, 0, 0, 0, ist), false},
	}
	for _, c := range cases {
		if got := feedStale(c.asOf, c.now); got != c.stale {
			t.Errorf("%s: feedStale=%v want %v", c.name, got, c.stale)
		}
	}
}

func TestParseMarketSnap(t *testing.T) {
	raw := []byte(`{"ltp":201.93,"dpr_lower":191.92,"dpr_upper":212.12,"tick_size":0.05,"last_updated":"2026-09-29T14:30:09+05:30","timestamp":1790672409695}`)
	snap, err := parseMarketSnap(raw)
	if err != nil {
		t.Fatal(err)
	}
	if snap.LTP != 201.93 || snap.DPRLower != 191.92 || snap.DPRUpper != 212.12 || snap.TickSize != 0.05 {
		t.Fatalf("fields: %+v", snap)
	}
	want := time.UnixMilli(1790672409695)
	if !snap.AsOf.Equal(want) {
		t.Fatalf("asOf %v want %v", snap.AsOf, want)
	}
	// last_updated only (no epoch) — RFC3339 with nanos and offset.
	snap2, err := parseMarketSnap([]byte(`{"ltp":428.1,"last_updated":"2026-10-22T11:16:37.208034725+05:30"}`))
	if err != nil {
		t.Fatal(err)
	}
	if snap2.AsOf.IsZero() || snap2.AsOf.Year() != 2026 || snap2.AsOf.In(istLocation()).Hour() != 11 {
		t.Fatalf("last_updated not parsed: %v", snap2.AsOf)
	}
	// Unstamped legacy row → zero AsOf (trusted).
	snap3, _ := parseMarketSnap([]byte(`{"ltp":10}`))
	if !snap3.AsOf.IsZero() {
		t.Fatalf("unstamped row must have zero AsOf: %v", snap3.AsOf)
	}
}
