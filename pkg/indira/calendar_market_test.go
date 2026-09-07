package indira

import "testing"

func istTime(y int, mo, d, h, mi int) timeShim { return timeShim{y, mo, d, h, mi} }

// timeShim builds an Asia/Kolkata time for table tests without importing
// time in every case line.
type timeShim struct{ y, mo, d, h, mi int }

func TestIsMarketOpen(t *testing.T) {
	// 2026-09-07 is a Monday; 09-05 Sat, 09-06 Sun; 08-15 Independence Day (holiday).
	cases := []struct {
		name string
		ts   timeShim
		want bool
	}{
		{"weekday 10:00 open", timeShim{2026, 9, 7, 10, 0}, true},
		{"weekday 09:15 boundary open", timeShim{2026, 9, 7, 9, 15}, true},
		{"weekday 15:30 boundary open", timeShim{2026, 9, 7, 15, 30}, true},
		{"weekday 09:14 pre-open closed", timeShim{2026, 9, 7, 9, 14}, false},
		{"weekday 15:31 post-close closed", timeShim{2026, 9, 7, 15, 31}, false},
		{"weekday 12:24 midday open", timeShim{2026, 9, 7, 12, 24}, true},
		{"Saturday midday closed (KEI/IIFL poison window)", timeShim{2026, 9, 5, 12, 24}, false},
		{"Sunday midday closed", timeShim{2026, 9, 6, 12, 0}, false},
		{"holiday 08-15 midday closed", timeShim{2026, 8, 15, 11, 0}, false},
	}
	loc := mustIST(t)
	for _, c := range cases {
		tt := timeInIST(c.ts, loc)
		if got := IsMarketOpen(tt); got != c.want {
			t.Errorf("%s: IsMarketOpen=%v want %v", c.name, got, c.want)
		}
	}
}

// IsMarketOpen must convert from any zone — a UTC instant during IST hours.
func TestIsMarketOpen_ConvertsFromUTC(t *testing.T) {
	// 2026-09-07 06:00 UTC == 11:30 IST Monday → open.
	utc := timeInUTC(2026, 9, 7, 6, 0)
	if !IsMarketOpen(utc) {
		t.Errorf("UTC 06:00 (11:30 IST Mon) should be open")
	}
	// 2026-09-05 06:00 UTC == 11:30 IST Saturday → closed.
	if IsMarketOpen(timeInUTC(2026, 9, 5, 6, 0)) {
		t.Errorf("UTC 06:00 Sat should be closed")
	}
}
