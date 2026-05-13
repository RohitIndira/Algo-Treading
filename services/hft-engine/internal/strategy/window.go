// Trade-window enforcement.
//
// Each strategy can specify a daily IST window like "09:15" → "15:30".
// Outside that window the state machine halts both sides with reason
// "window_closed". Empty strings on either bound disable the check.
package strategy

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// IST is the timezone all market hours are expressed in. Loaded once.
	istLoc     *time.Location
	istLoadErr error
	istOnce    sync.Once
)

// loadIST resolves Asia/Kolkata exactly once and caches it. Falls back
// to a fixed +05:30 zone if the system zoneinfo is missing (rare but
// happens in minimal Docker images).
func loadIST() *time.Location {
	istOnce.Do(func() {
		istLoc, istLoadErr = time.LoadLocation("Asia/Kolkata")
		if istLoadErr != nil {
			istLoc = time.FixedZone("IST", 5*60*60+30*60)
		}
	})
	return istLoc
}

// inTradeWindow returns true when the given moment (any timezone) falls
// inside the strategy's configured IST window. If either bound is empty,
// the check is disabled and we return true.
//
// Bounds are inclusive on both ends. Malformed strings disable the check.
func inTradeWindow(t time.Time, startHHMM, endHHMM string) bool {
	if startHHMM == "" || endHHMM == "" {
		return true
	}
	startMin, ok := parseHHMM(startHHMM)
	if !ok {
		return true
	}
	endMin, ok := parseHHMM(endHHMM)
	if !ok {
		return true
	}

	ist := t.In(loadIST())
	nowMin := ist.Hour()*60 + ist.Minute()
	return nowMin >= startMin && nowMin <= endMin
}

// parseHHMM accepts "HH:MM" (24-hour) and returns minutes-since-midnight.
// Returns false on any parse error so the caller can decide to no-op.
func parseHHMM(s string) (int, bool) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return 0, false
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		return 0, false
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}
