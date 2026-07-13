package timezone

import (
	"os"
	"strconv"
	"time"
)

// IST is the Indian Standard Time location (UTC+5:30).
// Uses the OS tzdata via LoadLocation; falls back to a fixed offset if tzdata
// is unavailable (e.g. scratch containers without zoneinfo).
var IST = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return time.FixedZone("IST", 5*3600+30*60)
	}
	return loc
}()

// allowSaturdayMock, when true, treats Saturday as a normal trading day so the
// system can participate in SEBI's Saturday mock/special sessions. Sunday is
// always closed. Controlled by the ALLOW_SATURDAY_MOCK env var (default false).
// Evaluated once at package init so the hot-path weekend checks stay a plain
// field read with no per-call env lookup.
var allowSaturdayMock = func() bool {
	v, _ := strconv.ParseBool(os.Getenv("ALLOW_SATURDAY_MOCK"))
	return v
}()

// AllowSaturdayMock reports whether Saturday mock trading is enabled.
func AllowSaturdayMock() bool { return allowSaturdayMock }

// IsMarketClosedDay reports whether t (interpreted in IST) falls on a weekend
// day on which trading is closed. Sunday is always closed; Saturday is closed
// unless ALLOW_SATURDAY_MOCK is enabled for the SEBI Saturday mock session.
// Note: this does not consult the NSE/BSE holiday calendar — callers that need
// holiday awareness must check that separately.
func IsMarketClosedDay(t time.Time) bool {
	switch t.In(IST).Weekday() {
	case time.Sunday:
		return true
	case time.Saturday:
		return !allowSaturdayMock
	default:
		return false
	}
}
