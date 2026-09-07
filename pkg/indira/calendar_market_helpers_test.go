package indira

import (
	"testing"
	"time"
)

func mustIST(t *testing.T) *time.Location {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatalf("load IST: %v", err)
	}
	return loc
}
func timeInIST(s timeShim, loc *time.Location) time.Time {
	return time.Date(s.y, time.Month(s.mo), s.d, s.h, s.mi, 0, 0, loc)
}
func timeInUTC(y, mo, d, h, mi int) time.Time {
	return time.Date(y, time.Month(mo), d, h, mi, 0, 0, time.UTC)
}
