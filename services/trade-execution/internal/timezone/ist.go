package timezone

import "time"

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
