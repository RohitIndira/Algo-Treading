package backfill

import "time"

var ist = time.FixedZone("IST", 5*60*60+30*60)

// PreviousTradingDay returns the most recent NSE_EQ trading day strictly before
// today (in IST). It skips weekends and dates present in the nseHolidays set
// ("YYYY-MM-DD" keys loaded from OdinMasterData.HolidayMaster sub_key=NSE_EQ).
func PreviousTradingDay(today time.Time, nseHolidays map[string]struct{}) time.Time {
	d := today.In(ist).AddDate(0, 0, -1)
	for {
		wd := d.Weekday()
		if wd != time.Saturday && wd != time.Sunday {
			if _, holiday := nseHolidays[d.Format("2006-01-02")]; !holiday {
				return d
			}
		}
		d = d.AddDate(0, 0, -1)
	}
}

// AMNTimeRange computes the [from, to) MongoDB query window for the AMN backfill.
//
// dt_tm in CAG_CHATBOT.NewsImpactDashboard stores IST wall-clock time but is
// tagged with a Z suffix (i.e. the UTC offset is wrong). To query correctly we
// must NOT apply the IST→UTC conversion: we take the IST wall-clock components
// and reinterpret them as time.UTC so the primitive.DateTime bytes match what
// MongoDB stores.
//
// Example: prev trading day = 2026-06-12, now = 2026-06-15 09:00 IST
//   from = 2026-06-12T15:31:00Z  (wall-clock components, UTC-tagged)
//   to   = 2026-06-15T09:00:00Z  (wall-clock components, UTC-tagged)
func AMNTimeRange(now time.Time, prevTradingDay time.Time) (from, to time.Time) {
	pd := prevTradingDay.In(ist)
	// Previous trading day at 15:31 IST wall-clock, stored in Mongo as Z.
	istFrom := time.Date(pd.Year(), pd.Month(), pd.Day(), 15, 31, 0, 0, ist)
	from = stripISTOffset(istFrom)

	nowIST := now.In(ist)
	to = stripISTOffset(nowIST)
	return
}

// stripISTOffset takes a time in IST and returns a UTC time whose wall-clock
// components (year/month/day/hour/minute/second) are identical to the IST value.
// This matches how MongoDB stores the dt_tm field: IST wall-clock, Z-tagged.
func stripISTOffset(t time.Time) time.Time {
	t = t.In(ist)
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
}
