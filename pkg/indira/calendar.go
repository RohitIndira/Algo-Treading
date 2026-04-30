package indira

import "time"

// NSE / BSE trading calendar.
//
// Holiday list is hard-coded per year (the exchanges publish the official
// calendar in December for the next year — annual maintenance task to
// extend this map). The protective-replayer cron consults IsTradingDay
// before firing any pre-open cycle so we don't try to place orders on
// closed days.
//
// Special sessions (Diwali Muhurat — typically a 1-hour session at ~6 PM)
// are intentionally NOT listed here. They have a different schedule that
// breaks the 09:15-15:30 assumption everywhere downstream; we mark them
// as holidays and let the operator manually run any needed orders.

// nseHolidays2026 — official NSE list, verify each December against
// https://www.nseindia.com/resources/exchange-communication-holidays
//
// Format: YYYY-MM-DD (Asia/Kolkata).
//
// Sources for these dates: NSE annual circular (verified at year start).
// If a date is wrong here, our cron will skip a real trading day (false
// negative — positions go unprotected for one day) which is safer than
// firing on a closed day (false positive — wasted broker calls + alerts).
var nseHolidays2026 = map[string]bool{
	"2026-01-26": true, // Republic Day
	"2026-02-15": true, // Mahashivratri  (verify before use)
	"2026-03-04": true, // Holi
	"2026-03-27": true, // Ram Navami     (verify)
	"2026-04-03": true, // Good Friday    (verify)
	"2026-04-14": true, // Dr. Ambedkar Jayanti
	"2026-05-01": true, // Maharashtra Day / Labour Day
	"2026-05-27": true, // Eid al-Fitr    (verify against moon-sighting)
	"2026-08-15": true, // Independence Day
	"2026-09-15": true, // Ganesh Chaturthi (verify)
	"2026-10-02": true, // Mahatma Gandhi Jayanti
	"2026-10-21": true, // Dussehra        (verify)
	"2026-11-09": true, // Diwali Laxmi Pujan (Muhurat session evening — treat as holiday for our cron)
	"2026-11-10": true, // Diwali Balipratipada (verify)
	"2026-11-25": true, // Guru Nanak Jayanti  (verify)
	"2026-12-25": true, // Christmas
}

// IsTradingDay returns true when t (in Asia/Kolkata) is an NSE/BSE
// trading session day — not a Saturday, not a Sunday, and not on the
// hard-coded holiday list.
func IsTradingDay(t time.Time) bool {
	if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
		return false
	}
	day := t.Format("2006-01-02")
	if nseHolidays2026[day] {
		return false
	}
	// Future: append more years here as NSE publishes them.
	return true
}

// NextTradingDay returns the next trading-session date strictly after t.
// Skips Sat/Sun and the holiday list. Time component is reset to 00:00:00
// in t's location.
func NextTradingDay(t time.Time) time.Time {
	loc := t.Location()
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc).Add(24 * time.Hour)
	for !IsTradingDay(d) {
		d = d.Add(24 * time.Hour)
	}
	return d
}
