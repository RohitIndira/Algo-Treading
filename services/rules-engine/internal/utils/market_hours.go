package utils

import (
	"time"
)

// MarketHours defines the trading session times
type MarketHours struct {
	OpenHour    int
	OpenMinute  int
	CloseHour   int
	CloseMinute int
	Timezone    *time.Location
	// AllowSaturday, when true, treats Saturday as a normal trading day so the
	// engine generates signals during SEBI's Saturday mock/special sessions.
	// Sunday is always closed. Wired from the ALLOW_SATURDAY_MOCK env var.
	AllowSaturday bool
}

// isClosedWeekend reports whether weekday is a weekend day on which trading is
// closed. Sunday is always closed; Saturday is closed unless AllowSaturday is
// set (SEBI Saturday mock session).
func (mh *MarketHours) isClosedWeekend(weekday time.Weekday) bool {
	switch weekday {
	case time.Sunday:
		return true
	case time.Saturday:
		return !mh.AllowSaturday
	default:
		return false
	}
}

// DefaultMarketHours returns the standard Indian market hours (9:15 AM to 3:30 PM IST)
func DefaultMarketHours() *MarketHours {
	// Load IST timezone
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		// Fallback to UTC+5:30 if timezone loading fails
		ist = time.FixedZone("IST", 5*60*60+30*60)
	}

	return &MarketHours{
		OpenHour:    9,
		OpenMinute:  15,
		CloseHour:   15,
		CloseMinute: 30,
		Timezone:    ist,
	}
}

// NewMarketHours creates a new MarketHours instance with custom configuration.
// allowSaturday enables Saturday as a trading day for SEBI mock sessions.
func NewMarketHours(openHour, openMinute, closeHour, closeMinute int, timezone string, allowSaturday bool) *MarketHours {
	// Load specified timezone
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		// Fallback to IST (UTC+5:30) if timezone loading fails
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}

	return &MarketHours{
		OpenHour:      openHour,
		OpenMinute:    openMinute,
		CloseHour:     closeHour,
		CloseMinute:   closeMinute,
		Timezone:      loc,
		AllowSaturday: allowSaturday,
	}
}

// IsMarketOpen checks if the current time is within market hours
func (mh *MarketHours) IsMarketOpen() bool {
	return mh.IsMarketOpenAt(time.Now())
}

// IsMarketOpenAt checks if the given time is within market hours
func (mh *MarketHours) IsMarketOpenAt(t time.Time) bool {
	// Convert to market timezone
	marketTime := t.In(mh.Timezone)

	// Get weekday
	weekday := marketTime.Weekday()

	// Check if it's a closed weekend (Sunday always; Saturday unless mock enabled)
	if mh.isClosedWeekend(weekday) {
		return false
	}

	// Extract hour and minute
	hour := marketTime.Hour()
	minute := marketTime.Minute()

	// Convert times to minutes since midnight for easier comparison
	currentMinutes := hour*60 + minute
	openMinutes := mh.OpenHour*60 + mh.OpenMinute
	closeMinutes := mh.CloseHour*60 + mh.CloseMinute

	// Check if current time is within market hours
	return currentMinutes >= openMinutes && currentMinutes <= closeMinutes
}

// GetMarketStatus returns a human-readable market status
func (mh *MarketHours) GetMarketStatus() string {
	now := time.Now().In(mh.Timezone)
	weekday := now.Weekday()

	if mh.isClosedWeekend(weekday) {
		return "Market closed (Weekend)"
	}

	if mh.IsMarketOpen() {
		return "Market open"
	}

	hour := now.Hour()
	minute := now.Minute()
	currentMinutes := hour*60 + minute
	openMinutes := mh.OpenHour*60 + mh.OpenMinute

	if currentMinutes < openMinutes {
		return "Market closed (Before opening)"
	}

	return "Market closed (After closing)"
}

// TimeUntilOpen returns the duration until market opens
// Returns 0 if market is currently open
func (mh *MarketHours) TimeUntilOpen() time.Duration {
	if mh.IsMarketOpen() {
		return 0
	}

	now := time.Now().In(mh.Timezone)
	year, month, day := now.Date()

	// Calculate next opening time
	nextOpen := time.Date(year, month, day, mh.OpenHour, mh.OpenMinute, 0, 0, mh.Timezone)

	// If we're past today's opening, move to next day
	if now.After(nextOpen) {
		nextOpen = nextOpen.Add(24 * time.Hour)
	}

	// Skip closed weekend days (Saturday counts as open when mock is enabled)
	for mh.isClosedWeekend(nextOpen.Weekday()) {
		nextOpen = nextOpen.Add(24 * time.Hour)
	}

	return nextOpen.Sub(now)
}

// TimeUntilClose returns the duration until market closes
// Returns 0 if market is currently closed
func (mh *MarketHours) TimeUntilClose() time.Duration {
	if !mh.IsMarketOpen() {
		return 0
	}

	now := time.Now().In(mh.Timezone)
	year, month, day := now.Date()

	closeTime := time.Date(year, month, day, mh.CloseHour, mh.CloseMinute, 0, 0, mh.Timezone)

	return closeTime.Sub(now)
}
