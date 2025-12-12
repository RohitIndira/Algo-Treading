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

// NewMarketHours creates a new MarketHours instance with custom configuration
func NewMarketHours(openHour, openMinute, closeHour, closeMinute int, timezone string) *MarketHours {
	// Load specified timezone
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		// Fallback to IST (UTC+5:30) if timezone loading fails
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}

	return &MarketHours{
		OpenHour:    openHour,
		OpenMinute:  openMinute,
		CloseHour:   closeHour,
		CloseMinute: closeMinute,
		Timezone:    loc,
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

	// Check if it's a weekend (Saturday or Sunday)
	if weekday == time.Saturday || weekday == time.Sunday {
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

	if weekday == time.Saturday || weekday == time.Sunday {
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

	// Skip weekends
	for nextOpen.Weekday() == time.Saturday || nextOpen.Weekday() == time.Sunday {
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
