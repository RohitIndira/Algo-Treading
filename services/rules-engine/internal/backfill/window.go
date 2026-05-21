// Package backfill implements the after-market-news backfill: when a user
// creates a strategy with process_after_market_news=true, the rules-engine
// replays news from the previous trading-session close against that strategy
// and places orders for any matches.
//
// Window semantics (all times IST):
//
//	Start = the most recent 15:31 close that has already happened.
//	End   = "now"            when created during market hours (09:15–15:30)
//	      = today 09:15       when created pre-open on a trading day
//	      = next 09:15 IST    when created post-close or on a non-trading day
//
// Dispatch:
//
//	immediate            when End == now (market is open)
//	deferred to End      otherwise — orders are held until the next 09:15 IST
//	                     and then placed at the live LTP at that moment.
package backfill

import (
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/holiday"
)

// Indian equity session boundaries. CloseHour/CloseMin is 15:31 (one minute
// past the 15:30 close) so the window picks up news produced strictly after
// the regular session ends.
const (
	openHour  = 9
	openMin   = 15
	closeHour = 15
	closeMin  = 31
)

// Window is the result of ComputeWindow: the [Start, End] news-time range to
// scan and the wall-clock instant DispatchAfter at/after which matched orders
// may be placed.
//
// When DispatchAfter is at or before "now", dispatch is immediate.
type Window struct {
	Start         time.Time
	End           time.Time
	DispatchAfter time.Time
}

// Immediate reports whether matched orders should be dispatched right away
// (versus held until DispatchAfter).
func (w Window) Immediate(now time.Time) bool {
	return !w.DispatchAfter.After(now)
}

// isTradingDay reports whether d (interpreted in its own location) is a
// weekday and not a loaded holiday. A nil checker means weekend-only logic.
func isTradingDay(d time.Time, h *holiday.Checker) bool {
	switch d.Weekday() {
	case time.Saturday, time.Sunday:
		return false
	}
	if h != nil && h.IsDateHoliday(d.Format("2006-01-02")) {
		return false
	}
	return true
}

// atTime returns the instant on day `d` at hour:min in loc.
func atTime(d time.Time, hour, min int, loc *time.Location) time.Time {
	return time.Date(d.Year(), d.Month(), d.Day(), hour, min, 0, 0, loc)
}

// previousTradingDay returns the most recent trading day strictly before `d`.
// The 14-iteration cap is a safety bound — exchange holidays never stack that
// deep — after which it returns whatever day it reached.
func previousTradingDay(d time.Time, loc *time.Location, h *holiday.Checker) time.Time {
	c := d.AddDate(0, 0, -1)
	for i := 0; i < 14; i++ {
		if isTradingDay(c, h) {
			return c
		}
		c = c.AddDate(0, 0, -1)
	}
	return c
}

// nextTradingDay returns the first trading day strictly after `d`.
func nextTradingDay(d time.Time, loc *time.Location, h *holiday.Checker) time.Time {
	c := d.AddDate(0, 0, 1)
	for i := 0; i < 14; i++ {
		if isTradingDay(c, h) {
			return c
		}
		c = c.AddDate(0, 0, 1)
	}
	return c
}

// ComputeWindow derives the backfill Window for a strategy created at `now`.
//
//	Case A — created during market hours on a trading day (09:15 ≤ now < 15:31):
//	         Start = previous trading day 15:31, End = now, dispatch immediately.
//
//	Case B — created pre-open on a trading day (now < 09:15):
//	         Start = previous trading day 15:31, End/DispatchAfter = today 09:15.
//
//	Case C — created post-close on a trading day (now ≥ 15:31):
//	         Start = today 15:31, End/DispatchAfter = next trading day 09:15.
//
//	Case D — created on a non-trading day (weekend / holiday):
//	         Start = previous trading day 15:31,
//	         End/DispatchAfter = next trading day 09:15.
func ComputeWindow(now time.Time, loc *time.Location, h *holiday.Checker) Window {
	now = now.In(loc)
	todayOpen := atTime(now, openHour, openMin, loc)
	todayClose := atTime(now, closeHour, closeMin, loc)
	tradingToday := isTradingDay(now, h)

	switch {
	case tradingToday && !now.Before(todayOpen) && now.Before(todayClose):
		// Case A
		prev := previousTradingDay(now, loc, h)
		return Window{
			Start:         atTime(prev, closeHour, closeMin, loc),
			End:           now,
			DispatchAfter: now,
		}

	case tradingToday && now.Before(todayOpen):
		// Case B
		prev := previousTradingDay(now, loc, h)
		return Window{
			Start:         atTime(prev, closeHour, closeMin, loc),
			End:           todayOpen,
			DispatchAfter: todayOpen,
		}

	case tradingToday && !now.Before(todayClose):
		// Case C — the most recent close is *today*.
		next := nextTradingDay(now, loc, h)
		nextOpen := atTime(next, openHour, openMin, loc)
		return Window{
			Start:         todayClose,
			End:           nextOpen,
			DispatchAfter: nextOpen,
		}

	default:
		// Case D
		prev := previousTradingDay(now, loc, h)
		next := nextTradingDay(now, loc, h)
		nextOpen := atTime(next, openHour, openMin, loc)
		return Window{
			Start:         atTime(prev, closeHour, closeMin, loc),
			End:           nextOpen,
			DispatchAfter: nextOpen,
		}
	}
}
