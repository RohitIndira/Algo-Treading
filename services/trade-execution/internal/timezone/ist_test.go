package timezone

import (
	"testing"
	"time"
)

func TestIsMarketClosedDay(t *testing.T) {
	sat := time.Date(2026, 7, 4, 10, 0, 0, 0, IST)  // Saturday
	sun := time.Date(2026, 7, 5, 10, 0, 0, 0, IST)  // Sunday
	mon := time.Date(2026, 7, 6, 10, 0, 0, 0, IST)  // Monday

	// Sanity: our reference dates are the weekdays we think they are.
	if sat.Weekday() != time.Saturday || sun.Weekday() != time.Sunday || mon.Weekday() != time.Monday {
		t.Fatalf("reference dates wrong: sat=%v sun=%v mon=%v", sat.Weekday(), sun.Weekday(), mon.Weekday())
	}

	orig := allowSaturdayMock
	defer func() { allowSaturdayMock = orig }()

	// Default (mock disabled): both weekend days are closed, weekday open.
	allowSaturdayMock = false
	if !IsMarketClosedDay(sat) {
		t.Errorf("mock off: Saturday should be closed")
	}
	if !IsMarketClosedDay(sun) {
		t.Errorf("mock off: Sunday should be closed")
	}
	if IsMarketClosedDay(mon) {
		t.Errorf("mock off: Monday should be open")
	}

	// Mock enabled: Saturday open, Sunday still closed, weekday open.
	allowSaturdayMock = true
	if IsMarketClosedDay(sat) {
		t.Errorf("mock on: Saturday should be open (SEBI mock)")
	}
	if !IsMarketClosedDay(sun) {
		t.Errorf("mock on: Sunday must stay closed")
	}
	if IsMarketClosedDay(mon) {
		t.Errorf("mock on: Monday should be open")
	}
}
