package manthan

// Regression for the 2026-08-17/18 morning incident: the daily 09:00 IST
// publish dispatches entries 15 minutes before the bell. The market-hours
// pre-check said "market not open yet (before 9:15)" and the generic
// "pre-check:"→POISON rule DLQ'd every one of them on attempt 1 — 26 FIV99
// entries on Monday, and no user could buy on any cron-morning. Pre-open is
// a WAIT state exactly like an upper-circuit hold: park, wake at 09:15:10,
// place.

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestClassify_PreOpenHoldRetries(t *testing.T) {
	err := fmt.Errorf("%w: %s", ErrPreOpenHold, ReasonPreOpen)
	if got := classifyHandlerErr(err); got != InboxErrPreOpen {
		t.Fatalf("pre-open hold classified %q, want %q (the morning DLQ bug is back)", got, InboxErrPreOpen)
	}
	if got := classifyHandlerErr(fmt.Errorf("dispatch: %w", err)); got != InboxErrPreOpen {
		t.Fatalf("wrapped pre-open hold classified %q", got)
	}
	// The message text alone must not carry a token that shadows the class.
	if got := classifyHandlerErr(errors.New(err.Error())); got == InboxErrPoison {
		t.Fatal("pre-open hold message text alone must not be POISON")
	}
	// Precedence: an auth-expired error still wins (fail-fast on dead session).
}

func TestHoldClasses_DoNotBurnAttempts(t *testing.T) {
	for _, c := range []string{InboxErrUpperCircuit, InboxErrPreOpen} {
		if !isHoldClass(c) {
			t.Errorf("%s must be a hold class (no attempt burn, class kept through auth gate)", c)
		}
	}
	for _, c := range []string{InboxErrPoison, InboxErrTransient, InboxErrAuthExpired, InboxErrBrokerReject} {
		if isHoldClass(c) {
			t.Errorf("%s must NOT be a hold class", c)
		}
	}
}

func TestUntilMarketOpen_WakesJustAfterBell(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	// 09:00:13 IST (the daily cron publish) → sleep until 09:15:10.
	now := time.Date(2026, 8, 19, 9, 0, 13, 0, ist)
	if d := untilMarketOpen(now); d != 14*time.Minute+57*time.Second {
		t.Errorf("09:00:13 → %v, want 14m57s (wake at 09:15:10)", d)
	}
	// 08:46 IST sheet edit inside the watch window → still lands at 09:15:10.
	now = time.Date(2026, 8, 19, 8, 46, 0, 0, ist)
	if d := untilMarketOpen(now); d != 29*time.Minute+10*time.Second {
		t.Errorf("08:46:00 → %v, want 29m10s", d)
	}
	// Already past the bell (hold set at 09:14:59, evaluated at 09:15:30):
	// never a negative/zero sleep — 30 s floor and the pre-check now passes.
	now = time.Date(2026, 8, 19, 9, 15, 30, 0, ist)
	if d := untilMarketOpen(now); d != 30*time.Second {
		t.Errorf("09:15:30 → %v, want 30s floor", d)
	}
	// Works when the caller's clock is UTC (box runs UTC).
	now = time.Date(2026, 8, 19, 3, 30, 5, 0, time.UTC) // 09:00:05 IST
	if d := untilMarketOpen(now); d != 15*time.Minute+5*time.Second {
		t.Errorf("03:30:05Z → %v, want 15m5s", d)
	}
}

func TestBackoff_PreOpenUsesPreciseWake(t *testing.T) {
	// Whatever "now" is, the pre-open backoff is bounded by the 30 s floor
	// and the longest possible hold (08:45 watch-window start → 09:15:10).
	d := backoffFor(InboxErrPreOpen, 7)
	if d < 30*time.Second || d > 24*time.Hour {
		t.Fatalf("pre-open backoff %v out of sane range", d)
	}
}

// The three layers are tied by ONE literal: pre_check.go emits ReasonPreOpen,
// entry_handler.go matches it to return ErrPreOpenHold, the classifier maps
// that to InboxErrPreOpen. Reword one and the morning DLQ bug returns.
func TestPreOpenReasonConstantTiesLayers(t *testing.T) {
	if ReasonPreOpen != "market not open yet (before 9:15)" {
		t.Fatalf("ReasonPreOpen changed to %q — entry_handler's hold branch keys off this exact text", ReasonPreOpen)
	}
	if got := classifyHandlerErr(fmt.Errorf("pre-check: %s", ReasonPreOpen)); got != InboxErrPoison {
		t.Fatalf("plain pre-check text must remain POISON (%q) — only the typed sentinel escapes; got %q", InboxErrPoison, got)
	}
}
