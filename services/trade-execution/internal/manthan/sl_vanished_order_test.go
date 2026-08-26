package manthan

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
)

// 2026-08-26 IOLCP incident: Indira silently downgraded a GTC SL to DAY, it
// expired at close, and next morning's trail modify hit "AU004 Pending Order
// Not Found". That error was classified as an auth failure (any "AU004"), the
// refresh+retry loop couldn't fix a nonexistent order, and the handler
// escalated to EMERGENCY MARKET SELL — repeatedly, every trail tick — on a
// healthy, profitable position. Only the broker's holdings limit stopped the
// duplicate sells from going short.

func TestOrderNotFoundIsNotAuthError(t *testing.T) {
	vanished := fmt.Errorf("modify order request failed: indira: session expired (AU0xx): AU004 Pending Order Not Found.(CFIXBusinessServer::ProcessModify)")

	if !IsOrderNotFoundError(vanished) {
		t.Fatal("'AU004 Pending Order Not Found' must classify as order-not-found")
	}
	if IsAuthError(vanished) {
		t.Fatal("'AU004 Pending Order Not Found' must NOT classify as an auth error — it means the modify TARGET is gone, not the session")
	}

	// Genuine auth failures still classify as auth.
	for _, e := range []error{
		fmt.Errorf("indira: AU004 Session data not received"),
		fmt.Errorf("HTTP error 401: Session expired"),
	} {
		if IsAuthError(e) != true {
			t.Fatalf("%q must remain an auth error", e)
		}
		if IsOrderNotFoundError(e) {
			t.Fatalf("%q must not classify as order-not-found", e)
		}
	}
}

func TestEmergencySellLatchRefusesRepeat(t *testing.T) {
	h := &SLHandler{lastEmergency: make(map[string]time.Time), logger: zap.NewNop()}

	// Simulate the first emergency sell having just fired.
	h.emergencyMu.Lock()
	h.lastEmergency["IOLCP"] = time.Now()
	h.emergencyMu.Unlock()

	// A second fire inside the window must be refused before any broker call:
	// emergencySellInternal's latch check runs first, so with a nil broker a
	// suppressed call returns an error instead of panicking on the adapter.
	err := h.emergencySellInternal(nil, &SymbolInfo{Symbol: "IOLCP"}, BrokerAuth{}, 121, "SL modify failed after retry")
	if err == nil {
		t.Fatal("second emergency sell inside the latch window must be suppressed")
	}

	// A different symbol is unaffected by IOLCP's latch — it proceeds past
	// the latch (and only then fails on the nil broker in this test setup).
	defer func() { _ = recover() }() // nil-broker panic beyond the latch is fine here
	_ = h.emergencySellInternal(nil, &SymbolInfo{Symbol: "OTHER"}, BrokerAuth{}, 1, "test")
}
