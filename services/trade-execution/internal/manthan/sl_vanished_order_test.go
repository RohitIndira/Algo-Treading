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

	// Simulate the first emergency sell for S4450/IOLCP having just fired.
	// The latch is keyed by user AND symbol (2026-10-22): two clients holding
	// the same stock must each get their own exit.
	h.emergencyMu.Lock()
	h.lastEmergency["S4450::IOLCP"] = time.Now() // user:strategy:symbol (strategy empty in this test)
	h.emergencyMu.Unlock()

	// A second fire inside the window must be refused before any broker call:
	// emergencySellInternal's latch check runs first, so with a nil broker a
	// suppressed call returns an error instead of panicking on the adapter.
	err := h.emergencySellInternal(nil, exitContext{}, &SymbolInfo{Symbol: "IOLCP"}, BrokerAuth{UserID: "S4450"}, 121, "SL modify failed after retry")
	if err == nil {
		t.Fatal("second emergency sell inside the latch window must be suppressed")
	}

	// Another client's IOLCP, and S4450's other symbol, are unaffected by
	// that latch — they proceed past it (and only then fail on the nil broker
	// in this test setup).
	for _, c := range []struct{ user, symbol string }{{"ISPL19694", "IOLCP"}, {"S4450", "OTHER"}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s/%s: expected to pass the latch and reach the (nil) broker", c.user, c.symbol)
				}
			}()
			_ = h.emergencySellInternal(nil, exitContext{}, &SymbolInfo{Symbol: c.symbol}, BrokerAuth{UserID: c.user}, 1, "test")
		}()
	}
}
