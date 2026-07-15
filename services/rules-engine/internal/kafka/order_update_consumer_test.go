package kafka

import "testing"

func TestSignalStatusFor(t *testing.T) {
	cases := []struct {
		name       string
		updateType string
		status     string
		wantStatus string
		wantOK     bool
	}{
		// Terminal cancels are recognised by update type.
		{"cap reached", "STRATEGY_TRADE_CAP_REACHED", "CANCELLED", "CANCELLED", true},
		{"watch cancelled", "PRICE_WATCH_CANCELLED", "CANCELLED", "CANCELLED", true},
		{"rejected", "ORDER_REJECTED", "REJECTED", "FAILED", true},
		// Fills counted as committed trades.
		{"paper filled", "ORDER_PAPER_FILLED", "FILLED", "EXECUTED", true},
		{"live executed", "EXECUTION_SUCCESS", "EXECUTED", "EXECUTED", true},
		{"status traded", "SOME_TYPE", "TRADED", "EXECUTED", true},
		// Downstream failures / cancels via status.
		{"status failed", "SOME_TYPE", "FAILED", "FAILED", true},
		{"status cancelled", "SOME_TYPE", "CANCELLED", "CANCELLED", true},
		// Transient updates must NOT change status.
		{"triggered is transient", "PRICE_MONITOR_TRIGGERED", "PENDING", "", false},
		{"pending is transient", "SOME_TYPE", "PENDING", "", false},
		{"empty", "", "", "", false},
		// Case-insensitivity.
		{"lowercase status", "x", "filled", "EXECUTED", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotStatus, gotOK := signalStatusFor(orderUpdate{UpdateType: c.updateType, Status: c.status})
			if gotStatus != c.wantStatus || gotOK != c.wantOK {
				t.Fatalf("signalStatusFor(%q,%q) = (%q,%v), want (%q,%v)",
					c.updateType, c.status, gotStatus, gotOK, c.wantStatus, c.wantOK)
			}
		})
	}
}
