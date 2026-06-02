package statusservice

import (
	"testing"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// TestResolveFilledQty covers the broker WS fill-quantity derivation, especially
// the EXECUTED-with-empty-TradedQTY case that previously left filled_quantity=0
// on a genuine fill and caused USER-DIRECT misattribution.
func TestResolveFilledQty(t *testing.T) {
	tests := []struct {
		name       string
		tradedQty  string
		pendingQty string
		originalQty int
		status     string
		placedQty  int
		want       int
	}{
		{
			name:      "explicit TradedQTY is respected",
			tradedQty: "15", status: "EXECUTED", originalQty: 15, want: 15,
		},
		{
			name:      "explicit zero TradedQTY is not overridden",
			tradedQty: "0", status: "EXECUTED", originalQty: 15, want: 0,
		},
		{
			// The IEX/PPL/DABUR production case: EXECUTED, empty TradedQTY, full fill.
			name:      "empty TradedQTY on EXECUTED derives from original-pending",
			tradedQty: "", pendingQty: "0", originalQty: 15, status: "EXECUTED", want: 15,
		},
		{
			name:      "empty TradedQTY partial fill uses original minus pending",
			tradedQty: "", pendingQty: "5", originalQty: 15, status: "EXECUTED", want: 10,
		},
		{
			// The NMDC case: TRADED with empty pending field (flexToInt -> 0).
			name:      "empty TradedQTY and empty PendingQty on TRADED",
			tradedQty: "", pendingQty: "", originalQty: 7, status: "TRADED", want: 7,
		},
		{
			name:      "empty TradedQTY falls back to placed qty when original missing",
			tradedQty: "", pendingQty: "", originalQty: 0, status: "EXECUTED", placedQty: 20, want: 20,
		},
		{
			name:      "non-fill status with empty TradedQTY leaves qty unchanged",
			tradedQty: "", pendingQty: "0", originalQty: 15, status: "CANCELLED", want: -1,
		},
		{
			name:      "fill status but no qty signal anywhere",
			tradedQty: "", pendingQty: "0", originalQty: 0, status: "EXECUTED", placedQty: 0, want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &indiraClient.WSOrderStatus{
				TradedQTY:        indiraClient.FlexInt(tt.tradedQty),
				PendingQty:       indiraClient.FlexInt(tt.pendingQty),
				OrderOriginalQty: tt.originalQty,
				OrderStatus:      tt.status,
			}
			if got := resolveFilledQty(ws, tt.placedQty); got != tt.want {
				t.Errorf("resolveFilledQty() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestParseTradeBookTime verifies the REST trade-book timestamp format parses in IST.
func TestParseTradeBookTime(t *testing.T) {
	got := parseTradeBookTime("2026-06-01 14:38:41")
	if got.IsZero() {
		t.Fatalf("parseTradeBookTime returned zero for a valid timestamp")
	}
	if h, m, s := got.Clock(); h != 14 || m != 38 || s != 41 {
		t.Errorf("parsed clock = %02d:%02d:%02d, want 14:38:41", h, m, s)
	}
	if !parseTradeBookTime("").IsZero() || !parseTradeBookTime("01-Jun-2026 14:38:41").IsZero() {
		t.Errorf("expected zero time for empty/wrong-format input")
	}
}

// TestAggregateTrades covers the trade-book reduction used for authoritative fills.
func TestAggregateTrades(t *testing.T) {
	// Single full fill (the IEX production case).
	qty, price, ts, ok := aggregateTrades([]indiraClient.TradeBook{
		{OrdId: "NZWKE0002F16", TradedQty: 15, TradedPrice: 126.6, TradeTime: "2026-06-01 14:38:41"},
	}, "NZWKE0002F16")
	if !ok || qty != 15 || price != 126.6 || ts.IsZero() {
		t.Fatalf("single fill: got qty=%d price=%v ts=%v ok=%v", qty, price, ts, ok)
	}

	// Two partial fills → summed qty, VWAP price, latest time.
	qty, price, ts, ok = aggregateTrades([]indiraClient.TradeBook{
		{OrdId: "X", TradedQty: 10, TradedPrice: 100, TradeTime: "2026-06-01 10:00:00"},
		{OrdId: "X", TradedQty: 30, TradedPrice: 110, TradeTime: "2026-06-01 10:05:00"},
	}, "X")
	wantVWAP := (10*100.0 + 30*110.0) / 40.0
	if !ok || qty != 40 || price != wantVWAP {
		t.Fatalf("partial fills: got qty=%d price=%v want qty=40 price=%v", qty, price, wantVWAP)
	}
	if h, m, _ := ts.Clock(); h != 10 || m != 5 {
		t.Errorf("partial fills: latest time = %02d:%02d, want 10:05", h, m)
	}

	// Stray rows for other orders are ignored; empty / zero-qty → ok=false.
	if _, _, _, ok := aggregateTrades([]indiraClient.TradeBook{{OrdId: "OTHER", TradedQty: 5}}, "MINE"); ok {
		t.Errorf("expected ok=false when no trades match the order id")
	}
	if _, _, _, ok := aggregateTrades(nil, "X"); ok {
		t.Errorf("expected ok=false for empty trade list")
	}
}
