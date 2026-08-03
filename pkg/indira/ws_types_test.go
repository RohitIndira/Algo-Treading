package indira

// FIX 7: the broker's EXECUTED trade frame (MessageType=TRD_MSG, verified live
// 2026-08-03) carries the fill in TradeQty / QuantityTradedToday, NOT TradedQTY
// (which is 0 on the ORD_NRML ack). These prove:
//   - FilledQty()           = per-trade  (TradeQty-first) for the broker_events log
//   - CumulativeFilledQty() = order total (QuantityTradedToday, NEVER TradeQty)
//     so the partial-fill check can't fire a spurious topup on a completing trade.

import (
	"encoding/json"
	"testing"
)

func parseWS(t *testing.T, raw string) *WSOrderStatus {
	t.Helper()
	var w WSOrderStatus
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &w
}

func TestFilledQty_PerTrade(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		// per-trade: the fill for THIS frame — 4, not 0 (TradedQTY) and not 38 (OrderOriginalQty)
		{"TRD_MSG partial 4 of 38", `{"MessageType":"TRD_MSG","OrderStatus":"EXECUTED","TradeQty":"4","QuantityTradedToday":4,"OrderOriginalQty":38,"TradedPrice":"100.5"}`, 4},
		{"TRD_MSG full fill", `{"MessageType":"TRD_MSG","OrderStatus":"EXECUTED","TradeQty":"1","QuantityTradedToday":1,"OrderOriginalQty":1}`, 1},
		{"ORD_NRML pending", `{"MessageType":"ORD_NRML","OrderStatus":"PENDING","TradedQTY":"0","OrderOriginalQty":38}`, 0},
		{"legacy TradedQTY only", `{"OrderStatus":"EXECUTED","TradedQTY":"7"}`, 7},
		{"number-typed QuantityTradedToday, no TradeQty", `{"MessageType":"TRD_MSG","QuantityTradedToday":5}`, 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseWS(t, c.raw).FilledQty(); got != c.want {
				t.Fatalf("FilledQty()=%d, want %d", got, c.want)
			}
		})
	}
}

func TestCumulativeFilledQty(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"partial cumulative 4", `{"MessageType":"TRD_MSG","OrderStatus":"EXECUTED","TradeQty":"4","QuantityTradedToday":4,"OrderOriginalQty":38}`, 4},
		// THE money case: a completing trade — cumulative must be 38 (total), NOT 34 (this trade).
		// Using TradeQty here (34 < 38) would fire a spurious MARKET topup.
		{"completing trade → total not per-trade", `{"MessageType":"TRD_MSG","OrderStatus":"EXECUTED","TradeQty":"34","QuantityTradedToday":38,"OrderOriginalQty":38}`, 38},
		{"ORD_NRML pending", `{"MessageType":"ORD_NRML","OrderStatus":"PENDING","TradedQTY":"0"}`, 0},
		{"legacy TradedQTY only", `{"OrderStatus":"EXECUTED","TradedQTY":"7"}`, 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseWS(t, c.raw).CumulativeFilledQty(); got != c.want {
				t.Fatalf("CumulativeFilledQty()=%d, want %d", got, c.want)
			}
		})
	}
}
