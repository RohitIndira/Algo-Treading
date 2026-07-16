package statemachine

import (
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/consumer"
)

// TestIsSLOrderTypeString locks down which broker `order_type` strings we
// treat as SL orders. Broker sources aren't consistent — REST_ORDERBOOK
// writes "SL", WSS bridge may write "SL-L" or "SL_LIMIT_SELL". Keep this
// list authoritative — a mis-match here silently drops SL_MODIFY events.
func TestIsSLOrderTypeString(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"SL", true},        // REST_ORDERBOOK canonical
		{"sl", true},        // case-insensitive
		{"  SL  ", true},    // whitespace tolerated
		{"SL_LIMIT", true},  // hypothetical WSS extended form
		{"SL_SELL", true},   // trade-execution's internal enum shape
		{"SL-L", true},      // Codifi/Indira shorthand
		{"SL-M", true},      // SL market variant
		{"Limit", false},    // entry LIMIT — not SL
		{"MARKET", false},   // entry MARKET
		{"REGULAR LIMIT", false},
		{"", false}, // empty → false (never routes)
		{"COVER", false},
		{"BO", false}, // bracket order — different animal, if we ever support it
	}
	for _, tc := range tests {
		if got := isSLOrderTypeString(tc.in); got != tc.want {
			t.Errorf("isSLOrderTypeString(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestIsSLTrackingEvent covers the full routing decision — every
// combination of (order_type, event_type, trigger_price, filled_qty)
// that could plausibly arrive on order.events. The routing gates
// current_sl mutation, so a false-positive here would corrupt SL
// tracking for a non-SL order.
func TestIsSLTrackingEvent(t *testing.T) {
	tests := []struct {
		name string
		ev   *consumer.OrderEvent
		want bool
	}{
		{
			name: "SL PENDING with trigger — routes to SL tracking",
			ev: &consumer.OrderEvent{
				OrderType:    "SL",
				EventType:    "STATUS_CHANGED",
				Status:       "PENDING",
				TriggerPrice: 512.90,
			},
			want: true,
		},
		{
			name: "SL trailing ratchet — new trigger via STATUS_CHANGED",
			ev: &consumer.OrderEvent{
				OrderType:    "SL",
				EventType:    "STATUS_CHANGED",
				Status:       "PENDING",
				TriggerPrice: 520.30,
			},
			want: true,
		},
		{
			name: "SL cancelled without a fill — routes to clear",
			ev: &consumer.OrderEvent{
				OrderType: "SL",
				EventType: "CANCELLED",
				FilledQty: 0,
			},
			want: true,
		},
		{
			name: "SL cancelled BUT partial-filled first — NOT SL tracking (goes to fill path instead)",
			ev: &consumer.OrderEvent{
				OrderType: "SL",
				EventType: "CANCELLED",
				FilledQty: 57, // triggered at broker, partial fill before cancel — treat as fill
			},
			want: false,
		},
		{
			name: "SL fills (SL trigger executed) — NOT SL tracking (goes to fill path as SELL exit)",
			ev: &consumer.OrderEvent{
				OrderType:    "SL",
				EventType:    "FILLED",
				TriggerPrice: 512.90,
				FilledQty:    38,
			},
			want: false,
		},
		{
			name: "SL partial fill on trigger — NOT SL tracking (fill path)",
			ev: &consumer.OrderEvent{
				OrderType:    "SL",
				EventType:    "PARTIALLY_FILLED",
				TriggerPrice: 512.90,
				FilledQty:    10,
			},
			want: false,
		},
		{
			name: "Entry LIMIT PENDING — never SL tracking (wrong order_type)",
			ev: &consumer.OrderEvent{
				OrderType: "Limit",
				EventType: "STATUS_CHANGED",
				Status:    "PENDING",
			},
			want: false,
		},
		{
			name: "SL STATUS_CHANGED with no trigger and no cancel — no-op",
			ev: &consumer.OrderEvent{
				OrderType: "SL",
				EventType: "STATUS_CHANGED",
				Status:    "MODIFIED", // e.g. broker announcing a MODIFIED but the payload didn't carry the new trigger
			},
			want: false,
		},
		{
			name: "empty order_type — never SL tracking",
			ev: &consumer.OrderEvent{
				OrderType:    "",
				EventType:    "STATUS_CHANGED",
				TriggerPrice: 100,
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSLTrackingEvent(tc.ev); got != tc.want {
				t.Errorf("isSLTrackingEvent = %v, want %v", got, tc.want)
			}
		})
	}
}
