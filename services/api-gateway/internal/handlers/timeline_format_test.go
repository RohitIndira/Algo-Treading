package handlers

import (
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

func TestRupees_IndianGrouping(t *testing.T) {
	cases := map[float64]string{
		0:        "₹0",
		500:      "₹500",
		5000:     "₹5,000",
		100000:   "₹1,00,000",
		500000:   "₹5,00,000",
		12500000: "₹1,25,00,000",
		-3200:    "-₹3,200",
	}
	for in, want := range cases {
		if got := rupees(in); got != want {
			t.Errorf("rupees(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatTimeline_Cases(t *testing.T) {
	// DEPLOYED reads capital from details JSON.
	icon, title, sub := formatTimeline(livealgos.TimelineRow{Kind: "DEPLOYED", DetailsJS: `{"capital":500000}`})
	if icon != "deployed" || title != "Algo deployed" || sub != "Algo was made live with capital deployment of ₹5,00,000" {
		t.Errorf("DEPLOYED wrong: %q / %q / %q", icon, title, sub)
	}
	// CAPITAL_DECREASED reads from/to.
	_, _, sub = formatTimeline(livealgos.TimelineRow{Kind: "CAPITAL_DECREASED", DetailsJS: `{"from":700000,"to":400000}`})
	if sub != "Deployed capital decreased from ₹7,00,000 to ₹4,00,000" {
		t.Errorf("CAPITAL_DECREASED wrong: %q", sub)
	}
	// Order fill.
	_, title, sub = formatTimeline(livealgos.TimelineRow{Kind: "ORDER_FILLED", Symbol: "KEI", Qty: 3, Price: 5626})
	if title != "Order filled" || sub != "Bought 3 KEI at ₹5,626" {
		t.Errorf("ORDER_FILLED wrong: %q / %q", title, sub)
	}
	// Unknown kind is safe (no panic, echoes kind).
	if i, tt, _ := formatTimeline(livealgos.TimelineRow{Kind: "WEIRD"}); i != "order" || tt != "WEIRD" {
		t.Errorf("unknown kind not handled: %q / %q", i, tt)
	}
}
