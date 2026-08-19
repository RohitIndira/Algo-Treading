package statemachine

import (
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
)

// 2026-08-11 S4450 CUB: the market-fallback leg's signal_id "mkt-<uuid>" was
// passed into the UUID column → INSERT failed → 91-share position silently
// never existed in positions_db (live-algo API showed 16 of 17 holdings).
func TestCanonicalSignalUUID(t *testing.T) {
	const parent = "bb193fed-5262-5c16-94bc-ae3cacd6b2fc"
	cases := []struct {
		name string
		meta tradeexec.OrderMeta
		want string
	}{
		{"plain entry uuid", tradeexec.OrderMeta{SignalID: parent}, parent},
		{"market fallback leg", tradeexec.OrderMeta{SignalID: "mkt-" + parent}, parent},
		{"sl child id", tradeexec.OrderMeta{SignalID: "sl-" + parent}, parent},
		{"protective replay id", tradeexec.OrderMeta{SignalID: "protective-" + parent + "-20260819"}, parent},
		{"entry signal wins when present", tradeexec.OrderMeta{SignalID: "mkt-" + parent, EntrySignalID: "11111111-2222-3333-4444-555555555555"}, "11111111-2222-3333-4444-555555555555"},
		{"garbage → empty", tradeexec.OrderMeta{SignalID: "manual"}, ""},
	}
	for _, c := range cases {
		if got := canonicalSignalUUID(c.meta); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
