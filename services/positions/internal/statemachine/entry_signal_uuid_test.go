package statemachine

import (
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
)

func TestCanonicalEntrySignalUUID(t *testing.T) {
	const parent = "0ea840fc-83f5-506d-8f87-6b46387a2f44"
	cases := map[string]tradeexec.OrderMeta{
		"plain uuid":                      {EntrySignalID: parent},
		"mkt- top-up child":               {EntrySignalID: "mkt-" + parent},
		"empty entry, child in signal_id": {SignalID: "mkt-" + parent},
	}
	for name, m := range cases {
		if got := canonicalEntrySignalUUID(m); got != parent {
			t.Fatalf("%s: got %q want %q", name, got, parent)
		}
	}
	if got := canonicalEntrySignalUUID(tradeexec.OrderMeta{EntrySignalID: "garbage"}); got != "" {
		t.Fatalf("garbage: got %q", got)
	}
}
