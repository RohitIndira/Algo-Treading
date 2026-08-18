package manthan

// The creation-time gate (2026-08-18): a strategy acts ONLY on signals whose
// first appearance in the Buy list is AFTER the strategy was created. The
// daily publish re-emits the whole list every day (emitted_at is always
// "today"), so the anchor is first_seen_at, stamped by data-ingestion and
// carried forward across days.
//
// Incident: FIV99 created Fri 2026-08-14 16:20Z; on Mon 2026-08-17 09:00 IST
// the republish dispatched it the entire 26-stock list (all first seen
// 2026-08-10) plus 3 stocks first seen Fri 03:30Z — every one predates the
// strategy and must be skipped; MANORAMA/SHANTIGOLD (first seen Mon 06:12Z)
// are the only genuine signals for it.

import (
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

func TestCreationGate_FIV99MondayScenario(t *testing.T) {
	fiv99 := types.UserStrategy{StrategyID: "e36bd07e", UserID: "FIV99",
		CreatedAt: time.Date(2026, 8, 14, 16, 20, 10, 0, time.UTC)}
	s4450 := types.UserStrategy{StrategyID: "a6cb5b08", UserID: "S4450",
		CreatedAt: time.Date(2026, 8, 11, 6, 35, 25, 0, time.UTC)}
	monday := "2026-08-17T03:30:13Z" // the republish instant — NOT the anchor

	cases := []struct {
		symbol, firstSeen    string
		fiv99Skip, s4450Skip bool
	}{
		{"GNA", "2026-08-10T07:57:53Z", true, true},        // old cohort: predates both (S4450 already holds it anyway)
		{"RAMRAT", "2026-08-14T03:30:13Z", true, false},    // Fri morning: before FIV99 (Fri night), after S4450
		{"MANORAMA", "2026-08-17T06:12:45Z", false, false}, // Mon: genuine signal for both
		{"MODISONLTD", "2026-08-18T06:27:53Z", false, false},
	}
	for _, c := range cases {
		sig := types.ManthanSignal{Symbol: c.symbol, EmittedAt: monday, FirstSeenAt: c.firstSeen}
		if got, _ := signalPredatesStrategy(sig, fiv99); got != c.fiv99Skip {
			t.Errorf("%s FIV99: predates=%v want %v", c.symbol, got, c.fiv99Skip)
		}
		if got, _ := signalPredatesStrategy(sig, s4450); got != c.s4450Skip {
			t.Errorf("%s S4450: predates=%v want %v", c.symbol, got, c.s4450Skip)
		}
	}
}

func TestCreationGate_RepublishNeverReopens(t *testing.T) {
	// A stock first seen on the 10th stays "predates" on every later
	// republish, no matter what emitted_at says.
	strat := types.UserStrategy{CreatedAt: time.Date(2026, 8, 14, 16, 20, 0, 0, time.UTC)}
	for _, day := range []string{"2026-08-17T03:30:00Z", "2026-08-19T03:30:00Z", "2026-09-01T03:30:00Z"} {
		sig := types.ManthanSignal{Symbol: "GNA", EmittedAt: day, FirstSeenAt: "2026-08-10T07:57:53Z"}
		if got, _ := signalPredatesStrategy(sig, strat); !got {
			t.Errorf("republish on %s must still be skipped", day)
		}
	}
	// ...but a stock that LEFT the list and came back is a new run (new
	// first_seen_at) and IS this strategy's signal.
	sig := types.ManthanSignal{Symbol: "GNA", EmittedAt: "2026-09-01T03:30:00Z", FirstSeenAt: "2026-09-01T03:30:00Z"}
	if got, _ := signalPredatesStrategy(sig, strat); got {
		t.Error("re-added stock (fresh first_seen_at) must be processed")
	}
}

func TestCreationGate_SameDayCatchUpIsSkipped(t *testing.T) {
	// Strategy created 11:00 IST; today's 09:00 IST publish predates it.
	strat := types.UserStrategy{CreatedAt: time.Date(2026, 8, 19, 5, 30, 0, 0, time.UTC)}
	sig := types.ManthanSignal{Symbol: "X", EmittedAt: "2026-08-19T03:30:00Z", FirstSeenAt: "2026-08-19T03:30:00Z"}
	if got, _ := signalPredatesStrategy(sig, strat); !got {
		t.Error("today's earlier signal must be skipped for a strategy created later today")
	}
	// A stock added to the sheet at 12:00 IST (after creation) is taken.
	sig = types.ManthanSignal{Symbol: "Y", EmittedAt: "2026-08-19T06:30:00Z", FirstSeenAt: "2026-08-19T06:30:00Z"}
	if got, _ := signalPredatesStrategy(sig, strat); got {
		t.Error("signal that arrives after creation must be processed")
	}
}

func TestCreationGate_FallbacksFailOpen(t *testing.T) {
	strat := types.UserStrategy{CreatedAt: time.Date(2026, 8, 14, 16, 20, 0, 0, time.UTC)}
	// Legacy payload without first_seen_at → emitted_at is the anchor.
	sig := types.ManthanSignal{Symbol: "L", EmittedAt: "2026-08-10T07:57:53Z"}
	if got, _ := signalPredatesStrategy(sig, strat); !got {
		t.Error("legacy payload: emitted_at before creation must be skipped")
	}
	// Nothing parseable → fail-open (never silently blocks a user).
	sig = types.ManthanSignal{Symbol: "N"}
	if got, _ := signalPredatesStrategy(sig, strat); got {
		t.Error("unstamped signal must not be blocked")
	}
	// Unknown strategy CreatedAt → fail-open.
	sig = types.ManthanSignal{Symbol: "U", FirstSeenAt: "2026-08-10T07:57:53Z"}
	if got, _ := signalPredatesStrategy(sig, types.UserStrategy{}); got {
		t.Error("strategy with zero CreatedAt must not block signals")
	}
}
