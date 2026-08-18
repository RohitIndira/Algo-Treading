package manthan

// Regression guard for the 2026-08-05 double-buy incident: fill confirmations
// were never wired, every position sat PENDING_ENTRY/Active=false forever, and
// the allocator's "already holding" check (which required pos.Active) waved
// day-2 signals through — the algo re-bought 7 symbols it already held with
// real money. The guard must block re-entry for ANY tracked non-exited state.

import (
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
	"go.uber.org/zap"
)

func reentryPortfolio() *types.Portfolio {
	return &types.Portfolio{
		UserID:         "S4450",
		StrategyID:     "strat-1",
		InitialCapital: 500000,
		CurrentCapital: 500000,
		MaxPositions:   25,
		PerStockBase:   20000,
		Positions: map[string]*types.Position{
			// The incident state: entry published, fill never confirmed.
			"PENDINGSYM": {Symbol: "PENDINGSYM", State: types.StatePendingEntry, Active: false, Quantity: 10},
			// Normal held position.
			"ACTIVESYM": {Symbol: "ACTIVESYM", State: types.StateActive, Active: true, Quantity: 5},
			// Exited — re-entry is allowed (cooldown is a separate gate).
			"EXITEDSYM": {Symbol: "EXITEDSYM", State: types.StateExited, Active: false},
		},
		Cooldown: map[string]*types.CooldownEntry{},
	}
}

func sig(symbol string) types.ManthanSignal {
	return types.ManthanSignal{
		RunDate: "2026-08-05", Symbol: symbol, ISIN: "INE000TEST", Industry: "Test",
		MCapBucket: "SMALL", IndexName: "NTYSLCP250", LatestPrice: 100, ATHClose: 120, Week52High: 120,
	}
}

func TestAllocator_BlocksReentryForAnyTrackedNonExitedState(t *testing.T) {
	a := NewAllocator(zap.NewNop())
	p := reentryPortfolio()

	res := a.Allocate(
		[]types.ManthanSignal{sig("PENDINGSYM"), sig("ACTIVESYM"), sig("EXITEDSYM"), sig("FRESHSYM")},
		p, map[string]float64{"NTYSLCP250": 1.0}, // full EMA fraction so fresh symbols pass the EMA gate
	)

	skipped := map[string]string{}
	for _, s := range res.Skipped {
		skipped[s.Symbol] = s.Reason
	}
	allocated := map[string]bool{}
	for _, al := range res.Allocations {
		allocated[al.Symbol] = true
	}

	// The incident case: PENDING_ENTRY must block re-entry.
	if allocated["PENDINGSYM"] {
		t.Fatalf("PENDING_ENTRY symbol was re-allocated — double-buy bug is back (skips=%v)", skipped)
	}
	if r, ok := skipped["PENDINGSYM"]; !ok || r == "" {
		t.Errorf("PENDINGSYM should be skipped with a reason, got skips=%v", skipped)
	}
	// ACTIVE must still block.
	if allocated["ACTIVESYM"] {
		t.Error("ACTIVE symbol was re-allocated")
	}
	// EXITED must NOT be blocked by the holding guard (cooldown is separate).
	if r, ok := skipped["EXITEDSYM"]; ok && r != "" && (r == "already holding" || r == "already holding (EXITED)") {
		t.Errorf("EXITED symbol blocked by holding guard: %q", r)
	}
	// A fresh symbol must still allocate — the guard must not over-block.
	if !allocated["FRESHSYM"] {
		t.Errorf("fresh symbol was not allocated; skips=%v", skipped)
	}
}

// Pendings must reserve their slot AND their sector/bucket cap from dispatch,
// otherwise every signal in a morning batch (one Kafka message each) sees the
// same free slot while earlier dispatches are still unfilled — 2026-08-17:
// FIV99 dispatched 26 entries on a 25-slot book.
func TestCaps_PendingEntriesOccupySlotsAndBuckets(t *testing.T) {
	positions := map[string]*types.Position{
		"A": {Symbol: "A", Industry: "Auto", MCapBucket: "SMALL", State: types.StatePendingEntry, Active: false},
		"B": {Symbol: "B", Industry: "Auto", MCapBucket: "SMALL", State: types.StateActive, Active: true},
		"C": {Symbol: "C", Industry: "Auto", MCapBucket: "SMALL", State: types.StateExitPending, Active: true},
		"X": {Symbol: "X", Industry: "Auto", MCapBucket: "SMALL", State: types.StateExited, Active: false},
	}
	if got := countActive(positions); got != 3 {
		t.Fatalf("occupied slots = %d, want 3 (pending + active + exit-pending; exited excluded)", got)
	}
	caps := types.NewCapCheck(4, positions) // MaxPerSector = 1, MaxPerBucket = 2
	if caps.SectorCount["Auto"] != 3 || caps.BucketCount["SMALL"] != 3 {
		t.Fatalf("cap counters = sector %d / bucket %d, want 3/3", caps.SectorCount["Auto"], caps.BucketCount["SMALL"])
	}
	if ok, _ := caps.CanAdd("Auto", "SMALL"); ok {
		t.Fatal("cap must be reached with pendings counted")
	}
}
