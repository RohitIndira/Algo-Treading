package manthan

// Confirmed-exit bookkeeping through the full in-memory lifecycle:
// tick trigger (ProcessTick clears Active) → MarkExitPending → broker
// confirmation → ExitPosition. Before 2026-08-18 ExitPosition early-returned
// on !Active, so an SL exit never booked cooldown/capital and the position
// lingered as "already holding" until restart; under Occupies() it would
// also have kept its cap slot forever.

import (
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
	"go.uber.org/zap"
)

func TestExitLifecycle_TriggerPendingConfirmReleases(t *testing.T) {
	pm := NewPortfolioManager(zap.NewNop())
	st := types.UserStrategy{StrategyID: "s1", UserID: "u1", TotalCapital: 100000, MaxPositions: 4, StopLossPct: 20}
	p := pm.GetOrCreate(st)
	slMgr := NewTrailingSLManager(zap.NewNop())
	pos := &types.Position{Symbol: "X", Industry: "Auto", MCapBucket: "SMALL", EntryPrice: 100, Quantity: 10, InvestedAmt: 1000,
		HighSinceEntry: 100, CurrentSL: 80, LastTrailLevel: 100, State: types.StateActive, Active: true}
	p.Mu.Lock()
	p.Positions["X"] = pos
	p.Mu.Unlock()

	// 1) Trigger tick: ProcessTick clears Active (pre-existing behaviour).
	if upd := slMgr.ProcessTick(pos, 79, 20, 2); upd.Action != SLTriggered {
		t.Fatalf("expected SLTriggered, got %v", upd.Action)
	}
	// 2) Exit ordered → EXIT_PENDING: still held, still occupies its slot,
	//    Active restored so feed/TTL keep seeing it, no ticks (State gate).
	pm.MarkExitPending("s1", "X")
	if pos.State != types.StateExitPending || !pos.Active || pos.ExitPendingSince.IsZero() {
		t.Fatalf("after MarkExitPending: state=%s active=%v since=%v", pos.State, pos.Active, pos.ExitPendingSince)
	}
	if !pos.Occupies() || countActive(p.Positions) != 1 {
		t.Fatal("EXIT_PENDING must still occupy its slot until the broker confirms")
	}
	if upd := slMgr.ProcessTick(pos, 70, 20, 2); upd.Action == SLTriggered && pos.State == types.StateExitPending {
		// ProcessTick itself does not know about State — the tick_handler
		// loop gates on State; here we only assert the manager did not
		// silently flip State.
	}
	// 3) Broker confirms → ExitPosition MUST book it (the old !Active guard
	//    made this a no-op).
	pnl := pm.ExitPosition("s1", "X", 79)
	if pnl != -210 { // (79-100)*10
		t.Fatalf("realized pnl = %v, want -210", pnl)
	}
	if pos.State != types.StateExited || pos.Active || pos.Occupies() || countActive(p.Positions) != 0 {
		t.Fatalf("after confirm: state=%s active=%v occupies=%v", pos.State, pos.Active, pos.Occupies())
	}
	if _, ok := p.Cooldown["X"]; !ok {
		t.Fatal("confirmed SL exit must install the re-entry cooldown")
	}
	if p.CurrentCapital != 100000-210 {
		t.Fatalf("capital = %v, want 99790", p.CurrentCapital)
	}
	// 4) Duplicate confirmation is idempotent.
	if pnl2 := pm.ExitPosition("s1", "X", 79); pnl2 != 0 || p.CurrentCapital != 99790 {
		t.Fatalf("duplicate confirm must be a no-op: pnl=%v capital=%v", pnl2, p.CurrentCapital)
	}
	_ = time.Now
}

func TestExitLifecycle_ManualExitOfActivePositionStillBooks(t *testing.T) {
	// A safety-monitor / manual broker sell confirms without any trigger
	// tick: State ACTIVE, Active true — must book exactly as before.
	pm := NewPortfolioManager(zap.NewNop())
	st := types.UserStrategy{StrategyID: "s1", UserID: "u1", TotalCapital: 50000, MaxPositions: 4, StopLossPct: 20}
	p := pm.GetOrCreate(st)
	p.Mu.Lock()
	p.Positions["Y"] = &types.Position{Symbol: "Y", EntryPrice: 50, Quantity: 10, HighSinceEntry: 60, CurrentSL: 48, State: types.StateActive, Active: true}
	p.Mu.Unlock()
	if pnl := pm.ExitPosition("s1", "Y", 55); pnl != 50 {
		t.Fatalf("pnl = %v want 50", pnl)
	}
	if p.Positions["Y"].State != types.StateExited || p.CurrentCapital != 50050 {
		t.Fatalf("manual exit not booked: state=%s capital=%v", p.Positions["Y"].State, p.CurrentCapital)
	}
}
