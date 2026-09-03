package manthan

// SyncExitedFromDB tests (real local trading_db; skips if unreachable) —
// the DB→memory half of the 2026-09-03 ghost slot-release fix:
//
//	TestSyncExitedFromDB_ReleasesExternallyExitedSlot — an EXITED row written
//	    by an external actor (admin ghost heal) books the in-memory exit,
//	    frees the slot, keeps capital neutral when no exit price exists,
//	    and is idempotent on a second pass.
//	TestSyncExitedFromDB_NoOpWhenMemoryAlreadyExited — the normal exit path
//	    (memory booked first) is never double-booked.

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

const dbSyncStrategyID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
const dbSyncSymbol = "DBSYNCSYM"

func seedExternallyExitedRow(t *testing.T, exitPrice float64) func() {
	t.Helper()
	db := openPersistTestDB(t)
	_, _ = db.Exec(`DELETE FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		dbSyncStrategyID, dbSyncSymbol)
	_, err := db.Exec(`
		INSERT INTO manthan_positions
			(strategy_id, user_id, symbol, entry_price, quantity, invested_amt,
			 status, exit_reason, exit_price, exit_time, entry_time, updated_at)
		VALUES ($1, 'UTEST', $2, 100, 10, 1000,
		        'EXITED', 'ADMIN_GHOST_CLEANUP', NULLIF($3,0), now(), now() - interval '1 day', now())`,
		dbSyncStrategyID, dbSyncSymbol, exitPrice)
	if err != nil {
		t.Fatalf("seed exited row: %v", err)
	}
	return func() {
		_, _ = db.Exec(`DELETE FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
			dbSyncStrategyID, dbSyncSymbol)
		db.Close()
	}
}

func TestSyncExitedFromDB_ReleasesExternallyExitedSlot(t *testing.T) {
	cleanup := seedExternallyExitedRow(t, 0) // no exit price — ghost-style
	defer cleanup()
	db := openPersistTestDB(t)
	defer db.Close()

	pm := NewPortfolioManager(zap.NewNop())
	st := types.UserStrategy{StrategyID: dbSyncStrategyID, UserID: "UTEST", TotalCapital: 100000, MaxPositions: 4, StopLossPct: 20}
	p := pm.GetOrCreate(st)
	pos := &types.Position{Symbol: dbSyncSymbol, EntryPrice: 100, Quantity: 10, InvestedAmt: 1000,
		HighSinceEntry: 100, State: types.StateActive, Active: true}
	p.Mu.Lock()
	p.Positions[dbSyncSymbol] = pos
	capBefore := p.CurrentCapital
	p.Mu.Unlock()

	synced, err := pm.SyncExitedFromDB(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if synced != 1 {
		t.Fatalf("expected 1 synced, got %d", synced)
	}
	p.Mu.Lock()
	state, capAfter := p.Positions[dbSyncSymbol].State, p.CurrentCapital
	p.Mu.Unlock()
	if state != types.StateExited {
		t.Fatalf("slot not released: state=%s", state)
	}
	// exit_price was NULL → booked at entry price → zero PnL → capital
	// undistorted. This is the ghost-heal invariant.
	if capAfter != capBefore {
		t.Fatalf("capital distorted by evidence-less close: %v → %v", capBefore, capAfter)
	}

	// Idempotency: second pass finds State=Exited and books nothing.
	synced2, err := pm.SyncExitedFromDB(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if synced2 != 0 {
		t.Fatalf("second pass must be a no-op, synced=%d", synced2)
	}
}

func TestSyncExitedFromDB_NoOpWhenMemoryAlreadyExited(t *testing.T) {
	cleanup := seedExternallyExitedRow(t, 120)
	defer cleanup()
	db := openPersistTestDB(t)
	defer db.Close()

	pm := NewPortfolioManager(zap.NewNop())
	st := types.UserStrategy{StrategyID: dbSyncStrategyID, UserID: "UTEST", TotalCapital: 100000, MaxPositions: 4, StopLossPct: 20}
	p := pm.GetOrCreate(st)
	p.Mu.Lock()
	p.Positions[dbSyncSymbol] = &types.Position{Symbol: dbSyncSymbol, EntryPrice: 100, Quantity: 10,
		State: types.StateExited} // normal path already booked it
	capBefore := p.CurrentCapital
	p.Mu.Unlock()

	synced, err := pm.SyncExitedFromDB(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if synced != 0 {
		t.Fatalf("already-exited memory must not re-book, synced=%d", synced)
	}
	p.Mu.Lock()
	capAfter := p.CurrentCapital
	p.Mu.Unlock()
	if capAfter != capBefore {
		t.Fatalf("capital moved on a no-op: %v → %v", capBefore, capAfter)
	}
}
