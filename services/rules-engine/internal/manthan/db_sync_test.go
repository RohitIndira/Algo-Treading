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

// 2026-10-09 regression: a stale EXPIRED (never-filled) row for a symbol the
// strategy holds again must NOT exit the live position. Once the live row is
// gone, the same stale row releases the slot as before.
func TestSyncExitedFromDB_IgnoresStaleRowWhileLiveRowExists(t *testing.T) {
	db := openPersistTestDB(t)
	defer db.Close()
	cleanup := func() {
		_, _ = db.Exec(`DELETE FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`, dbSyncStrategyID, dbSyncSymbol)
	}
	cleanup()
	defer cleanup()
	// Thursday's dispatch that never filled (EXPIRED, updated "now").
	if _, err := db.Exec(`
		INSERT INTO manthan_positions
			(strategy_id, user_id, symbol, entry_price, quantity, invested_amt,
			 status, exit_reason, exit_price, entry_time, updated_at)
		VALUES ($1, 'UTEST', $2, 151.77, 131, 19881,
		        'EXPIRED', 'ENTRY_NEVER_FILLED', NULL, now() - interval '2 days', now())`,
		dbSyncStrategyID, dbSyncSymbol); err != nil {
		t.Fatalf("seed expired row: %v", err)
	}
	// Friday's fill of the same symbol — the live row.
	if _, err := db.Exec(`
		INSERT INTO manthan_positions
			(strategy_id, user_id, symbol, entry_price, quantity, invested_amt, industry, mcap_bucket, index_name,
			 status, entry_time, updated_at)
		VALUES ($1, 'UTEST', $2, 145.62, 134, 19513, 'Auto', 'SMALL', 'NIFTY',
		        'ACTIVE', now() - interval '1 hour', now())`,
		dbSyncStrategyID, dbSyncSymbol); err != nil {
		t.Fatalf("seed live row: %v", err)
	}

	pm := NewPortfolioManager(zap.NewNop())
	st := types.UserStrategy{StrategyID: dbSyncStrategyID, UserID: "UTEST", TotalCapital: 500000, MaxPositions: 25, StopLossPct: 20}
	p := pm.GetOrCreate(st)
	p.Mu.Lock()
	p.Positions[dbSyncSymbol] = &types.Position{Symbol: dbSyncSymbol, EntryPrice: 145.62, Quantity: 134, InvestedAmt: 19513,
		HighSinceEntry: 145.62, State: types.StateActive, Active: true}
	p.Mu.Unlock()

	synced, err := pm.SyncExitedFromDB(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	p.Mu.Lock()
	state := p.Positions[dbSyncSymbol].State
	p.Mu.Unlock()
	if synced != 0 || state != types.StateActive {
		t.Fatalf("stale EXPIRED row exited a live position: synced=%d state=%s", synced, state)
	}

	// The live row disappears (e.g. admin ghost heal set it EXITED elsewhere,
	// or it was deleted): the stale row may now release the slot.
	if _, err := db.Exec(`DELETE FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2 AND status='ACTIVE'`,
		dbSyncStrategyID, dbSyncSymbol); err != nil {
		t.Fatalf("drop live row: %v", err)
	}
	synced, err = pm.SyncExitedFromDB(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("sync 2: %v", err)
	}
	p.Mu.Lock()
	state = p.Positions[dbSyncSymbol].State
	p.Mu.Unlock()
	if synced != 1 || state != types.StateExited {
		t.Fatalf("without a live row the stale row must release the slot: synced=%d state=%s", synced, state)
	}
}
