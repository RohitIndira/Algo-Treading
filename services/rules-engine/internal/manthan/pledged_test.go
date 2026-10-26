package manthan

// PLEDGED lifecycle state (2026-10-26):
//
//	TestPledged_DoesNotOccupySlot           — pure: a PLEDGED position frees its
//	    slot/cap but still blocks a re-entry of the same symbol.
//	TestSyncExitedFromDB_IgnoresPledgedRow  — DB: a PLEDGED row is a LIVE row;
//	    an older EXITED row for the same pair must not exit the position from
//	    memory, and the PLEDGED row itself is never booked as an exit.
//	TestSyncBrokerStateFromDB_Flips         — DB: ACTIVE→PLEDGED (slot released,
//	    trailing off) and PLEDGED→ACTIVE (protection resumed) follow the row.
//	TestPersistExit_ClosesPledgedRow        — DB: a confirmed exit of a pledged
//	    lot (client unpledged and sold by hand) still lands on the row.
//
// DB tests run against the local trading_db and skip when unreachable.

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

const pledgedStrategyID = "aaaaaaaa-1111-2222-3333-cccccccccccc"
const pledgedSymbol = "PLEDGESYM"

func TestPledged_DoesNotOccupySlot(t *testing.T) {
	positions := map[string]*types.Position{
		"HELD":    {Symbol: "HELD", State: types.StateActive, Active: true},
		"PLEDGED": {Symbol: "PLEDGED", State: types.StatePledged, Active: false},
		"PENDING": {Symbol: "PENDING", State: types.StatePendingEntry},
		"GONE":    {Symbol: "GONE", State: types.StateExited},
	}
	if positions["PLEDGED"].Occupies() {
		t.Fatal("a PLEDGED position must not occupy a slot")
	}
	if got := countActive(positions); got != 2 {
		t.Fatalf("occupied slots = %d, want 2 (HELD + PENDING)", got)
	}
	// The symbol is still held by the client: the allocator must not buy it
	// again while it is pledged.
	if reason, held := holdingReason(positions, "PLEDGED"); !held || reason != "already holding (PLEDGED)" {
		t.Fatalf("re-entry guard: held=%v reason=%q", held, reason)
	}
	if _, held := holdingReason(positions, "GONE"); held {
		t.Fatal("an exited symbol is not held")
	}
}

func seedPledgedRows(t *testing.T) func() {
	t.Helper()
	db := openPersistTestDB(t)
	clean := func() {
		_, _ = db.Exec(`DELETE FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`, pledgedStrategyID, pledgedSymbol)
	}
	clean()
	// An old EXITED round-trip (a week ago) …
	if _, err := db.Exec(`
		INSERT INTO manthan_positions
			(strategy_id, user_id, symbol, industry, mcap_bucket, index_name, entry_price, quantity, invested_amt,
			 status, exit_reason, exit_price, exit_time, entry_time, updated_at)
		VALUES ($1, 'UTEST', $2, 'Cables', 'SMALL', 'NIFTY SMALLCAP 250', 100, 10, 1000,
		        'EXITED', 'TSL_HIT', 90, now() - interval '7 days', now() - interval '9 days', now() - interval '1 hour')`,
		pledgedStrategyID, pledgedSymbol); err != nil {
		t.Fatalf("seed exited: %v", err)
	}
	// … and the CURRENT lot, held but pledged.
	if _, err := db.Exec(`
		INSERT INTO manthan_positions
			(strategy_id, user_id, symbol, industry, mcap_bucket, index_name, entry_price, quantity, invested_amt,
			 status, protective_last_status, protective_last_reason, entry_time, updated_at)
		VALUES ($1, 'UTEST', $2, 'Cables', 'SMALL', 'NIFTY SMALLCAP 250', 100, 10, 1000,
		        'PLEDGED', 'PLEDGED', 'Pledged: 10 of 10 pledged, 0 free — unpledge to let the strategy manage it',
		        now() - interval '2 days', now())`,
		pledgedStrategyID, pledgedSymbol); err != nil {
		t.Fatalf("seed pledged: %v", err)
	}
	return func() { clean(); db.Close() }
}

func newPledgedPortfolio(state types.PositionState, active bool) (*PortfolioManager, *types.Portfolio) {
	pm := NewPortfolioManager(zap.NewNop())
	st := types.UserStrategy{StrategyID: pledgedStrategyID, UserID: "UTEST", TotalCapital: 100000, MaxPositions: 4, StopLossPct: 20}
	p := pm.GetOrCreate(st)
	p.Mu.Lock()
	p.Positions[pledgedSymbol] = &types.Position{Symbol: pledgedSymbol, EntryPrice: 100, Quantity: 10, InvestedAmt: 1000,
		HighSinceEntry: 100, CurrentSL: 80, State: state, Active: active}
	p.Mu.Unlock()
	return pm, p
}

func TestSyncExitedFromDB_IgnoresPledgedRow(t *testing.T) {
	cleanup := seedPledgedRows(t)
	defer cleanup()
	db := openPersistTestDB(t)
	defer db.Close()

	pm, p := newPledgedPortfolio(types.StatePledged, false)
	synced, err := pm.SyncExitedFromDB(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if synced != 0 {
		t.Fatalf("a PLEDGED row is live — the stale EXITED row must not exit it (synced=%d)", synced)
	}
	p.Mu.Lock()
	state := p.Positions[pledgedSymbol].State
	p.Mu.Unlock()
	if state != types.StatePledged {
		t.Fatalf("state changed to %s", state)
	}
}

func TestSyncBrokerStateFromDB_Flips(t *testing.T) {
	cleanup := seedPledgedRows(t)
	defer cleanup()
	db := openPersistTestDB(t)
	defer db.Close()

	// Memory still ACTIVE (reconciler just wrote PLEDGED) → flips to PLEDGED.
	pm, p := newPledgedPortfolio(types.StateActive, true)
	pl, un, err := pm.SyncBrokerStateFromDB(context.Background(), db, nil)
	if err != nil || pl != 1 || un != 0 {
		t.Fatalf("pledge flip: pl=%d un=%d err=%v", pl, un, err)
	}
	p.Mu.Lock()
	pos := p.Positions[pledgedSymbol]
	state, active, occupies := pos.State, pos.Active, pos.Occupies()
	p.Mu.Unlock()
	if state != types.StatePledged || active || occupies {
		t.Fatalf("after pledge flip: state=%s active=%v occupies=%v", state, active, occupies)
	}
	// Idempotent.
	if pl, un, _ := pm.SyncBrokerStateFromDB(context.Background(), db, nil); pl+un != 0 {
		t.Fatalf("second pass flipped again: pl=%d un=%d", pl, un)
	}

	// Reconciler flips the row back to ACTIVE (unpledged) → memory resumes.
	if _, err := db.Exec(`UPDATE manthan_positions SET status='ACTIVE', protective_last_status='UNPLEDGED',
		protective_last_reason='Unpledged – protection resumed', updated_at=now()
		WHERE strategy_id=$1 AND symbol=$2 AND status='PLEDGED'`, pledgedStrategyID, pledgedSymbol); err != nil {
		t.Fatalf("unpledge row: %v", err)
	}
	pl, un, err = pm.SyncBrokerStateFromDB(context.Background(), db, nil)
	if err != nil || pl != 0 || un != 1 {
		t.Fatalf("unpledge flip: pl=%d un=%d err=%v", pl, un, err)
	}
	p.Mu.Lock()
	pos = p.Positions[pledgedSymbol]
	state, active, occupies = pos.State, pos.Active, pos.Occupies()
	p.Mu.Unlock()
	if state != types.StateActive || !active || !occupies {
		t.Fatalf("after unpledge flip: state=%s active=%v occupies=%v", state, active, occupies)
	}
}

func TestPersistExit_ClosesPledgedRow(t *testing.T) {
	cleanup := seedPledgedRows(t)
	defer cleanup()
	db := openPersistTestDB(t)
	defer db.Close()

	pub := &ManthanPublisher{db: db, logger: zap.NewNop()}
	if err := pub.PersistExit(context.Background(), pledgedStrategyID, pledgedSymbol, 95, -50, "MANUAL_EXIT"); err != nil {
		t.Fatalf("persist exit: %v", err)
	}
	var status, reason string
	var updated time.Time
	if err := db.QueryRow(`SELECT status, COALESCE(exit_reason,''), updated_at FROM manthan_positions
		WHERE strategy_id=$1 AND symbol=$2 ORDER BY entry_time DESC LIMIT 1`, pledgedStrategyID, pledgedSymbol).Scan(&status, &reason, &updated); err != nil {
		t.Fatalf("read: %v", err)
	}
	if status != "EXITED" || reason != "MANUAL_EXIT" {
		t.Fatalf("pledged row not closed by the confirmed exit: %s %s", status, reason)
	}
}

// A late fill confirmation must not re-activate a PLEDGED or EXIT_PENDING lot.
func TestProcessFillEvent_IgnoresPledgedAndExitPending(t *testing.T) {
	for _, st := range []types.PositionState{types.StatePledged, types.StateExitPending} {
		pm, p := newPledgedPortfolio(st, st == types.StateExitPending)
		pm.ProcessFillEvent(pledgedStrategyID, pledgedSymbol, 123, 10, 10, true, NewTrailingSLManager(zap.NewNop()), 20)
		p.Mu.Lock()
		pos := p.Positions[pledgedSymbol]
		p.Mu.Unlock()
		if pos.State != st || pos.EntryPrice != 100 {
			t.Fatalf("%s: fill re-activated the lot: state=%s entry=%v", st, pos.State, pos.EntryPrice)
		}
	}
}

// Newest row per (strategy, symbol) wins: an older PLEDGED row must not
// re-pledge a lot whose live row is ACTIVE.
func TestSyncBrokerStateFromDB_NewestRowWins(t *testing.T) {
	cleanup := seedPledgedRows(t)
	defer cleanup()
	db := openPersistTestDB(t)
	defer db.Close()
	// Add a NEWER ACTIVE row for the same pair (re-entry after the pledge).
	if _, err := db.Exec(`
		INSERT INTO manthan_positions
			(strategy_id, user_id, symbol, industry, mcap_bucket, index_name, entry_price, quantity, invested_amt,
			 status, entry_time, updated_at)
		VALUES ($1, 'UTEST', $2, 'Cables', 'SMALL', 'NIFTY SMALLCAP 250', 110, 5, 550,
		        'ACTIVE', now() - interval '1 hour', now() + interval '1 second')`,
		pledgedStrategyID, pledgedSymbol); err != nil {
		t.Fatalf("seed newer active: %v", err)
	}
	pm, p := newPledgedPortfolio(types.StateActive, true)
	pl, un, err := pm.SyncBrokerStateFromDB(context.Background(), db, nil)
	if err != nil || pl != 0 || un != 0 {
		t.Fatalf("older PLEDGED row must not win: pl=%d un=%d err=%v", pl, un, err)
	}
	p.Mu.Lock()
	state := p.Positions[pledgedSymbol].State
	p.Mu.Unlock()
	if state != types.StateActive {
		t.Fatalf("state flipped to %s", state)
	}
}
