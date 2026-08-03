package manthan

// FIX F restart-recovery tests (real local trading_db; skips if unreachable).
//
//   TestPersist_OpenIsIdempotent        — same signal_id twice = one row
//   TestPersist_TrailPersists            — ratchet writes current_sl/high/last_trail
//   TestRestart_ResumesTrailAtExactLevel — the core proof: persist → ratchet →
//                                          rehydrate resumes at the EXACT level
//                                          (no reset-to-entry, no regression)
//   TestPersist_ExitMarksExited          — exit → rehydrate restores nothing

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

const persistStrategyID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
const persistSignalID = "ffffffff-0000-1111-2222-333333333333"
const persistSymbol = "PERSISTSYM"

func openPersistTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=trading_db sslmode=disable")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("trading_db not reachable — skipping DB tests (%v)", err)
	}
	// clean any leftovers from a prior run
	_, _ = db.Exec(`DELETE FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol)
	return db
}

func cleanPersist(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec(`DELETE FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol)
}

// testEntryOrder builds a ManthanOrder for the persist path. Entry 100, SL 80.
func testEntryOrder() ManthanOrder {
	return ManthanOrder{
		OrderID:     persistSignalID,
		UserID:      "S4450",
		StrategyID:  persistStrategyID,
		Symbol:      persistSymbol,
		ISIN:        "INE000TEST01",
		Industry:    "IT",       // chk_active_has_classification requires non-empty
		MCapBucket:  "LARGE",    // "
		IndexName:   "NIFTY50",  // "
		EntryPrice:  100.00,
		Quantity:    10,
		InvestedAmt: 1000.00,
		StopLoss:    80.00,
		EMAAllocPct: 100,
	}
}

func rowTrail(t *testing.T, db *sql.DB) (currentSL, high, lastTrail float64, status string) {
	t.Helper()
	err := db.QueryRow(`
		SELECT current_sl, high_since_entry, last_trail_level, status
		FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol).Scan(&currentSL, &high, &lastTrail, &status)
	if err != nil {
		t.Fatalf("row trail query: %v", err)
	}
	return
}

func TestPersist_OpenIsIdempotent(t *testing.T) {
	db := openPersistTestDB(t)
	defer cleanPersist(t, db)
	p := &ManthanPublisher{db: db, logger: zap.NewNop()}

	if err := p.PersistPositionOpen(context.Background(), testEntryOrder()); err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := p.PersistPositionOpen(context.Background(), testEntryOrder()); err != nil {
		t.Fatalf("open 2 (replay): %v", err)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol).Scan(&n)
	if n != 1 {
		t.Fatalf("expected exactly 1 row after replay, got %d", n)
	}
}

func TestPersist_TrailPersists(t *testing.T) {
	db := openPersistTestDB(t)
	defer cleanPersist(t, db)
	p := &ManthanPublisher{db: db, logger: zap.NewNop()}
	_ = p.PersistPositionOpen(context.Background(), testEntryOrder())

	// ratchet: high 110, sl 88, last_trail 110
	pos := types.Position{Symbol: persistSymbol, CurrentSL: 88.00, HighSinceEntry: 110.00, LastTrailLevel: 110.00}
	if err := p.PersistTrail(context.Background(), persistStrategyID, pos); err != nil {
		t.Fatalf("trail: %v", err)
	}
	sl, high, lt, status := rowTrail(t, db)
	if sl != 88.00 || high != 110.00 || lt != 110.00 || status != "ACTIVE" {
		t.Fatalf("trail not persisted: sl=%.2f high=%.2f lt=%.2f status=%s", sl, high, lt, status)
	}
}

// The core restart proof: open → ratchet twice → simulate restart by building a
// FRESH PortfolioManager and rehydrating from the DB. The in-memory position
// must resume at the EXACT last-persisted trail (not reset to entry).
func TestRestart_ResumesTrailAtExactLevel(t *testing.T) {
	db := openPersistTestDB(t)
	defer cleanPersist(t, db)
	p := &ManthanPublisher{db: db, logger: zap.NewNop()}

	_ = p.PersistPositionOpen(context.Background(), testEntryOrder())
	// two ratchets: 80 -> 83.20 -> 88.00 (high 104 -> 110)
	_ = p.PersistTrail(context.Background(), persistStrategyID,
		types.Position{Symbol: persistSymbol, CurrentSL: 83.20, HighSinceEntry: 104.00, LastTrailLevel: 104.00})
	_ = p.PersistTrail(context.Background(), persistStrategyID,
		types.Position{Symbol: persistSymbol, CurrentSL: 88.00, HighSinceEntry: 110.00, LastTrailLevel: 110.00})

	// --- simulate restart: fresh PortfolioManager, rehydrate from DB ---
	pm := NewPortfolioManager(zap.NewNop())
	strategy := &types.UserStrategy{
		StrategyID: persistStrategyID, UserID: "S4450",
		TradingMode: "PAPER", StopLossPct: 20, TrailingSLPct: 2,
	}
	strategyByID := func(id string) *types.UserStrategy {
		if id == persistStrategyID {
			return strategy
		}
		return nil
	}
	// nil rdb + nil aliveChecker: live-position restore path only.
	restored, _, _, err := pm.RehydrateActivePositions(context.Background(), db, nil, nil, strategyByID)
	if err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	if restored != 1 {
		t.Fatalf("expected 1 restored position, got %d", restored)
	}

	// The resumed in-memory trail must EXACTLY match the last persisted ratchet.
	var got *types.Position
	for _, port := range pm.AllPortfolios() {
		port.Mu.RLock()
		if pp, ok := port.Positions[persistSymbol]; ok {
			cp := *pp
			got = &cp
		}
		port.Mu.RUnlock()
	}
	if got == nil {
		t.Fatal("position not restored into memory")
	}
	if got.CurrentSL != 88.00 {
		t.Fatalf("SL must resume at 88.00 (last ratchet), got %.2f — RESET-TO-ENTRY BUG", got.CurrentSL)
	}
	if got.HighSinceEntry != 110.00 || got.LastTrailLevel != 110.00 {
		t.Fatalf("high/last_trail must resume exactly: high=%.2f lt=%.2f", got.HighSinceEntry, got.LastTrailLevel)
	}
}

func TestPersist_ExitMarksExited(t *testing.T) {
	db := openPersistTestDB(t)
	defer cleanPersist(t, db)
	p := &ManthanPublisher{db: db, logger: zap.NewNop()}
	_ = p.PersistPositionOpen(context.Background(), testEntryOrder())

	if err := p.PersistExit(context.Background(), persistStrategyID, persistSymbol, 88.00, -120.00); err != nil {
		t.Fatalf("exit: %v", err)
	}
	_, _, _, status := rowTrail(t, db)
	if status != "EXITED" {
		t.Fatalf("expected EXITED, got %s", status)
	}

	// A fresh rehydrate must NOT restore an EXITED position.
	pm := NewPortfolioManager(zap.NewNop())
	strategy := &types.UserStrategy{StrategyID: persistStrategyID, UserID: "S4450", TradingMode: "PAPER", StopLossPct: 20, TrailingSLPct: 2}
	restored, _, _, err := pm.RehydrateActivePositions(context.Background(), db, nil, nil,
		func(id string) *types.UserStrategy { return strategy })
	if err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	if restored != 0 {
		t.Fatalf("EXITED position must not be restored; got restored=%d", restored)
	}
}
