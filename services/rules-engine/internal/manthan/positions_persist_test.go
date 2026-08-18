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
		Industry:    "IT",      // chk_active_has_classification requires non-empty
		MCapBucket:  "LARGE",   // "
		IndexName:   "NIFTY50", // "
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
	// New lifecycle: rows are born PENDING_ENTRY; the fill confirmation
	// promotes to ACTIVE — only then do trails apply.
	if err := p.PersistFillConfirmed(context.Background(), persistStrategyID, persistSymbol, 100.00, 10, 20); err != nil {
		t.Fatalf("confirm: %v", err)
	}

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
	_ = p.PersistFillConfirmed(context.Background(), persistStrategyID, persistSymbol, 100.00, 10, 20)
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
	_ = p.PersistFillConfirmed(context.Background(), persistStrategyID, persistSymbol, 100.00, 10, 20)

	if err := p.PersistExit(context.Background(), persistStrategyID, persistSymbol, 88.00, -120.00, "SL_TRIGGER"); err != nil {
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

// 2026-08-18 phantom fix: a dispatch is born PENDING_ENTRY and becomes ACTIVE
// only on a confirmed fill. An unconfirmed dispatch must never look held.
func TestPersist_DispatchIsPendingUntilFillConfirmed(t *testing.T) {
	db := openPersistTestDB(t)
	defer cleanPersist(t, db)
	p := &ManthanPublisher{db: db, logger: zap.NewNop()}

	_ = p.PersistPositionOpen(context.Background(), testEntryOrder())
	var status string
	if err := db.QueryRow(`SELECT status FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING_ENTRY" {
		t.Fatalf("dispatch persisted as %q, want PENDING_ENTRY (ACTIVE-at-dispatch was the phantom bug)", status)
	}

	// Fill confirmation promotes with the REAL fill price/qty.
	if err := p.PersistFillConfirmed(context.Background(), persistStrategyID, persistSymbol, 101.50, 9, 20); err != nil {
		t.Fatal(err)
	}
	var qty int
	var entry float64
	if err := db.QueryRow(`SELECT status, quantity, entry_price FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol).Scan(&status, &qty, &entry); err != nil {
		t.Fatal(err)
	}
	if status != "ACTIVE" || qty != 9 || entry != 101.50 {
		t.Fatalf("promotion wrong: status=%s qty=%d entry=%.2f (want ACTIVE/9/101.50)", status, qty, entry)
	}
	// The promoted row carries the POST-FILL stop/high/trail exactly as the
	// in-memory book (InitPosition) holds them — a restart rehydrates the
	// same 20% stop, never the provisional dispatch-time value.
	var sl, high, trail float64
	if err := db.QueryRow(`SELECT current_sl, high_since_entry, last_trail_level FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol).Scan(&sl, &high, &trail); err != nil {
		t.Fatal(err)
	}
	if sl != 81.20 || high != 101.50 || trail != 101.50 {
		t.Fatalf("post-fill state wrong: sl=%.2f high=%.2f trail=%.2f (want 81.20/101.50/101.50 = fill×0.80)", sl, high, trail)
	}
	// A late/duplicate confirmation on an already-ACTIVE row must never
	// LOWER a trailed stop (ratchet): trail to 90, re-confirm at 101.50 → 90 stays.
	if err := p.PersistTrail(context.Background(), persistStrategyID, types.Position{
		Symbol: persistSymbol, CurrentSL: 90.00, HighSinceEntry: 112.50, LastTrailLevel: 112.50,
	}); err != nil {
		t.Fatal(err)
	}
	_ = p.PersistFillConfirmed(context.Background(), persistStrategyID, persistSymbol, 101.50, 9, 20)
	if err := db.QueryRow(`SELECT current_sl, high_since_entry FROM manthan_positions WHERE strategy_id=$1 AND symbol=$2`,
		persistStrategyID, persistSymbol).Scan(&sl, &high); err != nil {
		t.Fatal(err)
	}
	if sl != 90.00 || high != 112.50 {
		t.Fatalf("late confirmation regressed the ratchet: sl=%.2f high=%.2f (want 90.00/112.50)", sl, high)
	}
}

// The allocator's initial stop MUST come from the strategy config. A literal
// 0.98 ("TEST MODE") shipped to production on this line and caused every
// phantom TSL exit of 2026-08-18. This test fails if anyone reintroduces a
// literal or lets a zero config through.
func TestAllocator_InitialSLIsStrategyStopLossPct(t *testing.T) {
	for _, tc := range []struct{ cfg, wantSL float64 }{
		{20, 80.00}, // Manthan
		{0, 80.00},  // zero config → default 20, never a 0%/2% stop
		{15, 85.00},
	} {
		got := 100.0 * (1 - effectiveStopLossPct(tc.cfg)/100)
		if got < tc.wantSL-1e-9 || got > tc.wantSL+1e-9 {
			t.Errorf("cfg=%v: initial SL on entry 100 = %.2f, want %.2f", tc.cfg, got, tc.wantSL)
		}
	}
	if effectiveStopLossPct(0.98) != 0.98 { // a real (odd) config is honoured
		t.Error("effectiveStopLossPct altered a valid config value")
	}
}
