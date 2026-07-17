package store

// Tests the three read methods against a real local positions_db. Each
// test TRUNCATES positions between runs so ordering + counts are
// deterministic. Skips (not fails) if positions_db isn't reachable —
// convenient for laptop dev without infra.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=positions_db sslmode=disable")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("positions_db not reachable — skipping (%v)", err)
	}
	// TRUNCATE via CASCADE to also drop dependent position_events rows.
	if _, err := db.Exec(`TRUNCATE positions, position_events CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return db
}

// insertLot writes one row directly. Bypasses the positions svc state
// machine — tests here care about SELECTs, not the write path.
//
// Manthan constraint enforcement (strategy_id + signal_id NOT NULL for
// MANTHAN, must be NULL for USER_MANUAL) is done in Go before the INSERT
// so the SQL stays fully typed for lib/pq's parameter binding.
func insertLot(t *testing.T, db *sql.DB, row lotRow) uuid.UUID {
	t.Helper()
	id := uuid.New()

	var strategyArg, signalArg interface{}
	if row.origin == "MANTHAN" {
		strategyArg = row.strategyID
		signalArg = row.signalID
	}
	// else leave nil → INSERT writes NULL

	_, err := db.Exec(`
		INSERT INTO positions (
		  position_id, origin, user_id, strategy_id, signal_id,
		  symbol, exchange, status,
		  entry_price, entry_time, quantity, invested_amount,
		  exit_price, exit_time, exit_reason, realized_pnl,
		  entry_broker_order_id, current_sl, high_since_entry
		) VALUES (
		  $1, $2, $3, $4::uuid, $5::uuid,
		  $6, $7, $8,
		  $9, $10, $11, $12,
		  $13, $14, $15, $16,
		  $17, $18, $19)`,
		id, row.origin, row.userID,
		strategyArg, signalArg,
		row.symbol, row.exchange, row.status,
		row.entryPrice, row.entryTime, row.qty, row.invested,
		nullFloat(row.exitPrice), nullTime(row.exitTime), nullStr(row.exitReason), nullFloat(row.realized),
		row.brokerOrderID, nullFloat(row.currentSL), nullFloat(row.highSince),
	)
	if err != nil {
		t.Fatalf("insertLot: %v", err)
	}
	return id
}

type lotRow struct {
	origin, userID, strategyID, signalID, symbol, exchange, status string
	entryPrice                                                     float64
	entryTime                                                      time.Time
	qty                                                            int
	invested                                                       float64
	exitPrice                                                      float64
	exitTime                                                       time.Time
	exitReason                                                     string
	realized                                                       float64
	brokerOrderID                                                  string
	currentSL, highSince                                           float64
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullFloat(v float64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

// mkStore builds one for tests.
func mkStore(t *testing.T, db *sql.DB) *Store {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	return New(db, logger)
}

// stableUUID keeps signal/strategy UUIDs consistent within a test so
// the SIGNATURE of the SUMs stays predictable for the reader.
func stableUUID(seed int) string {
	return fmt.Sprintf("%08d-0000-0000-0000-000000000000", seed)
}

// -----------------------------------------------------------------------------
// SummaryFor
// -----------------------------------------------------------------------------

func TestSummary_ZeroRows(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	sum, err := s.SummaryFor(context.Background(), "S4450")
	if err != nil {
		t.Fatalf("SummaryFor: %v", err)
	}
	if sum.TotalInvested != 0 || sum.ActiveLotCount != 0 || sum.ClosedLotCount != 0 {
		t.Errorf("empty user must produce all-zero summary, got %+v", sum)
	}
}

func TestSummary_MixedActiveAndExited(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	yesterday := time.Now().Add(-30 * time.Hour)
	todayNoon := time.Now()

	// User has (after 2026-07-16 Manthan-only scope change, USER_MANUAL
	// lots are EXCLUDED from every summary number below — they still
	// exist in positions_db but the portfolio API no longer counts them):
	//
	//   ACTIVE MANTHAN SBI    20 @ ₹500 = ₹10000 invested  ← counted
	//   ACTIVE USER   IDEA   100 @ ₹10  = ₹1000 invested   ← excluded
	//   EXITED MANTHAN ITC   50 @ ₹200 → 220 → +₹1000 (yesterday) ← counted lifetime, not today
	//   EXITED USER   TCS    10 @ 3000 → 3200 → +₹2000 (today)  ← excluded
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(11),
		symbol: "SBI", exchange: "NSE", status: "ACTIVE",
		entryPrice: 500, entryTime: yesterday, qty: 20, invested: 10000,
		brokerOrderID: "B1",
	})
	insertLot(t, db, lotRow{
		origin: "USER_MANUAL", userID: "S4450",
		symbol: "IDEA", exchange: "NSE", status: "ACTIVE",
		entryPrice: 10, entryTime: yesterday, qty: 100, invested: 1000,
		brokerOrderID: "B2",
	})
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(12),
		symbol: "ITC", exchange: "NSE", status: "EXITED",
		entryPrice: 200, entryTime: yesterday.Add(-24 * time.Hour), qty: 50, invested: 10000,
		exitPrice: 220, exitTime: yesterday, exitReason: "SL_TRIGGER", realized: 1000,
		brokerOrderID: "B3",
	})
	insertLot(t, db, lotRow{
		origin: "USER_MANUAL", userID: "S4450",
		symbol: "TCS", exchange: "NSE", status: "EXITED",
		entryPrice: 3000, entryTime: yesterday, qty: 10, invested: 30000,
		exitPrice: 3200, exitTime: todayNoon, exitReason: "MANUAL_EXIT", realized: 2000,
		brokerOrderID: "B4",
	})

	sum, err := s.SummaryFor(context.Background(), "S4450")
	if err != nil {
		t.Fatalf("SummaryFor: %v", err)
	}

	// Expected numbers reflect Manthan-only scope: SBI ACTIVE + ITC EXITED.
	if got, want := sum.TotalInvested, 10000.0; got != want {
		t.Errorf("total_invested: got %v, want %v (SBI ACTIVE Manthan only)", got, want)
	}
	if got, want := sum.TotalRealizedPnLLifetime, 1000.0; got != want {
		t.Errorf("total_realized_pnl_lifetime: got %v, want %v (ITC Manthan exit only; TCS user-manual excluded)", got, want)
	}
	// Today's realized: TCS was today but is USER_MANUAL → excluded. ITC was
	// yesterday. Manthan-only scope + today filter → 0.
	if got, want := sum.TodayRealizedPnL, 0.0; got != want {
		t.Errorf("today_realized_pnl: got %v, want %v (Manthan-only + today: no matches)", got, want)
	}
	if sum.ActiveLotCount != 1 || sum.ClosedLotCount != 1 {
		t.Errorf("counts: got active=%d closed=%d, want 1/1 (Manthan-only)", sum.ActiveLotCount, sum.ClosedLotCount)
	}
	if got, want := sum.ManthanInvested, 10000.0; got != want {
		t.Errorf("manthan_invested: got %v, want %v", got, want)
	}
	// user_manual_invested stays wire-compatible but always 0 under Manthan scope.
	if got, want := sum.UserManualInvested, 0.0; got != want {
		t.Errorf("user_manual_invested: got %v, want %v (Manthan-only scope forces 0)", got, want)
	}
}

func TestSummary_OtherUsersInvisible(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(11),
		symbol: "SBI", exchange: "NSE", status: "ACTIVE",
		entryPrice: 500, entryTime: time.Now(), qty: 10, invested: 5000,
		brokerOrderID: "B1",
	})
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "OTHER_USER",
		strategyID: stableUUID(1), signalID: stableUUID(21),
		symbol: "SBI", exchange: "NSE", status: "ACTIVE",
		entryPrice: 500, entryTime: time.Now(), qty: 100, invested: 50000,
		brokerOrderID: "B2",
	})

	sum, err := s.SummaryFor(context.Background(), "S4450")
	if err != nil {
		t.Fatalf("SummaryFor: %v", err)
	}
	if sum.TotalInvested != 5000 {
		t.Errorf("cross-user leak: got %v, want 5000 (only S4450's row)", sum.TotalInvested)
	}
}

// -----------------------------------------------------------------------------
// ActiveLotsFor
// -----------------------------------------------------------------------------

func TestActiveLots_OrderByEntryTime(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	// Insert in reverse chronological order to prove the ORDER BY matters.
	// Both MANTHAN — after the 2026-07-16 Manthan-only scope change,
	// USER_MANUAL lots are excluded entirely (see TestActiveLots_SkipsUserManual).
	t2 := time.Now().Add(-1 * time.Hour)
	t1 := time.Now().Add(-3 * time.Hour)
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(12),
		symbol: "LATER", exchange: "NSE", status: "ACTIVE",
		entryPrice: 20, entryTime: t2, qty: 5, invested: 100, brokerOrderID: "B2",
	})
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(11),
		symbol: "EARLIER", exchange: "NSE", status: "ACTIVE",
		entryPrice: 100, entryTime: t1, qty: 3, invested: 300,
		currentSL: 90, highSince: 105,
		brokerOrderID: "B1",
	})

	got, err := s.ActiveLotsFor(context.Background(), "S4450")
	if err != nil {
		t.Fatalf("ActiveLotsFor: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].Symbol != "EARLIER" {
		t.Errorf("order: got %s first, want EARLIER (entry_time ASC)", got[0].Symbol)
	}
	if got[0].CurrentSL != 90 || got[0].HighSinceEntry != 105 {
		t.Errorf("SL/high fields: got sl=%v high=%v, want 90/105", got[0].CurrentSL, got[0].HighSinceEntry)
	}
	if got[1].Symbol != "LATER" {
		t.Errorf("order: got %s second, want LATER", got[1].Symbol)
	}
}

// TestActiveLots_SkipsUserManual — after the 2026-07-16 Manthan-only
// scope change, USER_MANUAL lots must be excluded from the portfolio
// API (LTP feed doesn't cover the arbitrary user-manual universe, and
// portfolio numbers are meant to reflect Manthan strategy P&L only).
func TestActiveLots_SkipsUserManual(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(11),
		symbol: "MANTHAN_LOT", exchange: "NSE", status: "ACTIVE",
		entryPrice: 100, entryTime: time.Now(), qty: 10, invested: 1000, brokerOrderID: "B1",
	})
	insertLot(t, db, lotRow{
		origin: "USER_MANUAL", userID: "S4450",
		symbol: "MANUAL_LOT", exchange: "NSE", status: "ACTIVE",
		entryPrice: 100, entryTime: time.Now(), qty: 10, invested: 1000, brokerOrderID: "B2",
	})

	got, err := s.ActiveLotsFor(context.Background(), "S4450")
	if err != nil {
		t.Fatalf("ActiveLotsFor: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 row (MANTHAN only), got %d", len(got))
	}
	if got[0].Symbol != "MANTHAN_LOT" {
		t.Errorf("wrong lot returned: got %s, want MANTHAN_LOT", got[0].Symbol)
	}
	if got[0].Origin != "MANTHAN" {
		t.Errorf("wrong origin: got %s, want MANTHAN", got[0].Origin)
	}
}

func TestActiveLots_SkipsExited(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(11),
		symbol: "ACTIVE_ONE", exchange: "NSE", status: "ACTIVE",
		entryPrice: 100, entryTime: time.Now(), qty: 10, invested: 1000, brokerOrderID: "B1",
	})
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(12),
		symbol: "EXITED_ONE", exchange: "NSE", status: "EXITED",
		entryPrice: 100, entryTime: time.Now(), qty: 10, invested: 1000,
		exitPrice: 90, exitTime: time.Now(), exitReason: "SL_TRIGGER", realized: -100,
		brokerOrderID: "B2",
	})

	got, err := s.ActiveLotsFor(context.Background(), "S4450")
	if err != nil {
		t.Fatalf("ActiveLotsFor: %v", err)
	}
	if len(got) != 1 || got[0].Symbol != "ACTIVE_ONE" {
		t.Errorf("expected [ACTIVE_ONE], got %+v", got)
	}
}

// -----------------------------------------------------------------------------
// ClosedLotsPaged
// -----------------------------------------------------------------------------

func TestClosedLotsPaged_ExitTimeDescOrder(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	// Three exits at increasing exit_time — should come back newest first.
	t0 := time.Now().Add(-3 * time.Hour)
	t1 := time.Now().Add(-2 * time.Hour)
	t2 := time.Now().Add(-1 * time.Hour)

	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(11),
		symbol: "OLDEST", exchange: "NSE", status: "EXITED",
		entryPrice: 100, entryTime: t0.Add(-time.Hour), qty: 10, invested: 1000,
		exitPrice: 110, exitTime: t0, exitReason: "SL_TRIGGER", realized: 100, brokerOrderID: "B1",
	})
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(12),
		symbol: "MID", exchange: "NSE", status: "EXITED",
		entryPrice: 100, entryTime: t1.Add(-time.Hour), qty: 5, invested: 500,
		exitPrice: 95, exitTime: t1, exitReason: "SL_TRIGGER", realized: -25, brokerOrderID: "B2",
	})
	insertLot(t, db, lotRow{
		// MANTHAN (was USER_MANUAL before the 2026-07-16 scope change).
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(13),
		symbol: "NEWEST", exchange: "NSE", status: "EXITED",
		entryPrice: 100, entryTime: t2.Add(-time.Hour), qty: 3, invested: 300,
		exitPrice: 120, exitTime: t2, exitReason: "STRATEGY_EXIT", realized: 60, brokerOrderID: "B3",
	})

	rows, total, err := s.ClosedLotsPaged(context.Background(), "S4450", 1, 50)
	if err != nil {
		t.Fatalf("ClosedLotsPaged: %v", err)
	}
	if total != 3 || len(rows) != 3 {
		t.Fatalf("total=%d, rows=%d, want 3/3", total, len(rows))
	}
	if rows[0].Symbol != "NEWEST" || rows[1].Symbol != "MID" || rows[2].Symbol != "OLDEST" {
		t.Errorf("order: got %s, %s, %s, want NEWEST, MID, OLDEST",
			rows[0].Symbol, rows[1].Symbol, rows[2].Symbol)
	}
	if rows[0].RealizedPnL != 60 || rows[0].ExitReason != "STRATEGY_EXIT" {
		t.Errorf("first row projections: got realized=%v reason=%q, want 60/STRATEGY_EXIT",
			rows[0].RealizedPnL, rows[0].ExitReason)
	}
}

func TestClosedLotsPaged_Pagination(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	// 5 exits, ordered so page 1 gets rows 5,4,3 and page 2 gets 2,1.
	for i := 1; i <= 5; i++ {
		exitTime := time.Now().Add(-time.Duration(6-i) * time.Hour) // i=5 → newest
		insertLot(t, db, lotRow{
			origin: "MANTHAN", userID: "S4450",
			strategyID: stableUUID(1), signalID: stableUUID(10 + i),
			symbol: fmt.Sprintf("SYM%d", i), exchange: "NSE", status: "EXITED",
			entryPrice: 100, entryTime: exitTime.Add(-time.Hour), qty: 1, invested: 100,
			exitPrice: 110, exitTime: exitTime, exitReason: "SL_TRIGGER", realized: 10,
			brokerOrderID: fmt.Sprintf("B%d", i),
		})
	}

	// Page 1, size 3
	rows, total, err := s.ClosedLotsPaged(context.Background(), "S4450", 1, 3)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if total != 5 || len(rows) != 3 {
		t.Fatalf("page 1: total=%d rows=%d, want 5/3", total, len(rows))
	}
	if rows[0].Symbol != "SYM5" || rows[2].Symbol != "SYM3" {
		t.Errorf("page 1 order: got %s...%s, want SYM5...SYM3",
			rows[0].Symbol, rows[2].Symbol)
	}

	// Page 2, size 3 → should get 2 rows (SYM2, SYM1)
	rows2, total2, err := s.ClosedLotsPaged(context.Background(), "S4450", 2, 3)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if total2 != 5 || len(rows2) != 2 {
		t.Fatalf("page 2: total=%d rows=%d, want 5/2", total2, len(rows2))
	}
	if rows2[0].Symbol != "SYM2" || rows2[1].Symbol != "SYM1" {
		t.Errorf("page 2 order: got %s, %s, want SYM2, SYM1", rows2[0].Symbol, rows2[1].Symbol)
	}
}

func TestClosedLotsPaged_ClampsBadInput(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	s := mkStore(t, db)

	// One EXITED row (MANTHAN — post 2026-07-16 scope change portfolio API
	// is Manthan-only; USER_MANUAL wouldn't be returned).
	insertLot(t, db, lotRow{
		origin: "MANTHAN", userID: "S4450",
		strategyID: stableUUID(1), signalID: stableUUID(11),
		symbol: "SYM", exchange: "NSE", status: "EXITED",
		entryPrice: 100, entryTime: time.Now().Add(-time.Hour), qty: 1, invested: 100,
		exitPrice: 110, exitTime: time.Now(), exitReason: "SL_TRIGGER", realized: 10,
		brokerOrderID: "B1",
	})

	// page=0 → should coerce to 1
	rows, _, err := s.ClosedLotsPaged(context.Background(), "S4450", 0, 999)
	if err != nil {
		t.Fatalf("page=0: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("page=0 with 999 size (clamped to 200): got %d rows, want 1", len(rows))
	}
	// pageSize=-5 → falls back to 50
	rows, _, err = s.ClosedLotsPaged(context.Background(), "S4450", 1, -5)
	if err != nil {
		t.Fatalf("pageSize=-5: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("pageSize=-5 (default 50): got %d rows, want 1", len(rows))
	}
}
