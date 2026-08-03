package manthan

// FIX 3 — "every OPEN filled BUY position has exactly one active SL".
// Runs against local execution_db; skips if unreachable.
//
//   TestNaked_ListsOnlyUnprotectedOpenBuys — the 4-condition invariant query
//   TestNaked_ResetTerminalSLToPending      — idempotent re-drive (no dup SL)

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

const nkStrategy = "cccccccc-dddd-eeee-ffff-000011112222"

func openExecTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=execution_db sslmode=disable")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("execution_db not reachable — skipping DB tests (%v)", err)
	}
	nkClean(t, db)
	return db
}

func nkClean(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec(`DELETE FROM manthan_orders WHERE strategy_id=$1`, nkStrategy)
}

// seedOrder inserts one manthan_orders row; returns its id.
func seedOrder(t *testing.T, db *sql.DB, signalID, symbol, side, otype, status string, filledQty int, brokerID string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO manthan_orders
			(signal_id, strategy_id, user_id, symbol, order_type, order_side,
			 qty, filled_qty, avg_fill_price, status, broker_order_id)
		VALUES ($1,$2,'S4450',$3,$4,$5,$6,$6,100.00,$7,$8)
		RETURNING id`,
		signalID, nkStrategy, symbol, otype, side, filledQty, status, brokerID).Scan(&id)
	if err != nil {
		t.Fatalf("seed %s/%s: %v", symbol, status, err)
	}
	return id
}

func TestNaked_ListsOnlyUnprotectedOpenBuys(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	// A: naked — FILLED BUY, open, no SL → SHOULD appear
	seedOrder(t, db, "sig-A", "SYMA", "BUY", "LIMIT_BUY", "FILLED", 10, "NB1")
	// B: protected — FILLED BUY + active SL_PLACED → should NOT appear
	seedOrder(t, db, "sig-B", "SYMB", "BUY", "LIMIT_BUY", "FILLED", 10, "NB2")
	seedOrder(t, db, "sl-sig-B", "SYMB", "SELL", "SL_SELL", "SL_PLACED", 0, "NBSL")
	// C: closed — FILLED BUY then SELL nets to 0 → should NOT appear
	seedOrder(t, db, "sig-C", "SYMC", "BUY", "LIMIT_BUY", "FILLED", 10, "NB3")
	seedOrder(t, db, "sig-C-sell", "SYMC", "SELL", "MARKET_SELL", "FILLED", 10, "NB3S")
	// D: PAPER — filled BUY but paper broker id → should NOT appear
	seedOrder(t, db, "sig-D", "SYMD", "BUY", "LIMIT_BUY", "FILLED", 10, "PAPER-D")
	// E: deferred — FILLED BUY + SL_DEFERRED_BAND (deliberate) → should NOT appear
	seedOrder(t, db, "sig-E", "SYME", "BUY", "LIMIT_BUY", "FILLED", 10, "NB4")
	seedOrder(t, db, "sl-sig-E", "SYME", "SELL", "SL_SELL", "SL_DEFERRED_BAND", 0, "")
	// F: naked with terminal SL — FILLED BUY + only a REJECTED SL → SHOULD appear
	seedOrder(t, db, "sig-F", "SYMF", "BUY", "LIMIT_BUY", "FILLED", 10, "NB5")
	seedOrder(t, db, "sl-sig-F", "SYMF", "SELL", "SL_SELL", "REJECTED", 0, "")

	got, err := r.ListNakedOpenBuyPositions(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	inList := map[string]bool{}
	for _, p := range got {
		if p.StrategyID == nkStrategy {
			inList[p.Symbol] = true
		}
	}
	if !inList["SYMA"] {
		t.Error("SYMA (naked, no SL) must be listed")
	}
	if !inList["SYMF"] {
		t.Error("SYMF (naked, only REJECTED SL) must be listed")
	}
	for _, s := range []string{"SYMB", "SYMC", "SYMD", "SYME"} {
		if inList[s] {
			t.Errorf("%s must NOT be listed (protected/closed/paper/deferred)", s)
		}
	}
}

func TestNaked_ResetTerminalSLToPending(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	seedOrder(t, db, "sig-R", "SYMR", "BUY", "LIMIT_BUY", "FILLED", 10, "NBR")
	slID := seedOrder(t, db, "sl-sig-R", "SYMR", "SELL", "SL_SELL", "REJECTED", 0, "")

	id, found, err := r.ResetTerminalSLToPending(ctx, "sl-sig-R", 80.0, 79.5)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if !found || id != slID {
		t.Fatalf("expected to reset row %d, got id=%d found=%v", slID, id, found)
	}
	var status string
	_ = db.QueryRow(`SELECT status FROM manthan_orders WHERE id=$1`, slID).Scan(&status)
	if status != "PENDING" {
		t.Fatalf("expected PENDING after reset, got %s", status)
	}

	// No terminal row for a non-existent signal → found=false (no error).
	_, found2, err := r.ResetTerminalSLToPending(ctx, "sl-does-not-exist", 1, 1)
	if err != nil || found2 {
		t.Fatalf("expected (not found, nil err); got found=%v err=%v", found2, err)
	}
}
