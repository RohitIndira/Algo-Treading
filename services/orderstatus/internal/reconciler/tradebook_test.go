package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

const tbTestPrefix = "TB-TEST-"

func tbTestDB(t *testing.T) *sql.DB {
	t.Helper()
	env := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		env("POSTGRES_HOST", "localhost"), env("POSTGRES_PORT", "5432"),
		env("POSTGRES_USER", "postgres"), env("POSTGRES_PASSWORD", "postgres"),
		env("ORDER_STATUS_DB", "order_status_db"), env("POSTGRES_SSL_MODE", "disable"))
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("no test DB (open): %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Skipf("no test DB (ping): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func tbCleanup(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`DELETE FROM broker_events WHERE broker_order_id LIKE $1`, tbTestPrefix+"%"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

func countRows(t *testing.T, db *sql.DB, brokerOrderID string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM broker_events WHERE broker_order_id = $1`, brokerOrderID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", brokerOrderID, err)
	}
	return n
}

// TestTradebookSweep_CollideInsertSkip drives one user's tradebook with three
// trades — one colliding with a pre-existing WSS row, one fresh fill, one
// pre-cutoff — and asserts the resulting broker_events state + the WARN.
func TestTradebookSweep_CollideInsertSkip(t *testing.T) {
	db := tbTestDB(t)
	tbCleanup(t, db)
	t.Cleanup(func() { tbCleanup(t, db) })

	// Capture WARN+ logs so we can assert the collision is loud, not silent.
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	writer := store.NewWriter(db, logger)
	r := New(nil, writer, nil, nil, nil, Config{}, logger)

	// Boot cutoff: trades before 10:00 IST are historical and must be skipped.
	cutoff, err := parseIndiraOrdDate("2026-07-20 10:00:00")
	if err != nil {
		t.Fatalf("parse cutoff: %v", err)
	}
	r.cutoff = cutoff

	ctx := context.Background()

	// Pre-seed a WSS row for TB-TEST-1 whose event_seq equals trade1's
	// ExchTrdId → forces the cross-source collision path.
	const collideSeq int64 = 111111111111111
	if _, inserted, err := writer.Insert(ctx, &store.Event{
		BrokerOrderID: tbTestPrefix + "1",
		EventSeq:      collideSeq,
		Source:        store.SourceWSS,
		EventType:     store.EventFilled,
		Status:        "EXECUTED",
		UserID:        "TEST-USER",
		RawPayload:    []byte(`{"seed":"wss"}`),
	}); err != nil || !inserted {
		t.Fatalf("seed WSS row: inserted=%v err=%v", inserted, err)
	}

	trades := []indira.TradeBook{
		{ // trade1 — collides with the seeded WSS row
			OrdId:       tbTestPrefix + "1",
			ExchTrdId:   json.RawMessage("111111111111111"),
			TradeTime:   "2026-07-20 11:00:00", // after cutoff
			TradedPrice: 100.5,
			TradedQty:   5,
			OrdAction:   "BUY",
		},
		{ // trade2 — fresh fill with authoritative price
			OrdId:       tbTestPrefix + "2",
			ExchTrdId:   json.RawMessage("222222222222222"),
			TradeTime:   "2026-07-20 11:05:00", // after cutoff
			TradedPrice: 200.25,
			TradedQty:   3,
			OrdAction:   "SELL",
		},
		{ // trade3 — pre-cutoff, must be skipped
			OrdId:       tbTestPrefix + "3",
			ExchTrdId:   json.RawMessage("333333333333333"),
			TradeTime:   "2026-07-20 09:00:00", // BEFORE cutoff
			TradedPrice: 300.0,
			TradedQty:   2,
			OrdAction:   "BUY",
		},
	}

	observed, inserted, published, collided, preCutoff, skipped :=
		r.tradebookSweepForTrades(ctx, "TEST-USER", trades)

	if observed != 3 {
		t.Errorf("observed = %d, want 3", observed)
	}
	if inserted != 1 {
		t.Errorf("inserted = %d, want 1 (only trade2)", inserted)
	}
	if collided != 1 {
		t.Errorf("collided = %d, want 1 (trade1)", collided)
	}
	if preCutoff != 1 {
		t.Errorf("preCutoff = %d, want 1 (trade3)", preCutoff)
	}
	if published != 0 {
		t.Errorf("published = %d, want 0 (nil publisher)", published)
	}
	if skipped != 0 {
		t.Errorf("other skipped = %d, want 0", skipped)
	}

	// DB state: TB-TEST-1 keeps ONLY the seeded WSS row (tradebook deduped);
	// TB-TEST-2 got exactly one REST_TRADEBOOK row with the real price;
	// TB-TEST-3 has none.
	if n := countRows(t, db, tbTestPrefix+"1"); n != 1 {
		t.Errorf("TB-TEST-1 rows = %d, want 1 (WSS only)", n)
	}
	if n := countRows(t, db, tbTestPrefix+"2"); n != 1 {
		t.Errorf("TB-TEST-2 rows = %d, want 1 (tradebook)", n)
	}
	if n := countRows(t, db, tbTestPrefix+"3"); n != 0 {
		t.Errorf("TB-TEST-3 rows = %d, want 0 (pre-cutoff)", n)
	}

	var src string
	var price sql.NullFloat64
	if err := db.QueryRow(`SELECT source, traded_price FROM broker_events WHERE broker_order_id=$1`,
		tbTestPrefix+"2").Scan(&src, &price); err != nil {
		t.Fatalf("read TB-TEST-2: %v", err)
	}
	if src != string(store.SourceRESTTradebook) {
		t.Errorf("TB-TEST-2 source = %q, want REST_TRADEBOOK", src)
	}
	if !price.Valid || price.Float64 != 200.25 {
		t.Errorf("TB-TEST-2 traded_price = %v, want 200.25", price)
	}

	// The collision must have produced exactly one loud WARN.
	if got := logs.FilterMessageSnippet("collided").Len(); got != 1 {
		t.Errorf("collision WARN count = %d, want 1", got)
	}
}
