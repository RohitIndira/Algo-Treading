package manthan

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestFirstSeenCarryQuery_SourceByRunState(t *testing.T) {
	first := firstSeenCarryQuery(false)
	if !strings.Contains(first, "MAX(run_date)") || !strings.Contains(first, "run_date = CURRENT_DATE") {
		t.Fatalf("day's first publish must read today's and the previous publish day's rows:\n%s", first)
	}
	later := firstSeenCarryQuery(true)
	if strings.Contains(later, "MAX(run_date)") || !strings.Contains(later, "run_date = CURRENT_DATE") {
		t.Fatalf("a later publish today must read today's rows only:\n%s", later)
	}
}

// Same-day churn against the real signals_db schema (local Postgres; skipped
// when unreachable):
//  1. yesterday: X (first_seen T0); nothing published today → X inherits T0
//  2. a publish landed today (audit row) and X is NOT in today's signal rows
//     (operator removed it) → X is NOT inherited → re-add stamps fresh
//  3. X back in today's rows with first_seen T1 → X inherits T1
func TestLoadFirstSeen_SameDayRemoveReaddStartsNewRun(t *testing.T) {
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=signals_db sslmode=disable")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("signals_db not reachable — skipping (%v)", err)
	}
	defer db.Close()
	cleanup := func() {
		_, _ = db.Exec(`DELETE FROM manthan_signals WHERE symbol LIKE 'TSEEN%'`)
		_, _ = db.Exec(`DELETE FROM manthan_stocks WHERE symbol LIKE 'TSEEN%'`)
	}
	cleanup()
	t.Cleanup(cleanup)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 7, 9, 2, 0, 0, time.UTC)
	t1 := time.Date(2026, 10, 8, 8, 30, 0, 0, time.UTC)

	// A real publish from an earlier day carrying X.
	if _, err := db.Exec(`INSERT INTO manthan_signals (run_date, symbol, first_seen_at) VALUES (CURRENT_DATE - 1, 'TSEEN_X', $1)`, t0); err != nil {
		t.Fatalf("seed yesterday: %v", err)
	}

	// 1. Day's first publish: inherit from yesterday.
	got, err := loadFirstSeen(ctx, db, false)
	if err != nil {
		t.Fatalf("load (first publish): %v", err)
	}
	if !got["TSEEN_X"].Equal(t0) {
		t.Fatalf("first publish must inherit yesterday's first_seen: got %v want %v", got["TSEEN_X"], t0)
	}

	// 2. A later publish today: X was removed from the sheet, so it is absent
	//    from today's rows → must NOT be inherited.
	if _, err := db.Exec(`INSERT INTO manthan_stocks (run_date, symbol, status) VALUES (CURRENT_DATE, 'TSEEN_OTHER', 'DATA_DROPPED')`); err != nil {
		t.Fatalf("seed audit: %v", err)
	}
	got, err = loadFirstSeen(ctx, db, true)
	if err != nil {
		t.Fatalf("load (later publish): %v", err)
	}
	if _, carried := got["TSEEN_X"]; carried {
		t.Fatalf("a stock absent from today's rows after a publish today must start a new run, got %v", got["TSEEN_X"])
	}

	// 3. X re-added earlier today with first_seen T1 → inherit T1 (no drift on re-runs).
	if _, err := db.Exec(`INSERT INTO manthan_signals (run_date, symbol, first_seen_at) VALUES (CURRENT_DATE, 'TSEEN_X', $1)`, t1); err != nil {
		t.Fatalf("seed today: %v", err)
	}
	got, err = loadFirstSeen(ctx, db, true)
	if err != nil {
		t.Fatalf("load (re-run): %v", err)
	}
	if !got["TSEEN_X"].Equal(t1) {
		t.Fatalf("re-run must keep today's first_seen: got %v want %v", got["TSEEN_X"], t1)
	}

	// hadPublishToday sees the audit row written in step 2.
	p := &Publisher{db: db}
	ok, err := p.hadPublishToday(ctx)
	if err != nil || !ok {
		t.Fatalf("hadPublishToday = %v, %v; want true", ok, err)
	}
}
