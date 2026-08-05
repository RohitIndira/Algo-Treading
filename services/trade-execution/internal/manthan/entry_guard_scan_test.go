package manthan

// Regression guard for the 2026-08-05 double-buy: GetActiveEntryBySymbol used
// a NULL-unsafe row scanner, so a FILLED entry whose broker_status/last_error
// were NULL (the NORMAL state for a cleanly-filled order) errored at Scan —
// and the entry handler swallowed the error as "no duplicate", re-buying held
// symbols with real money. Only rows with those columns populated (NRBBEARING,
// stamped by an earlier reconciliation) were caught.
//
// Runs against local execution_db; skips if unreachable.

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/lib/pq"
)

func TestGetActiveEntryBySymbol_NullSafeScan(t *testing.T) {
	db := openExecTestDB(t) // helper from naked_position_test.go (cleans nkStrategy rows)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	// The incident shape: FILLED LIMIT_BUY with broker_status + last_error
	// BOTH NULL (seedOrder leaves them NULL — same as a clean production fill).
	seedOrder(t, db, "sig-scan", "SCANSYM", "BUY", "LIMIT_BUY", "FILLED", 10, "NBSCAN")

	got, err := r.GetActiveEntryBySymbol(ctx, nkStrategy, "SCANSYM")
	if err != nil {
		t.Fatalf("NULL-safe scan must not error on a clean FILLED row: %v", err)
	}
	if got == nil {
		t.Fatal("duplicate-guard lookup returned nil for a held symbol — double-buy bug is back")
	}
	if got.Symbol != "SCANSYM" || got.Status != StatusFilled {
		t.Errorf("wrong row: %+v", got)
	}

	// No row → sql.ErrNoRows (the handler treats this as "no duplicate").
	_, err = r.GetActiveEntryBySymbol(ctx, nkStrategy, "NOSUCHSYM")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("no-match must surface sql.ErrNoRows, got %v", err)
	}
}
