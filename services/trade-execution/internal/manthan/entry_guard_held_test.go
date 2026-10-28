package manthan

// Regression guard for 2026-10-28: the entry duplicate check treated every
// FILLED BUY as "already have position" forever, so a stock could never be
// re-entered after its stop or exit FILLED (S4450 SHREEPUSHK: stopped out
// 10-23, fresh signal 10-28 dropped 26 times; 43 pairs blocked fleet-wide).
// HeldEntryBySymbol must answer from the order-derived net holding.
//
// Runs against local execution_db; skips if unreachable.

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestHeldEntryBySymbol_ReentryAfterStopOut(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	// Held: FILLED BUY, nothing sold → the guard must block a second entry.
	seedOrder(t, db, "sig-held", "HELDSYM", "BUY", "LIMIT_BUY", "FILLED", 42, "NBHELD")
	e, net, err := r.HeldEntryBySymbol(ctx, nkStrategy, "HELDSYM")
	if err != nil || e == nil || net != 42 {
		t.Fatalf("held lot must be reported: entry=%v net=%d err=%v", e, net, err)
	}

	// Stopped out: FILLED BUY 42 + FILLED stop SELL 42 → nothing held → the
	// guard must let today's fresh signal through (sql.ErrNoRows).
	seedOrder(t, db, "sig-out", "OUTSYM", "BUY", "LIMIT_BUY", "FILLED", 42, "NBOUT")
	seedOrder(t, db, "sl-sig-out", "OUTSYM", "SELL", "SL_SELL", "FILLED", 42, "NBOUTSL")
	e, net, err = r.HeldEntryBySymbol(ctx, nkStrategy, "OUTSYM")
	if !errors.Is(err, sql.ErrNoRows) || e != nil || net != 0 {
		t.Fatalf("stopped-out lot must NOT block re-entry: entry=%v net=%d err=%v", e, net, err)
	}

	// Partially sold: 42 bought, 20 sold → still held → still blocks.
	seedOrder(t, db, "sig-part", "PARTSYM", "BUY", "LIMIT_BUY", "FILLED", 42, "NBPART")
	seedOrder(t, db, "ex-sig-part", "PARTSYM", "SELL", "MARKET_SELL", "FILLED", 20, "NBPARTEX")
	e, net, err = r.HeldEntryBySymbol(ctx, nkStrategy, "PARTSYM")
	if err != nil || e == nil || net != 22 {
		t.Fatalf("partially held lot must still block: entry=%v net=%d err=%v", e, net, err)
	}

	// Never bought → no rows.
	if _, _, err = r.HeldEntryBySymbol(ctx, nkStrategy, "NOSUCHSYM"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown symbol must surface sql.ErrNoRows, got %v", err)
	}
}
