package manthan

// Regressions for the 2026-08-17/18 EOD arming incident. Three coupled
// defects meant the book was only ever protected overnight BY ACCIDENT:
//   1. nextTradingDate was calendar-tomorrow, so the ArmRetryWorker's 00:03
//      IST cycle stamped AMOs that entered TODAY's session with TOMORROW's
//      date;
//   2. InsertAMOOrder's signal_id was "<entry>-amo-<date>" and the table has
//      UNIQUE(signal_id), so one REJECTED attempt (dead token at 16:35)
//      blocked every later re-arm for that date with "duplicate key";
//   3. HasActiveProtectionForToday / InsertAMOOrder trusted any non-terminal
//      row for (entry, date) — including (1)'s stale rows — so both the
//      evening Phase A and the 09:14 cron said "already protected" for a
//      session that had no broker-side order at all.

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

var istTZ = time.FixedZone("IST", 5*3600+1800)

func TestNextTradingDate_IsTheSessionTheAMOEnters(t *testing.T) {
	p := &ProtectiveReplay{ist: istTZ}
	cases := []struct {
		name string
		now  time.Time
		want string
	}{
		{"16:35 Tue after close → Wed", time.Date(2026, 8, 18, 16, 35, 0, 0, istTZ), "2026-08-19"},
		{"00:03 Tue arm-retry → Tue (today's session!)", time.Date(2026, 8, 18, 0, 3, 0, 0, istTZ), "2026-08-18"},
		{"08:50 Tue pre-open → Tue", time.Date(2026, 8, 18, 8, 50, 0, 0, istTZ), "2026-08-18"},
		{"15:29 Tue → Tue", time.Date(2026, 8, 18, 15, 29, 59, 0, istTZ), "2026-08-18"},
		{"15:30 Tue exactly → Wed", time.Date(2026, 8, 18, 15, 30, 0, 0, istTZ), "2026-08-19"},
		{"Fri 16:35 → Mon", time.Date(2026, 8, 21, 16, 35, 0, 0, istTZ), "2026-08-24"},
		{"Sat 00:03 → Mon", time.Date(2026, 8, 22, 0, 3, 0, 0, istTZ), "2026-08-24"},
		{"UTC caller 18:33Z Mon (=00:03 IST Tue) → Tue", time.Date(2026, 8, 17, 18, 33, 0, 0, time.UTC), "2026-08-18"},
	}
	for _, c := range cases {
		if got := p.nextTradingDate(c.now).Format("2006-01-02"); got != c.want {
			t.Errorf("%s: got %s want %s", c.name, got, c.want)
		}
	}
}

func TestArmedWindowStart_IsPreviousSessionClose(t *testing.T) {
	// Wed 19th → Tue 18th 15:30 IST (minus 60 s clock-skew tolerance)
	got := armedWindowStart(time.Date(2026, 8, 19, 0, 0, 0, 0, istTZ))
	if want := time.Date(2026, 8, 18, 15, 29, 0, 0, istTZ); !got.Equal(want) {
		t.Errorf("Wed: got %v want %v", got, want)
	}
	// Mon 24th → Fri 21st 15:30 IST (skips the weekend)
	got = armedWindowStart(time.Date(2026, 8, 24, 0, 0, 0, 0, istTZ))
	if want := time.Date(2026, 8, 21, 15, 29, 0, 0, istTZ); !got.Equal(want) {
		t.Errorf("Mon: got %v want %v", got, want)
	}
	// The incident row: created 00:03 IST Tue, stamped Wed → BEFORE Tue's
	// close → not live protection for Wed.
	stale := time.Date(2026, 8, 18, 0, 3, 0, 0, istTZ)
	if !stale.Before(armedWindowStart(time.Date(2026, 8, 19, 0, 0, 0, 0, istTZ))) {
		t.Error("mislabelled 00:03 row must fall before the armed window for the next day")
	}
	// A legit 16:35 Tue AMO for Wed is inside the window.
	legit := time.Date(2026, 8, 18, 16, 35, 0, 0, istTZ)
	if legit.Before(armedWindowStart(time.Date(2026, 8, 19, 0, 0, 0, 0, istTZ))) {
		t.Error("16:35 AMO for next day must be inside the armed window")
	}
}

// DB-backed: reproduces last night exactly.
func TestInsertAMOOrder_ReArmsAfterRejectedAndIgnoresStaleRows(t *testing.T) {
	db := openExecTestDB(t)
	defer nkClean(t, db)
	r := NewRepository(db)
	ctx := context.Background()

	entryID := seedOrder(t, db, "amo-entry-1", "IOLCP", "BUY", "LIMIT_BUY", "FILLED", 121, "B1")
	pos := PositionNeedingProtection{
		EntryOrderID: entryID, EntrySignalID: "amo-entry-1", StrategyID: nkStrategy, UserID: "S4450",
		Symbol: "IOLCP", Exchange: "NSE", NetQty: 121, IndiraSymbol: "STK_IOLCP_EQ_NSE_1", ExchangeToken: "1",
	}
	wed := time.Date(2026, 8, 19, 0, 0, 0, 0, istTZ)

	// 1) First attempt inserts with the historical id.
	id1, exists, err := r.InsertAMOOrder(ctx, entryID, pos, wed, 131.56, 131.00)
	if err != nil || exists {
		t.Fatalf("first insert: id=%d exists=%v err=%v", id1, exists, err)
	}
	var sig string
	_ = db.QueryRow(`SELECT signal_id FROM manthan_orders WHERE id=$1`, id1).Scan(&sig)
	if sig != "amo-entry-1-amo-20260819" {
		t.Fatalf("first signal_id = %q", sig)
	}
	// Same cycle re-run → already armed (row is fresh: created now, after Tue close? "now" is
	// whatever the test clock is; force created_at inside the window explicitly).
	_, _ = db.Exec(`UPDATE manthan_orders SET created_at = $2 WHERE id=$1`, id1, time.Date(2026, 8, 18, 16, 35, 0, 0, istTZ))
	if _, exists, err = r.InsertAMOOrder(ctx, entryID, pos, wed, 131.56, 131.00); err != nil || !exists {
		t.Fatalf("re-run must report alreadyExists: exists=%v err=%v", exists, err)
	}

	// 2) Broker rejects it (dead token). The old code could now NEVER re-arm
	//    for the 19th: "duplicate key value violates unique constraint
	//    manthan_orders_signal_id_key" every 5 minutes until midnight.
	if err := r.UpdateOrderRejected(ctx, id1, "indira: session expired (AU0xx)"); err != nil {
		t.Fatal(err)
	}
	id2, exists, err := r.InsertAMOOrder(ctx, entryID, pos, wed, 131.56, 131.00)
	if err != nil || exists {
		t.Fatalf("re-arm after REJECTED must insert a fresh row: id=%d exists=%v err=%v", id2, exists, err)
	}
	_ = db.QueryRow(`SELECT signal_id FROM manthan_orders WHERE id=$1`, id2).Scan(&sig)
	if sig != "amo-entry-1-amo-20260819-r1" {
		t.Fatalf("retry signal_id = %q, want -r1 suffix", sig)
	}

	// 3) Stale row: SL_PLACED for the 19th but CREATED at 00:03 IST on the
	//    18th (the mislabelled arm-retry cycle; that order was live on the
	//    18th and swept at 15:30). It must count as NEITHER "already armed"
	//    for InsertAMOOrder NOR "already protected" for the 09:14 cron.
	_, _ = db.Exec(`UPDATE manthan_orders SET status='REJECTED' WHERE id=$1`, id2) // clear (2)'s live row
	staleID := seedOrder(t, db, "amo-entry-1-amo-20260819-stale", "IOLCP", "SELL", "SL_SELL_AMO", "SL_PLACED", 0, "NYMZX002AD=8")
	_, _ = db.Exec(`UPDATE manthan_orders SET parent_order_id=$2, trade_date=$3, created_at=$4 WHERE id=$1`,
		staleID, entryID, wed, time.Date(2026, 8, 18, 0, 3, 0, 0, istTZ))
	protected, err := r.HasActiveProtectionForToday(ctx, entryID, wed)
	if err != nil || protected {
		t.Fatalf("stale pre-close row reported as live protection: protected=%v err=%v", protected, err)
	}
	id3, exists, err := r.InsertAMOOrder(ctx, entryID, pos, wed, 131.56, 131.00)
	if err != nil || exists {
		t.Fatalf("stale row must not block a real re-arm: id=%d exists=%v err=%v", id3, exists, err)
	}
	// And the fresh row (created now — but pin it inside the window) IS protection.
	_, _ = db.Exec(`UPDATE manthan_orders SET created_at=$2 WHERE id=$1`, id3, time.Date(2026, 8, 18, 16, 40, 0, 0, istTZ))
	if protected, err = r.HasActiveProtectionForToday(ctx, entryID, wed); err != nil || !protected {
		t.Fatalf("fresh in-window row must be protection: protected=%v err=%v", protected, err)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM manthan_orders WHERE parent_order_id=$1 AND trade_date=$2`, entryID, wed).Scan(&n)
	if n != 4 {
		t.Fatalf("expected 4 attempt rows (orig, r1, stale, r3), got %d", n)
	}
	// The stale row was retired in the DB (never touched at the broker) so it
	// can no longer masquerade as protection or block the unique index.
	var staleStatus string
	_ = db.QueryRow(`SELECT status FROM manthan_orders WHERE id=$1`, staleID).Scan(&staleStatus)
	if staleStatus != "EXPIRED" {
		t.Fatalf("stale pre-window row status = %s, want EXPIRED", staleStatus)
	}
	_ = sql.ErrNoRows
}
