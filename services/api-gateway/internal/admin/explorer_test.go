package admin

// M6 tests — DB-backed against the real local schemas across all four
// databases, with a dedicated symbol so fixtures never collide.

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"testing"
	"time"
)

const m6Sym = "TADMM6SYM"

func openSignalsFeedDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=signals_db sslmode=disable")
	if err != nil {
		t.Fatalf("open signals_db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("signals_db not reachable — skipping M6 tests (%v)", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedM6(t *testing.T, signals, trading, exec, pos *sql.DB) {
	t.Helper()
	cleanup := func() {
		_, _ = signals.Exec(`DELETE FROM manthan_stocks WHERE symbol = $1`, m6Sym)
		_, _ = signals.Exec(`DELETE FROM manthan_signals WHERE symbol = $1`, m6Sym)
		_, _ = trading.Exec(`DELETE FROM manthan_signal_decisions WHERE symbol = $1`, m6Sym)
		_, _ = trading.Exec(`DELETE FROM manthan_cooldown WHERE symbol = $1`, m6Sym)
		_, _ = exec.Exec(`DELETE FROM signal_inbox WHERE payload->>'symbol' = $1`, m6Sym)
		// orders + positions + manthan_positions ride the m2User cleanups
	}
	cleanup()
	t.Cleanup(cleanup)

	// Day 1: universe row rejected; day 2: eligible + published signal.
	mustExec(t, signals, `INSERT INTO manthan_stocks (run_date, symbol, status, reason, pe, fscore, created_at)
		VALUES (current_date - 1, $1, 'FILTER_REJECTED', 'PE out of range', 99, 40, now() - interval '2 days'),
		       (current_date, $1, 'ELIGIBLE', NULL, 22, 75, now() - interval '26 hours')`, m6Sym)
	mustExec(t, signals, `INSERT INTO manthan_signals (run_date, symbol, pe, fscore, latest_price, first_seen_at, published_to_kafka_at, created_at)
		VALUES (current_date, $1, 22, 75, 450, now() - interval '25 hours', now() - interval '25 hours', now() - interval '25 hours')`, m6Sym)

	// Per-user allocation decision (the fixture user + a second user the
	// filter test must exclude).
	mustExec(t, trading, `INSERT INTO manthan_signal_decisions
			(signal_id, user_id, strategy_id, symbol, ltp_at_decision, intended_qty,
			 intended_invested, initial_sl_target, status, decided_at, dispatched_at)
		VALUES ('aaaaaaaa-6666-2222-3333-000000000001', $1, $2::uuid, $3, 450, 42, 18900, 360, 'DISPATCHED', now() - interval '24 hours', now() - interval '24 hours'),
		       ('aaaaaaaa-6666-2222-3333-000000000002', 'TADM_OTHER', $2::uuid, $3, 450, 10, 4500, 360, 'REJECTED', now() - interval '24 hours 1 minute', NULL)`,
		m2User, m2Strat, m6Sym)
	mustExec(t, trading, `UPDATE manthan_signal_decisions SET rejection_reason='margin insufficient'
		WHERE user_id='TADM_OTHER' AND symbol=$1`, m6Sym)
	// The prod shape that 500'd the first deploy: a non-entry decision row
	// (migration 009) with NULL qty/ltp/sl-target.
	mustExec(t, trading, `INSERT INTO manthan_signal_decisions
			(signal_id, user_id, strategy_id, symbol, signal_type, status, decided_at, payload,
			 ltp_at_decision, intended_qty, intended_invested, initial_sl_target)
		VALUES ('aaaaaaaa-6666-2222-3333-000000000004', $1, $2::uuid, $3, 'SL_MODIFY', 'DISPATCHED',
			now() - interval '20 hours', '{}'::jsonb, NULL, NULL, NULL, NULL)`, m2User, m2Strat, m6Sym)

	// Inbox: a DONE entry and an AUTH_EXPIRED DLQ.
	mustExec(t, exec, `INSERT INTO signal_inbox (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset, status, attempts, created_at, completed_at)
		VALUES ('aaaaaaaa-6666-2222-3333-000000000001', $1, 'MANTHAN_ENTRY', jsonb_build_object('symbol',$2::text), 't', 0, 101, 'DONE', 1, now() - interval '23 hours', now() - interval '23 hours')`,
		m2User, m6Sym)
	mustExec(t, exec, `INSERT INTO signal_inbox (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset, status, attempts, last_error_class, last_error, created_at)
		VALUES ('aaaaaaaa-6666-2222-3333-000000000003', $1, 'MANTHAN_ENTRY', jsonb_build_object('symbol',$2::text), 't', 0, 102, 'DLQ', 3, 'AUTH_EXPIRED', 'broker AU004: invalid session', now() - interval '22 hours')`,
		m2User, m6Sym)

	// Orders: filled entry + rejected SL attempt with taxonomy tags.
	mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, avg_fill_price, broker_order_id, status, created_at, filled_at)
		VALUES ('tadm-m6-entry', $1, $2, $3, 'BUY', 'BUY', 42, 42, 451.4, 'NBRK00042M6', 'FILLED', now() - interval '23 hours', now() - interval '23 hours')`,
		m2Strat, m2User, m6Sym)
	mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, last_error, exchange_error_tag, reject_category, created_at)
		VALUES ('tadm-m6-slrej', $1, $2, $3, 'SL_SELL', 'SELL', 42, 0, 'REJECTED', 'RMS:Rule: Check circuit limit', 'CIRCUIT_LIMIT', 'DPR_BAND', now() - interval '22 hours')`,
		m2Strat, m2User, m6Sym)

	// Position: opened then exited with P&L; cooldown row from the exit.
	mustExec(t, pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			status, entry_price, entry_time, quantity, invested_amount, exit_price, exit_time, exit_reason, realized_pnl)
		VALUES ('aaaaaaaa-6666-2222-3333-000000000e01', 'MANTHAN', $1, $2, 'aaaaaaaa-6666-4444-3333-000000000001', $3, 'NSE',
			'EXITED', 451.4, now() - interval '23 hours', 42, 18958, 420, now() - interval '2 hours', 'SL_TRIGGER', -1318.8)`,
		m2User, m2Strat, m6Sym)
	mustExec(t, trading, `INSERT INTO manthan_cooldown (strategy_id, symbol, ath_at_exit, exit_price, reentry_below, exit_time)
		VALUES ($1::uuid, $2, 525, 420, 420, now() - interval '2 hours')`, m2Strat, m6Sym)
}

func TestM6_Trace_EndToEnd(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	signals := openSignalsFeedDB(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())
	seedM6(t, signals, trading, exec, pos)

	ex := NewExplorer(signals, NewFleetStore(trading, exec, pos))
	res, err := ex.Trace(context.Background(), m6Sym, "", 10)
	if err != nil {
		t.Fatalf("trace: %v", err)
	}

	stages := map[string]int{}
	for _, ev := range res.Events {
		stages[ev.Stage]++
	}
	for _, want := range []string{"UNIVERSE", "SIGNAL", "DECISION", "INBOX", "ORDER", "FILL", "POSITION", "EXIT", "COOLDOWN"} {
		if stages[want] == 0 {
			t.Fatalf("stage %s missing (got %+v)", want, stages)
		}
	}
	if stages["UNIVERSE"] != 2 || stages["DECISION"] != 3 || stages["INBOX"] != 2 {
		t.Fatalf("stage counts: %+v", stages)
	}
	// Chronological.
	for i := 1; i < len(res.Events); i++ {
		if res.Events[i].TS.Before(res.Events[i-1].TS) {
			t.Fatalf("events not chronological at %d", i)
		}
	}
	// The rejection day's verdict text must surface.
	var sawReject, sawAuthDLQ, sawCooldown, sawNullQty bool
	for _, ev := range res.Events {
		if ev.Stage == "UNIVERSE" && strings.Contains(ev.Summary, "PE out of range") {
			sawReject = true
		}
		if ev.Stage == "INBOX" && strings.Contains(ev.Summary, "[AUTH_EXPIRED]") {
			sawAuthDLQ = true
		}
		if ev.Stage == "COOLDOWN" && strings.Contains(ev.Summary, "below 420.00") {
			sawCooldown = true
		}
		if ev.Stage == "DECISION" && strings.Contains(ev.Summary, "SL_MODIFY") {
			sawNullQty = true
		}
	}
	if !sawReject || !sawAuthDLQ || !sawCooldown || !sawNullQty {
		t.Fatalf("summaries missing: reject=%v authDLQ=%v cooldown=%v nullQty=%v", sawReject, sawAuthDLQ, sawCooldown, sawNullQty)
	}

	// User filter: TADM_OTHER's decision must vanish.
	filtered, err := ex.Trace(context.Background(), m6Sym, m2User, 10)
	if err != nil {
		t.Fatalf("filtered trace: %v", err)
	}
	for _, ev := range filtered.Events {
		if ev.Stage == "DECISION" && strings.Contains(ev.Summary, "TADM_OTHER") {
			t.Fatalf("user filter leaked TADM_OTHER: %s", ev.Summary)
		}
	}
}

func TestM6_Candidates(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	signals := openSignalsFeedDB(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())
	seedM6(t, signals, trading, exec, pos)

	ex := NewExplorer(signals, NewFleetStore(trading, exec, pos))
	res, err := ex.Candidates(context.Background(), "")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	// Latest run day carries our ELIGIBLE row.
	var found bool
	for _, r := range res.Rows {
		if r.Symbol == m6Sym && r.Status == "ELIGIBLE" {
			found = true
		}
	}
	if !found {
		t.Fatalf("today's ELIGIBLE candidate missing (date=%s rows=%d)", res.RunDate, len(res.Rows))
	}
	if len(res.RecentDates) == 0 || res.RecentDates[0] != res.RunDate {
		t.Fatalf("recent dates: %+v (run=%s)", res.RecentDates, res.RunDate)
	}

	// Yesterday: the rejection with its reason counted.
	yesterday, err := ex.Candidates(context.Background(), time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
	if err != nil {
		t.Fatalf("yesterday: %v", err)
	}
	if yesterday.CountsByReason["PE out of range"] == 0 {
		t.Fatalf("yesterday's rejection reason missing: %+v", yesterday.CountsByReason)
	}

	// Unwired signals_db must refuse loudly, not half-answer.
	if _, err := NewExplorer(nil, NewFleetStore(trading, exec, pos)).Candidates(context.Background(), ""); err == nil {
		t.Fatalf("nil signals_db must error")
	}
}

func TestM6_InboxAndRejections_HTTP(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	signals := openSignalsFeedDB(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())
	seedM6(t, signals, trading, exec, pos)

	fleet := NewFleetStore(trading, exec, pos)
	ex := NewExplorer(signals, fleet)

	// Class filter narrows to the DLQ row.
	inbox, err := ex.Inbox(context.Background(), "", "AUTH_EXPIRED", "", 7)
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	if len(inbox.Rows) == 0 {
		t.Fatalf("class filter found nothing")
	}
	for _, r := range inbox.Rows {
		if r.ErrorClass != "AUTH_EXPIRED" {
			t.Fatalf("class filter leaked %+v", r)
		}
	}
	if inbox.Rows[0].Symbol == "" {
		t.Fatalf("symbol not extracted from payload: %+v", inbox.Rows[0])
	}

	rej, err := ex.Rejections(context.Background(), 7)
	if err != nil {
		t.Fatalf("rejections: %v", err)
	}
	var tagged bool
	for _, b := range rej {
		if b.ExchangeErrorTag == "CIRCUIT_LIMIT" && b.RejectCategory == "DPR_BAND" && b.Count >= 1 {
			tagged = true
			if !strings.Contains(b.SampleError, "circuit") {
				t.Fatalf("sample error missing: %+v", b)
			}
		}
	}
	if !tagged {
		t.Fatalf("tagged rejection bucket missing: %+v", rej)
	}

	// HTTP chain: all four routes.
	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M6ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetFleetStore(fleet)
	h.SetExplorer(ex)
	r := newRouterFor(t, h)
	token := elevateViaHTTP(t, r, "TADM_M6ADMIN")

	rec := m4do(t, r, token, "GET", "/api/v1/admin/trace/"+m6Sym+"?days=10", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"DECISION"`) {
		t.Fatalf("trace http: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/signals/candidates", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), m6Sym) {
		t.Fatalf("candidates http: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/inbox?class=AUTH_EXPIRED", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "AU004") {
		t.Fatalf("inbox http: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/rejections", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "CIRCUIT_LIMIT") {
		t.Fatalf("rejections http: %d %.200s", rec.Code, rec.Body.String())
	}
}
