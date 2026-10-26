package admin

// Book-reconciler action tests against the local DBs (skip when unreachable):
//
//	TestBookActions_PersistPledgeUnpledge — verdict lands on positions_db;
//	    Pledge flips trading_db to PLEDGED + parks the stop row (or inserts a
//	    marker) + writes the POSITION_PLEDGED event; a second Pledge is a
//	    no-op; Unpledge reverses all of it and leaves the lot naked for the
//	    next protection pass.
//	TestBookActions_PledgeLeavesLiveBrokerOrderAlone — a stop row whose
//	    broker order is live in this run's order book is never parked.
//	TestBookActions_HealWritesReconcilerReason — GhostHeal with
//	    ExitReason NOT_HELD_AT_BROKER writes that reason + the verdict on the
//	    book row, MANUAL_EXIT with the tradebook price otherwise.
//	TestReconcile_VerdictsFromSnapshot — ReconStore.Reconcile judges book lots
//	    and ledger-only lots from one broker read.

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

const m2PosAAA = "aaaaaaaa-1111-2222-3333-00000000aaa1"

func seedTradingPosition(t *testing.T, trading *sql.DB, symbol, status string) {
	t.Helper()
	mustExec(t, trading, `DELETE FROM manthan_positions WHERE strategy_id = $1 AND symbol = $2`, m2Strat, symbol)
	mustExec(t, trading, `
		INSERT INTO manthan_positions (strategy_id, user_id, symbol, isin, industry, mcap_bucket, index_name,
		                               entry_price, quantity, invested_amt, status, entry_time, updated_at)
		VALUES ($1, $2, $3, 'INE000000001', 'Cables', 'SMALL', 'NIFTY SMALLCAP 250', 100, 10, 1000, $4, now() - interval '2 days', now())`,
		m2Strat, m2User, symbol, status)
	t.Cleanup(func() {
		_, _ = trading.Exec(`DELETE FROM manthan_positions WHERE strategy_id = $1 AND symbol = $2`, m2Strat, symbol)
	})
}

func newBookActionsEnv(t *testing.T, broker *m7Broker) (*Actions, *sql.DB, *sql.DB, *sql.DB) {
	t.Helper()
	trading, execDB, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, execDB, pos, time.Now())
	fleet := NewFleetStore(trading, execDB, pos)
	a := NewActions(fleet, userKeyedCreds{m2User: credsOK(m2User)}, broker,
		NewStrategyControl(&stubStrategyRPC{}, fleet), nil, "http://127.0.0.1:1", "", "", nil)
	return a, trading, execDB, pos
}

func pledgedVerdict(prev string) Verdict {
	return Verdict{UserID: m2User, StrategyID: m2Strat, Symbol: "AAA", BookQty: 10,
		State: BrokerStatePledged, PrevState: prev, PositionIDs: []string{m2PosAAA},
		Reason:   "Pledged: 10 of 10 pledged, 0 free — unpledge to let the strategy manage it",
		Evidence: BrokerEvidence{BrokerQty: 10, PledgedQty: 10, BrokerLeg: "OK"}, CheckedAt: time.Now()}
}

func TestBookActions_PersistPledgeUnpledge(t *testing.T) {
	a, trading, execDB, pos := newBookActionsEnv(t, &m7Broker{})
	ctx := context.Background()
	seedTradingPosition(t, trading, "AAA", "ACTIVE")
	v := pledgedVerdict(BrokerStateHeld)

	// Verdict persisted on the positions_db row.
	n, err := a.PersistVerdicts(ctx, []Verdict{v}, VerdictSourceLogin)
	if err != nil || n != 1 {
		t.Fatalf("persist: n=%d err=%v", n, err)
	}
	var state, reason, source string
	var checked sql.NullTime
	var evidence string
	if err := pos.QueryRow(`SELECT broker_state, broker_state_reason, broker_state_source, broker_checked_at, broker_state_evidence::text
		FROM positions WHERE position_id = $1`, m2PosAAA).Scan(&state, &reason, &source, &checked, &evidence); err != nil {
		t.Fatalf("read verdict: %v", err)
	}
	if state != BrokerStatePledged || !strings.Contains(reason, "unpledge") || source != "login" || !checked.Valid || !strings.Contains(evidence, `"pledged_qty": 10`) {
		t.Fatalf("persisted verdict wrong: %s %q %s %v %s", state, reason, source, checked.Valid, evidence)
	}

	// Pledge: trading_db row PLEDGED with the reason, stop row parked, event.
	res, err := a.Pledge(ctx, v, nil)
	if err != nil {
		t.Fatalf("pledge: %v (%v)", err, res)
	}
	var tStatus, tLastStatus, tReason string
	if err := trading.QueryRow(`SELECT status, COALESCE(protective_last_status,''), COALESCE(protective_last_reason,'')
		FROM manthan_positions WHERE strategy_id=$1 AND symbol='AAA'`, m2Strat).Scan(&tStatus, &tLastStatus, &tReason); err != nil {
		t.Fatalf("trading row: %v", err)
	}
	if tStatus != "PLEDGED" || tLastStatus != "PLEDGED" || !strings.Contains(tReason, "unpledge") {
		t.Fatalf("trading_db not pledged: %s %s %q", tStatus, tLastStatus, tReason)
	}
	var parkedStatus, parkedTag string
	if err := execDB.QueryRow(`SELECT status, COALESCE(broker_status,'') FROM manthan_orders WHERE signal_id='tadm-m2-sl-aaa'`).Scan(&parkedStatus, &parkedTag); err != nil {
		t.Fatalf("stop row: %v", err)
	}
	if parkedStatus != "SL_DEFERRED_BAND" || parkedTag != "PLEDGED" {
		t.Fatalf("stop row not parked PLEDGED: %s/%s", parkedStatus, parkedTag)
	}
	if res["stop_rows_parked"].(int64) != 1 || res["event_written"] != true {
		t.Fatalf("pledge result: %v", res)
	}
	var events int
	_ = execDB.QueryRow(`SELECT COUNT(*) FROM manthan_order_events e JOIN manthan_orders o ON o.id=e.order_id
		WHERE o.user_id=$1 AND e.event_type='POSITION_PLEDGED'`, m2User).Scan(&events)
	if events != 1 {
		t.Fatalf("expected one POSITION_PLEDGED event, got %d", events)
	}

	// The fleet row now counts AAA as pledged, not armed and not naked; the
	// attention queue carries the sentence.
	fleet := NewFleetStore(trading, execDB, pos)
	rows, err := fleet.Fleet(ctx)
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	for _, r := range rows {
		if r.StrategyID == m2Strat {
			if r.Pledged != 1 || r.PledgedSymbols[0] != "AAA" || r.Armed != 0 {
				t.Fatalf("fleet row: pledged=%d armed=%d naked=%v", r.Pledged, r.Armed, r.NakedSymbols)
			}
		}
	}
	items, _, err := fleet.Attention(ctx)
	if err != nil {
		t.Fatalf("attention: %v", err)
	}
	found := false
	for _, it := range items {
		if it.Kind == "BROKER_PLEDGED" && it.UserID == m2User && it.Symbol == "AAA" {
			found = true
			if it.Severity != "WARN" || !strings.Contains(it.Detail, "unpledge") || it.Module != "book_reconciler" {
				t.Fatalf("BROKER_PLEDGED item wrong: %+v", it)
			}
		}
		if it.Kind == "NAKED_POSITION" && it.Symbol == "AAA" {
			t.Fatalf("a pledged lot must not read NAKED: %+v", it)
		}
	}
	if !found {
		t.Fatalf("BROKER_PLEDGED missing from attention")
	}

	// Second Pledge: idempotent (no new park, no new event).
	res2, err := a.Pledge(ctx, v, nil)
	if err != nil {
		t.Fatalf("pledge again: %v", err)
	}
	if res2["stop_rows_parked"].(int64) != 0 {
		t.Fatalf("re-pledge parked again: %v", res2)
	}

	// Unpledge: trading_db ACTIVE again, parked row closed, event written.
	v.State, v.PrevState, v.Reason = BrokerStateHeld, BrokerStatePledged, ReasonUnpledged+" — Held: broker holds 10 of 10 (10 free)"
	ures, err := a.Unpledge(ctx, v)
	if err != nil {
		t.Fatalf("unpledge: %v", err)
	}
	if err := trading.QueryRow(`SELECT status, COALESCE(protective_last_status,''), COALESCE(protective_last_reason,'')
		FROM manthan_positions WHERE strategy_id=$1 AND symbol='AAA'`, m2Strat).Scan(&tStatus, &tLastStatus, &tReason); err != nil {
		t.Fatalf("trading row: %v", err)
	}
	if tStatus != "ACTIVE" || tLastStatus != "UNPLEDGED" || tReason != ReasonUnpledged {
		t.Fatalf("trading_db not reactivated: %s %s %q", tStatus, tLastStatus, tReason)
	}
	if err := execDB.QueryRow(`SELECT status, COALESCE(broker_status,'') FROM manthan_orders WHERE signal_id='tadm-m2-sl-aaa'`).Scan(&parkedStatus, &parkedTag); err != nil {
		t.Fatalf("stop row: %v", err)
	}
	if parkedStatus != "CANCELLED" || parkedTag != "UNPLEDGED" || ures["stop_rows_unparked"].(int64) != 1 {
		t.Fatalf("stop row not released: %s/%s %v", parkedStatus, parkedTag, ures)
	}
	_ = execDB.QueryRow(`SELECT COUNT(*) FROM manthan_order_events e JOIN manthan_orders o ON o.id=e.order_id
		WHERE o.user_id=$1 AND e.event_type='POSITION_UNPLEDGED'`, m2User).Scan(&events)
	if events != 1 {
		t.Fatalf("expected one POSITION_UNPLEDGED event, got %d", events)
	}
}

// A lot with NO stop row gets a marker row so trade-execution's queries see
// it as covered + PLEDGED; a stop row whose broker order is live is left.
func TestBookActions_PledgeMarkerAndLiveOrder(t *testing.T) {
	a, trading, execDB, _ := newBookActionsEnv(t, &m7Broker{})
	ctx := context.Background()

	// CCC has no stop row at all → marker row.
	seedTradingPosition(t, trading, "CCC", "ACTIVE")
	v := pledgedVerdict("")
	v.Symbol, v.PositionIDs = "CCC", []string{"aaaaaaaa-1111-2222-3333-00000000ccc1"}
	res, err := a.Pledge(ctx, v, nil)
	if err != nil {
		t.Fatalf("pledge CCC: %v", err)
	}
	if _, ok := res["marker_row"]; !ok {
		t.Fatalf("expected a marker row: %v", res)
	}
	var markerStatus, markerTag, sig string
	if err := execDB.QueryRow(`SELECT status, broker_status, signal_id FROM manthan_orders WHERE id = $1`, res["marker_row"]).Scan(&markerStatus, &markerTag, &sig); err != nil {
		t.Fatalf("marker: %v", err)
	}
	if markerStatus != "SL_DEFERRED_BAND" || markerTag != "PLEDGED" || !strings.HasPrefix(sig, "pledged-"+m2User) {
		t.Fatalf("marker wrong: %s %s %s", markerStatus, markerTag, sig)
	}
	// Re-pledge after an unpledge re-uses the marker (ON CONFLICT).
	if _, err := a.Unpledge(ctx, v); err != nil {
		t.Fatalf("unpledge CCC: %v", err)
	}
	res, err = a.Pledge(ctx, v, nil)
	if err != nil {
		t.Fatalf("re-pledge CCC: %v", err)
	}
	var markers int
	_ = execDB.QueryRow(`SELECT COUNT(*) FROM manthan_orders WHERE signal_id = $1`, sig).Scan(&markers)
	if markers != 1 {
		t.Fatalf("marker duplicated: %d", markers)
	}

	// AAA's stop row carries a LIVE broker order → never parked, reported.
	seedTradingPosition(t, trading, "AAA", "ACTIVE")
	mustExec(t, execDB, `UPDATE manthan_orders SET broker_order_id='BRK-LIVE-1' WHERE signal_id='tadm-m2-sl-aaa'`)
	va := pledgedVerdict("")
	res, err = a.Pledge(ctx, va, map[string]bool{"BRK-LIVE-1": true})
	if err != nil {
		t.Fatalf("pledge AAA: %v", err)
	}
	left, _ := res["stop_rows_left_live_at_broker"].([]string)
	if res["stop_rows_parked"].(int64) != 0 || len(left) != 1 || left[0] != "BRK-LIVE-1" {
		t.Fatalf("live order must be left alone: %v", res)
	}
	var st string
	_ = execDB.QueryRow(`SELECT status FROM manthan_orders WHERE signal_id='tadm-m2-sl-aaa'`).Scan(&st)
	if st != "SL_PLACED" {
		t.Fatalf("live stop row was touched: %s", st)
	}
}

// GhostHeal writes the reconciler's reason and verdict on the book row.
func TestBookActions_HealWritesReconcilerReason(t *testing.T) {
	a, trading, execDB, pos := newBookActionsEnv(t, &m7Broker{})
	ctx := context.Background()
	seedTradingPosition(t, trading, "CCC", "PLEDGED") // a pledged lot that vanished is closed too

	plan := &GhostPlan{UserID: m2User, Symbol: "CCC", BookRows: 1, BookQty: 10, StrategyID: m2Strat,
		ExitReason: ExitReasonNotHeldAtBroker, liveOrderIDs: map[string]bool{}}
	if _, err := a.GhostHeal(ctx, plan); err != nil {
		t.Fatalf("heal: %v", err)
	}
	var status, reason, bstate, breason string
	var price sql.NullFloat64
	if err := pos.QueryRow(`SELECT status, exit_reason, exit_price, broker_state, broker_state_reason FROM positions
		WHERE user_id=$1 AND symbol='CCC'`, m2User).Scan(&status, &reason, &price, &bstate, &breason); err != nil {
		t.Fatalf("book row: %v", err)
	}
	if status != "EXITED" || reason != ExitReasonNotHeldAtBroker || price.Valid || bstate != BrokerStateNotHeld || breason != ReasonNotHeldClosed {
		t.Fatalf("book row after heal: %s %s %v %s %q", status, reason, price.Valid, bstate, breason)
	}
	var tStatus, tReason string
	_ = trading.QueryRow(`SELECT status, COALESCE(exit_reason,'') FROM manthan_positions WHERE strategy_id=$1 AND symbol='CCC'`, m2Strat).Scan(&tStatus, &tReason)
	if tStatus != "EXITED" || tReason != ExitReasonNotHeldAtBroker {
		t.Fatalf("trading row after heal: %s %s", tStatus, tReason)
	}
	_ = execDB // ledger net for CCC is 0 in the fixtures → no FIX-5 row expected

	// MANUAL_EXIT at the tradebook price for a sold-today lot.
	plan2 := &GhostPlan{UserID: m2User, Symbol: "BBB", BookRows: 1, BookQty: 10, StrategyID: m2Strat,
		ExitReason: ExitReasonManualExit, ExitPrice: 123.45, liveOrderIDs: map[string]bool{}}
	plan2.Evidence.TradebookSell = true
	if _, err := a.GhostHeal(ctx, plan2); err != nil {
		t.Fatalf("heal BBB: %v", err)
	}
	if err := pos.QueryRow(`SELECT status, exit_reason, exit_price, broker_state, broker_state_reason FROM positions
		WHERE user_id=$1 AND symbol='BBB'`, m2User).Scan(&status, &reason, &price, &bstate, &breason); err != nil {
		t.Fatalf("book row BBB: %v", err)
	}
	if reason != ExitReasonManualExit || !price.Valid || price.Float64 != 123.45 || bstate != BrokerStateSoldToday || breason != ReasonSoldTodayShort {
		t.Fatalf("BBB after heal: %s %v %s %q", reason, price, bstate, breason)
	}
}

// Reconcile judges book lots and ledger-only lots from ONE broker read.
func TestReconcile_VerdictsFromSnapshot(t *testing.T) {
	trading, execDB, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, execDB, pos, time.Now())
	fleet := NewFleetStore(trading, execDB, pos)
	// Ledger-only lot: a filled BUY of ZZZ with no book row.
	mustExec(t, execDB, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, broker_order_id, filled_at, created_at)
		VALUES ('tadm-m2-buy-zzz', $1, $2, 'ZZZ', 'LIMIT_BUY', 'BUY', 5, 5, 'FILLED', 'NB-ZZZ', now() - interval '10 days', now() - interval '10 days')`, m2Strat, m2User)

	broker := stubBroker{
		holdings: []indiraClient.Holding{
			nseHolding("AAA", 10),
			{Symbol: []indiraClient.HoldingSymbol{{Exc: "NSE", DispSym: "BBB"}}, Qty: 10, FreeQty: 0, PledgeQty: 10},
		},
		// CCC hidden under a standing SELL (ours).
		orderbook: []indiraClient.OrderBook{{OrdId: "SL-CCC", OrdAction: "SELL", Status: "Pending", Cancellable: true, Qty: 10, RemainQty: 10,
			Symbol: indiraClient.OrderBookSymbol{Exc: "NSE", DispSym: "CCC"}}},
	}
	rs := NewReconStore(userKeyedCreds{m2User: credsOK(m2User)}, broker, fleet)
	res, err := rs.Reconcile(context.Background(), m2User)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.BrokerLeg != "OK" || res.Snapshot() == nil {
		t.Fatalf("broker leg: %s", res.BrokerLeg)
	}
	got := map[string]Verdict{}
	for _, v := range res.Verdicts {
		got[v.Symbol] = v
	}
	if got["AAA"].State != BrokerStateHeld || got["BBB"].State != BrokerStatePledged || got["CCC"].State != BrokerStateHeld {
		t.Fatalf("book verdicts: AAA=%s BBB=%s CCC=%s", got["AAA"].State, got["BBB"].State, got["CCC"].State)
	}
	if z, ok := got["ZZZ"]; !ok || !z.LedgerOnly || z.State != BrokerStateNotHeld || z.BookQty != 5 {
		t.Fatalf("ledger-only verdict: %+v", z)
	}
	if res.VerdictSummary[BrokerStateHeld] != 2 || res.VerdictSummary[BrokerStatePledged] != 1 || res.VerdictSummary[BrokerStateNotHeld] != 1 {
		t.Fatalf("summary: %v", res.VerdictSummary)
	}
	if res.Verdicts[0].Symbol != "ZZZ" { // worst first
		t.Fatalf("sort: %s first", res.Verdicts[0].Symbol)
	}
	// prev_state round-trips once persisted.
	a := NewActions(fleet, userKeyedCreds{m2User: credsOK(m2User)}, &m7Broker{stubBroker: broker},
		NewStrategyControl(&stubStrategyRPC{}, fleet), nil, "http://127.0.0.1:1", "", "", nil)
	if _, err := a.PersistVerdicts(context.Background(), res.Verdicts, VerdictSourceSweep); err != nil {
		t.Fatalf("persist: %v", err)
	}
	res2, err := rs.Reconcile(context.Background(), m2User)
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	for _, v := range res2.Verdicts {
		if v.Symbol == "BBB" && v.PrevState != BrokerStatePledged {
			t.Fatalf("prev_state not read back: %+v", v)
		}
	}
}
