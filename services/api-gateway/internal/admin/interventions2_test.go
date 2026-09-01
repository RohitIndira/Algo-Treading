package admin

// M7 A2/B tests — guardrails first: the SL-cancel refusal, the vanished-
// order no-escalation verdict, ghost evidence gates, TYPED handshakes,
// and the square-off proxy shape. Broker + trade-execution + the
// rebalancer CLI are all stubs.

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// m7Broker implements actionBroker with canned data + call recording.
type m7Broker struct {
	stubBroker
	orderbook []indiraClient.OrderBook
	obErr     error
	cancelled []indiraClient.CancelOrderRequest
}

func (b *m7Broker) GetOrderBook(context.Context, *indiraClient.AuthContext) ([]indiraClient.OrderBook, error) {
	return b.orderbook, b.obErr
}
func (b *m7Broker) CancelOrder(_ context.Context, _ *indiraClient.AuthContext, req *indiraClient.CancelOrderRequest) error {
	b.cancelled = append(b.cancelled, *req)
	return nil
}

type m7Env struct {
	root    http.Handler
	token   string
	trading *sql.DB
	exec    *sql.DB
	pos     *sql.DB
	broker  *m7Broker
	actions *Actions
	runs    *[][]string
}

func newM7BEnv(t *testing.T, broker *m7Broker, teURL string) *m7Env {
	t.Helper()
	trading, execDB, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, execDB, pos, time.Now())

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M7B", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	fleet := NewFleetStore(trading, execDB, pos)
	h.SetFleetStore(fleet)

	runs := &[][]string{}
	a := NewActions(fleet, userKeyedCreds{m2User: credsOK(m2User)}, broker,
		NewStrategyControl(&stubStrategyRPC{}, fleet), nil, teURL, "/fake/rebalancer", "/tmp",
		[]string{"REDIS_ADDR=localhost:6389", "REDIS_URI=localhost:6389"})
	a.run = func(_ context.Context, dir string, env []string, bin string, args ...string) ([]byte, error) {
		rec := append([]string{bin}, args...)
		rec = append(rec, env...) // env travels with the record for assertions
		*runs = append(*runs, rec)
		return []byte("PLAN: topup FOO +10 | 1 entry | total ₹18900"), nil
	}
	h.SetActions(a)
	r := newRouterFor(t, h)
	return &m7Env{root: r, token: elevateViaHTTP(t, r, "TADM_M7B"),
		trading: trading, exec: execDB, pos: pos, broker: broker, actions: a, runs: runs}
}

func TestM7B_OrderViewAndCancel(t *testing.T) {
	broker := &m7Broker{orderbook: []indiraClient.OrderBook{
		{OrdId: "BRK-AMO-1", Status: "Requested", Cancellable: true,
			Symbol: indiraClient.OrderBookSymbol{Symbol: "STK_AAA_EQ_NSE_1", Exc: "NSE", DispSym: "AAA"}},
		{OrdId: "BRK-DONE-1", Status: "Executed", Cancellable: false,
			Symbol: indiraClient.OrderBookSymbol{Symbol: "STK_BBB_EQ_NSE_2", Exc: "NSE", DispSym: "BBB"}},
	}}
	env := newM7BEnv(t, broker, "http://127.0.0.1:1")

	// Ledger rows: an SL (refused) and an AMO-queue entry (cancellable).
	mustExec(t, env.exec, `UPDATE manthan_orders SET broker_order_id='BRK-SL-1' WHERE user_id=$1 AND symbol='AAA' AND status='SL_PLACED'`, m2User)
	mustExec(t, env.exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, broker_order_id, created_at)
		VALUES ('tadm-m7b-amo', $1, $2, 'AAA', 'AMO_SELL', 'SELL', 10, 0, 'PLACED', 'BRK-AMO-1', now())`, m2Strat, m2User)

	// View: live broker order.
	rec := m4do(t, env.root, env.token, "GET", "/api/v1/admin/users/"+m2User+"/orders/BRK-AMO-1", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"verdict":"LIVE"`) {
		t.Fatalf("view live: %d %.300s", rec.Code, rec.Body.String())
	}
	// View: vanished.
	rec = m4do(t, env.root, env.token, "GET", "/api/v1/admin/users/"+m2User+"/orders/NOPE-1", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"verdict":"VANISHED"`) {
		t.Fatalf("view vanished: %d %.300s", rec.Code, rec.Body.String())
	}

	// Cancel an SL-family ledger order → the June refusal, broker untouched.
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/users/"+m2User+"/orders/BRK-SL-1/cancel",
		`{"confirmed":true,"reason":"probe"}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "June-liquidation") {
		t.Fatalf("SL cancel not refused: %d %s", rec.Code, rec.Body.String())
	}
	if len(broker.cancelled) != 0 {
		t.Fatalf("broker touched on refused cancel: %+v", broker.cancelled)
	}
	// Cancel a vanished order → no-escalation verdict.
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/users/"+m2User+"/orders/NOPE-1/cancel",
		`{"confirmed":true,"reason":"probe"}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "NOTHING to escalate") {
		t.Fatalf("vanished cancel: %d %s", rec.Code, rec.Body.String())
	}
	// Cancel a terminal order → refused with broker status.
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/users/"+m2User+"/orders/BRK-DONE-1/cancel",
		`{"confirmed":true,"reason":"probe"}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "not cancellable") {
		t.Fatalf("terminal cancel: %d %s", rec.Code, rec.Body.String())
	}
	// Happy path: the AMO-queue order cancels, request carries symbol+exc.
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/users/"+m2User+"/orders/BRK-AMO-1/cancel",
		`{"confirmed":true,"reason":"stuck AMO row"}`)
	if rec.Code != 200 {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body.String())
	}
	if len(broker.cancelled) != 1 || broker.cancelled[0].OrdId != "BRK-AMO-1" ||
		broker.cancelled[0].Symbol != "STK_AAA_EQ_NSE_1" || broker.cancelled[0].Exc != "NSE" {
		t.Fatalf("cancel request shape: %+v", broker.cancelled)
	}
}

func TestM7B_GhostPreviewAndHeal(t *testing.T) {
	// Broker holds BBB (not a ghost) and nothing of GHOSTY.
	broker := &m7Broker{stubBroker: stubBroker{holdings: []indiraClient.Holding{nseHolding("BBB", 10)}}}
	env := newM7BEnv(t, broker, "http://127.0.0.1:1")

	// GHOSTY: 2 ACTIVE book rows entered 30 days ago + a FILLED BUY ledger.
	for i, id := range []string{"aaaaaaaa-1111-2222-3333-000000000g01", "aaaaaaaa-1111-2222-3333-000000000g02"} {
		id = strings.Replace(id, "g", fmt.Sprint(7+i), 1)
		mustExec(t, env.pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
				status, entry_price, entry_time, quantity, invested_amount)
			VALUES ($1, 'MANTHAN', $2, $3, $4, 'GHOSTY', 'NSE', 'ACTIVE', 100, now() - interval '30 days', 5, 500)`,
			id, m2User, m2Strat, strings.Replace(id, "-2222-", "-4444-", 1))
	}
	mustExec(t, env.exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, filled_at, created_at)
		VALUES ('tadm-m7b-ghost-buy', $1, $2, 'GHOSTY', 'LIMIT_BUY', 'BUY', 10, 10, 'FILLED', now() - interval '30 days', now() - interval '30 days')`,
		m2Strat, m2User)

	// Not a ghost (broker holds BBB) → refused.
	rec := m4do(t, env.root, env.token, "GET", "/api/v1/admin/users/"+m2User+"/ghosts/BBB", "")
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "not a ghost") {
		t.Fatalf("held symbol not refused: %d %s", rec.Code, rec.Body.String())
	}

	// Preview GHOSTY: past settlement, holdings absent → plan with text.
	rec = m4do(t, env.root, env.token, "GET", "/api/v1/admin/users/"+m2User+"/ghosts/GHOSTY", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "HEAL GHOST GHOSTY FOR "+m2User) {
		t.Fatalf("ghost preview: %d %.400s", rec.Code, rec.Body.String())
	}
	want := fmt.Sprintf("HEAL GHOST GHOSTY FOR %s — CLOSE 2 BOOK ROWS ×10", m2User)
	if !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("confirmation text: %.400s (want %q)", rec.Body.String(), want)
	}

	healURL := "/api/v1/admin/users/" + m2User + "/ghosts/GHOSTY/heal"
	// Wrong text → 412, nothing written.
	if rec := m4do(t, env.root, env.token, "POST", healURL, `{"confirmation_text":"HEAL WRONG"}`); rec.Code != 412 {
		t.Fatalf("wrong text: %d", rec.Code)
	}
	// Exact text → book rows close + FIX-5 ledger SELL written.
	rec = m4do(t, env.root, env.token, "POST", healURL, fmt.Sprintf(`{"confirmation_text":%q}`, want))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"book_rows_closed":2`) {
		t.Fatalf("heal: %d %s", rec.Code, rec.Body.String())
	}
	var active int
	if err := env.pos.QueryRow(`SELECT COUNT(*) FROM positions WHERE user_id=$1 AND symbol='GHOSTY' AND status='ACTIVE'`, m2User).Scan(&active); err != nil || active != 0 {
		t.Fatalf("book not closed: %v n=%d", err, active)
	}
	var reason string
	if err := env.pos.QueryRow(`SELECT exit_reason FROM positions WHERE user_id=$1 AND symbol='GHOSTY' LIMIT 1`, m2User).Scan(&reason); err != nil || reason != "ADMIN_GHOST_CLEANUP" {
		t.Fatalf("exit reason: %v %q", err, reason)
	}
	var ledgerSells int
	if err := env.exec.QueryRow(`SELECT COUNT(*) FROM manthan_orders WHERE user_id=$1 AND symbol='GHOSTY' AND order_type='MARKET_SELL' AND status='FILLED'`, m2User).Scan(&ledgerSells); err != nil || ledgerSells != 1 {
		t.Fatalf("FIX-5 row missing: %v n=%d", err, ledgerSells)
	}
	// Second heal → 422 (no ACTIVE rows left).
	if rec := m4do(t, env.root, env.token, "POST", healURL, fmt.Sprintf(`{"confirmation_text":%q}`, want)); rec.Code != 422 {
		t.Fatalf("double heal: %d", rec.Code)
	}
}

func TestM7B_GhostOpenSellOrderGate(t *testing.T) {
	// The ALIVUS false-positive: holdings absent (freeQty=0 hides the row)
	// BUT a standing SL SELL sits in the order book → NOT a ghost.
	broker := &m7Broker{orderbook: []indiraClient.OrderBook{
		{OrdId: "BRK-SL-9", Status: "Requested", OrdAction: "SELL", Cancellable: true,
			Symbol: indiraClient.OrderBookSymbol{DispSym: "HIDDENQ", BaseSym: "HIDDENQ", Exc: "NSE"}},
	}}
	env := newM7BEnv(t, broker, "http://127.0.0.1:1")
	mustExec(t, env.pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			status, entry_price, entry_time, quantity, invested_amount)
		VALUES ('aaaaaaaa-1111-2222-3333-000000000d01', 'MANTHAN', $1, $2, 'aaaaaaaa-1111-4444-3333-000000000d01', 'HIDDENQ', 'NSE',
			'ACTIVE', 100, now() - interval '30 days', 14, 1400)`, m2User, m2Strat)
	rec := m4do(t, env.root, env.token, "GET", "/api/v1/admin/users/"+m2User+"/ghosts/HIDDENQ", "")
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "open SELL order BRK-SL-9") {
		t.Fatalf("open-sell ghost not refused: %d %s", rec.Code, rec.Body.String())
	}
}

func TestM7B_GhostSettlementGate(t *testing.T) {
	broker := &m7Broker{} // holds nothing, no tradebook sells
	env := newM7BEnv(t, broker, "http://127.0.0.1:1")
	// Fresh position (20h) with no tradebook evidence → refused.
	mustExec(t, env.pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			status, entry_price, entry_time, quantity, invested_amount)
		VALUES ('aaaaaaaa-1111-2222-3333-000000000f01', 'MANTHAN', $1, $2, 'aaaaaaaa-1111-4444-3333-000000000f01', 'FRESHY', 'NSE',
			'ACTIVE', 100, now() - interval '20 hours', 5, 500)`, m2User, m2Strat)
	rec := m4do(t, env.root, env.token, "GET", "/api/v1/admin/users/"+m2User+"/ghosts/FRESHY", "")
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "settlement window") {
		t.Fatalf("fresh ghost not gated: %d %s", rec.Code, rec.Body.String())
	}
}

func TestM7B_SquareOffTypedProxy(t *testing.T) {
	var teCalls []string
	te := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		teCalls = append(teCalls, r.Method+" "+r.URL.String())
		w.Write([]byte(`{"user_id":"` + r.URL.Query().Get("user_id") + `","qty":10,"mode":"MARKET","cancelled_sls":["B1"],"sell_broker_id":"S1"}`))
	}))
	defer te.Close()
	env := newM7BEnv(t, &m7Broker{}, te.URL)

	base := "/api/v1/admin/strategies/" + m2Strat + "/positions/AAA/squareoff"
	// Preview → blast radius with qty 10 (fixture AAA) and 1 standing stop.
	rec := m4do(t, env.root, env.token, "POST", base, `{"preview":true}`)
	want := fmt.Sprintf("SQUARE OFF AAA ×10 FOR %s — CANCEL 1 STOPS AND SELL AT MARKET VALUE UNKNOWN (LTP FEED DOWN)", m2User)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("squareoff preview: %d %.400s (want %q)", rec.Code, rec.Body.String(), want)
	}
	if len(teCalls) != 0 {
		t.Fatalf("TE touched during preview: %v", teCalls)
	}
	// Wrong text → 412, no TE call.
	if rec := m4do(t, env.root, env.token, "POST", base, `{"confirmation_text":"SQUARE OFF WRONG"}`); rec.Code != 412 {
		t.Fatalf("wrong text: %d", rec.Code)
	}
	if len(teCalls) != 0 {
		t.Fatalf("TE touched on denial: %v", teCalls)
	}
	// Exact text → proxied once, report returned.
	rec = m4do(t, env.root, env.token, "POST", base, fmt.Sprintf(`{"confirmation_text":%q}`, want))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"sell_broker_id":"S1"`) {
		t.Fatalf("squareoff exec: %d %s", rec.Code, rec.Body.String())
	}
	if len(teCalls) != 1 || !strings.Contains(teCalls[0], "user_id="+m2User) || !strings.Contains(teCalls[0], "symbol=AAA") {
		t.Fatalf("proxy shape: %v", teCalls)
	}
	// Unknown symbol → 422, no TE call.
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/strategies/"+m2Strat+"/positions/NOSUCH/squareoff", `{"preview":true}`)
	if rec.Code != 422 {
		t.Fatalf("no position: %d %s", rec.Code, rec.Body.String())
	}
}

func TestM7B_Rebalance(t *testing.T) {
	env := newM7BEnv(t, &m7Broker{}, "http://127.0.0.1:1")

	// Preview: dry-run flag always present.
	rec := m4do(t, env.root, env.token, "POST", "/api/v1/admin/rebalance/preview", fmt.Sprintf(`{"user_id":%q}`, m2User))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "PLAN:") {
		t.Fatalf("preview: %d %s", rec.Code, rec.Body.String())
	}
	if got := (*env.runs)[0]; got[1] != "--dry-run" || got[2] != "--user" || got[3] != m2User {
		t.Fatalf("preview args: %v", got)
	}
	// The child MUST receive the gateway-verified redis (the 30%-fallback bug).
	var redisEnv bool
	for _, e := range (*env.runs)[0] {
		if e == "REDIS_ADDR=localhost:6389" {
			redisEnv = true
		}
	}
	if !redisEnv {
		t.Fatalf("rebalancer env missing REDIS_ADDR: %v", (*env.runs)[0])
	}

	// Trigger without user → 422.
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/rebalance/trigger", `{"confirmation_text":"x"}`)
	if rec.Code != 422 {
		t.Fatalf("no user: %d %s", rec.Code, rec.Body.String())
	}
	// Preview step returns the text; typed exec runs WITHOUT --dry-run.
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/rebalance/trigger", fmt.Sprintf(`{"preview":true,"user_id":%q}`, m2User))
	want := "REBALANCE " + m2User + " — PUBLISH REAL ENTRY ORDERS"
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("trigger preview: %d %s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, env.root, env.token, "POST", "/api/v1/admin/rebalance/trigger",
		fmt.Sprintf(`{"user_id":%q,"confirmation_text":%q}`, m2User, want))
	if rec.Code != 200 {
		t.Fatalf("trigger: %d %s", rec.Code, rec.Body.String())
	}
	last := (*env.runs)[len(*env.runs)-1]
	for _, a := range last {
		if a == "--dry-run" {
			t.Fatalf("trigger ran dry: %v", last)
		}
	}
	if last[1] != "--user" || last[2] != m2User {
		t.Fatalf("trigger args: %v", last)
	}
}
