package admin

// M5 tests. Pure logic (ThreeWay, holdingsTotals, evaluateTrigger) is
// hermetic; the board and reconcile run against the real local DBs with
// the shared fleet fixtures; broker + LTP are stubs.

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

// ── stubs ───────────────────────────────────────────────────────────────

// userKeyedCreds answers per-user: absent user → NO_CREDENTIAL shape.
type userKeyedCreds map[string]*pb.GetUserCredentialsResponse

func (m userKeyedCreds) GetUserCredentials(_ context.Context, req *pb.GetUserCredentialsRequest) (*pb.GetUserCredentialsResponse, error) {
	if r, ok := m[req.UserId]; ok {
		return r, nil
	}
	return &pb.GetUserCredentialsResponse{Success: false}, nil
}

// stubBroker serves canned holdings; orderbook/trades are static.
type stubBroker struct {
	holdings []indiraClient.Holding
	err      error
}

func (b stubBroker) GetHoldings(context.Context, *indiraClient.AuthContext) ([]indiraClient.Holding, error) {
	return b.holdings, b.err
}
func (b stubBroker) GetOrderBookRaw(context.Context, *indiraClient.AuthContext) ([]byte, error) {
	return []byte(`[{"ordId":"OB1"}]`), b.err
}
func (b stubBroker) GetTradeBook(context.Context, *indiraClient.AuthContext, ...string) ([]indiraClient.TradeBook, error) {
	return []indiraClient.TradeBook{{OrdId: "TB1"}}, b.err
}

func nseHolding(sym string, qty int) indiraClient.Holding {
	return indiraClient.Holding{
		Symbol: []indiraClient.HoldingSymbol{{Exc: "NSE", DispSym: sym}},
		Qty:    qty,
	}
}

// stubLTP serves fixed quotes by token.
type stubLTP struct {
	quotes map[string]livealgos.LTPQuote
	status livealgos.Status
}

func (s stubLTP) FetchByTokens(_ context.Context, tokens []string) (map[string]livealgos.LTPQuote, livealgos.Status) {
	return s.quotes, s.status
}

// ── pure logic ──────────────────────────────────────────────────────────

func TestM5_ThreeWay_Classes(t *testing.T) {
	now := time.Now()
	old, fresh := now.Add(-30*24*time.Hour), now.Add(-24*time.Hour)
	active := []BookLot{
		{Symbol: "CLEAN", Qty: 10, Rows: 1, EntryTime: old},
		{Symbol: "GHOSTOLD", Qty: 5, Rows: 1, EntryTime: old},
		{Symbol: "GHOSTNEW", Qty: 5, Rows: 1, EntryTime: fresh},
		{Symbol: "QTYOFF", Qty: 10, Rows: 1, EntryTime: old},
		{Symbol: "DUPES", Qty: 170, Rows: 17, EntryTime: old},
	}
	broker := map[string]int{"CLEAN": 10, "QTYOFF": 7, "DUPES": 170, "MANUAL": 3}

	got := ThreeWay(active, []string{"STALESL"}, broker, true, now)

	byKey := map[string]Mismatch{}
	for _, m := range got {
		byKey[m.Class+"/"+m.Symbol] = m
	}
	if len(got) != 6 { // 2 ghosts + qty + dupes + unknown + unledgered
		t.Fatalf("want 6 mismatches, got %d: %+v", len(got), got)
	}
	if m := byKey["GHOST/GHOSTOLD"]; m.BookQty != 5 || m.Caveat != "" {
		t.Fatalf("old ghost must have NO settlement caveat: %+v", m)
	}
	if m := byKey["GHOST/GHOSTNEW"]; !strings.Contains(m.Caveat, "settlement") {
		t.Fatalf("fresh ghost must carry the settlement caveat: %+v", m)
	}
	if m := byKey["QTY_MISMATCH/QTYOFF"]; m.BookQty != 10 || m.BrokerQty != 7 {
		t.Fatalf("qty mismatch: %+v", m)
	}
	if m := byKey["DUPLICATE_BOOK_ROWS/DUPES"]; m.BookRows != 17 {
		t.Fatalf("duplicate rows: %+v", m)
	}
	if m := byKey["UNKNOWN_HOLDING/MANUAL"]; m.BrokerQty != 3 {
		t.Fatalf("unknown holding: %+v", m)
	}
	if _, ok := byKey["UNLEDGERED_EXIT/STALESL"]; !ok {
		t.Fatalf("missing unledgered exit: %+v", got)
	}

	// Broker leg down: only ledger-side classes may fire.
	down := ThreeWay(active, []string{"STALESL"}, nil, false, now)
	for _, m := range down {
		if m.Class != "DUPLICATE_BOOK_ROWS" && m.Class != "UNLEDGERED_EXIT" {
			t.Fatalf("broker-leg-down produced broker-side class %s", m.Class)
		}
	}
	if len(down) != 2 {
		t.Fatalf("broker-leg-down want 2, got %+v", down)
	}
}

func TestM5_HoldingsTotals(t *testing.T) {
	h := []indiraClient.Holding{
		nseHolding("PLAIN", 10),
		{Symbol: []indiraClient.HoldingSymbol{{Exc: "NSE", DispSym: "BUCKETS"}},
			HoldingQty: 3, UsedQty: 2, BTST: 1, PledgeQty: 4}, // Qty=0 → sum buckets
		{Symbol: []indiraClient.HoldingSymbol{{Exc: "BSE", DispSym: "BSEONLY"}}, Qty: 7},
	}
	got := holdingsTotals(h)
	if got["PLAIN"] != 10 || got["BUCKETS"] != 10 || got["BSEONLY"] != 7 {
		t.Fatalf("totals: %+v", got)
	}
}

func TestM5_EvaluateTrigger(t *testing.T) {
	if msg := evaluateTrigger(95, 100); msg != "" {
		t.Fatalf("in-band trigger flagged: %s", msg)
	}
	if msg := evaluateTrigger(102, 100); !strings.Contains(msg, "AT/ABOVE") {
		t.Fatalf("above-market trigger not flagged: %q", msg)
	}
	if msg := evaluateTrigger(30, 100); !strings.Contains(msg, "scale break") {
		t.Fatalf("far-below trigger not flagged: %q", msg)
	}
	if msg := evaluateTrigger(0, 100); msg != "" {
		t.Fatalf("zero trigger must not flag: %q", msg)
	}
	if msg := evaluateTrigger(95, 0); msg != "" {
		t.Fatalf("zero LTP must not flag: %q", msg)
	}
}

// ── DB-backed board ─────────────────────────────────────────────────────

// seedM5 extends the fleet fixtures: trail state, triggers/tokens on the
// SL rows, an AMO_PENDING symbol, and an ACTIVE position for the give-up
// symbol so CAPPED can appear on the board.
func seedM5(t *testing.T, trading, exec, pos *sql.DB, now time.Time) {
	t.Helper()
	cleanupM5 := func() {
		_, _ = trading.Exec(`DELETE FROM manthan_positions WHERE strategy_id = $1::uuid`, m2Strat)
	}
	cleanupM5()
	t.Cleanup(cleanupM5)

	// Triggers + token on the fixtures' SL rows.
	mustExec(t, exec, `UPDATE manthan_orders SET trigger_price=95, broker_trigger_price=94.5, exchange_token='990001'
		WHERE user_id=$1 AND symbol='AAA' AND status='SL_PLACED'`, m2User)
	mustExec(t, exec, `UPDATE manthan_orders SET trigger_price=88 WHERE user_id=$1 AND symbol='BBB' AND status='SL_DEFERRED_BAND'`, m2User)

	// HHH: overnight AMO queue row + ACTIVE position.
	mustExec(t, pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			status, entry_price, entry_time, quantity, invested_amount)
		VALUES ('aaaaaaaa-1111-2222-3333-000000000801', 'MANTHAN', $1, $2, 'aaaaaaaa-1111-4444-3333-000000000801', 'HHH', 'NSE',
			'ACTIVE', 200, $3, 5, 1000)`, m2User, m2Strat, now.Add(-20*time.Hour))
	mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, trigger_price, created_at)
		VALUES ('tadm-m5-hhh', $1, $2, 'HHH', 'SL_SELL_AMO', 'SELL', 5, 0, 'AMO_PENDING', 160, now())`, m2Strat, m2User)

	// FFF: fixtures already seeded 6 failed overnight attempts; give it an
	// ACTIVE position so the give-up shows as a CAPPED board row.
	mustExec(t, pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			status, entry_price, entry_time, quantity, invested_amount)
		VALUES ('aaaaaaaa-1111-2222-3333-000000000601', 'MANTHAN', $1, $2, 'aaaaaaaa-1111-4444-3333-000000000601', 'FFF', 'NSE',
			'ACTIVE', 50, $3, 20, 1000)`, m2User, m2Strat, now.Add(-30*24*time.Hour))

	// A manual row with NULL strategy_id — the live prod shape that
	// crashed the first board deploy. Must render, as NAKED.
	mustExec(t, pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			status, entry_price, entry_time, quantity, invested_amount)
		VALUES ('aaaaaaaa-1111-2222-3333-000000000901', 'USER_MANUAL', $1, NULL, NULL, 'MANUALPOS', 'NSE',
			'ACTIVE', 10, $2, 100, 1000)`, m2User, now.Add(-90*24*time.Hour))

	// Trail state for AAA (armed) and CCC (naked): the manthan book.
	mustExec(t, trading, `INSERT INTO manthan_positions (strategy_id, user_id, symbol, entry_price, quantity, invested_amt,
			high_since_entry, current_sl, status, entry_time, industry, mcap_bucket, index_name)
		VALUES ($1::uuid, $2, 'AAA', 100, 10, 1000, 120, 96, 'ACTIVE', $3, 'Test', 'SMALL', 'NIFTY500'),
		       ($1::uuid, $2, 'CCC', 80, 10, 800, 85, 68, 'ACTIVE', $3, 'Test', 'SMALL', 'NIFTY500')`,
		m2Strat, m2User, now.Add(-48*time.Hour))
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %.60s...: %v", q, err)
	}
}

func m5Rows(t *testing.T, board *ProtectionBoard) map[string]ProtectionRow {
	t.Helper()
	// The board is fleet-wide; assertions look only at the fixture user.
	out := map[string]ProtectionRow{}
	for _, r := range board.Rows {
		if r.UserID == m2User {
			out[r.Symbol] = r
		}
	}
	return out
}

func TestM5_Board_States(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)
	seedM5(t, trading, exec, pos, now)

	fleet := NewFleetStore(trading, exec, pos)
	prot := NewProtectionStore(fleet, stubLTP{
		status: livealgos.StatusHealthy,
		quotes: map[string]livealgos.LTPQuote{"990001": {Token: "990001", LTP: 100}},
	})
	board, err := prot.Board(context.Background())
	if err != nil {
		t.Fatalf("board: %v", err)
	}
	rows := m5Rows(t, board)

	want := map[string]string{
		"AAA": "ARMED", "BBB": "DEFERRED_BAND", "CCC": "NAKED",
		"FFF": "CAPPED", "HHH": "AMO_PENDING", "MANUALPOS": "NAKED",
	}
	for sym, state := range want {
		r, ok := rows[sym]
		if !ok {
			t.Fatalf("board missing %s (rows: %+v)", sym, rows)
		}
		if r.State != state {
			t.Fatalf("%s state = %s, want %s", sym, r.State, state)
		}
	}
	if r := rows["AAA"]; r.BrokerTrigger != 94.5 || r.EntryPrice != 100 || r.HighSinceEntry != 120 || r.CurrentSL != 96 {
		t.Fatalf("AAA context: %+v", r)
	}
	if r := rows["AAA"]; r.LTP != 100 || r.DistanceToTriggerPct < 5.4 || r.DistanceToTriggerPct > 5.6 {
		t.Fatalf("AAA distance: LTP=%v dist=%v", r.LTP, r.DistanceToTriggerPct)
	}
	if r := rows["BBB"]; r.IntendedTrigger != 88 {
		t.Fatalf("BBB intended trigger: %+v", r)
	}
	if r := rows["FFF"]; r.FailedAttempts < 5 {
		t.Fatalf("FFF failed attempts: %+v", r)
	}
	if board.LTPStatus != string(livealgos.StatusHealthy) {
		t.Fatalf("ltp status: %s", board.LTPStatus)
	}
	// Worst-first ordering: the fixture user's first-seen state must not
	// rank better than any later one.
	last := -1
	for _, r := range board.Rows {
		if rank := stateRank(r.State); rank < last {
			t.Fatalf("board not sorted worst-first")
		} else {
			last = rank
		}
	}
}

func TestM5_Board_NoLTPFeed(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)
	seedM5(t, trading, exec, pos, now)

	prot := NewProtectionStore(NewFleetStore(trading, exec, pos), nil)
	board, err := prot.Board(context.Background())
	if err != nil {
		t.Fatalf("board: %v", err)
	}
	if board.LTPStatus != string(livealgos.StatusUnavailable) {
		t.Fatalf("no feed must report UNAVAILABLE, got %s", board.LTPStatus)
	}
	for _, r := range board.Rows {
		if r.LTP != 0 || r.DistanceToTriggerPct != 0 {
			t.Fatalf("no feed must not invent distance: %+v", r)
		}
	}
}

// ── reconcile + scale check + HTTP ──────────────────────────────────────

func TestM5_Reconcile_EndToEnd(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)
	seedM5(t, trading, exec, pos, now)
	// DDD is EXITED in fixtures; give it a still-open SL row → stale stop.
	mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, created_at)
		VALUES ('tadm-m5-ddd-stale', $1, $2, 'DDD', 'SL_SELL', 'SELL', 10, 0, 'SL_PLACED', now())`, m2Strat, m2User)

	fleet := NewFleetStore(trading, exec, pos)
	// Broker: AAA clean, BBB qty off, CCC+HHH+FFF absent (ghosts), ZZZ manual.
	rs := NewReconStore(
		userKeyedCreds{m2User: credsOK(m2User)},
		stubBroker{holdings: []indiraClient.Holding{
			nseHolding("AAA", 10), nseHolding("BBB", 5), nseHolding("ZZZ", 7),
		}},
		fleet,
	)
	res, err := rs.Reconcile(context.Background(), m2User)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.BrokerLeg != "OK" {
		t.Fatalf("broker leg: %+v", res)
	}
	classes := map[string]int{}
	bySym := map[string]Mismatch{}
	for _, m := range res.Mismatches {
		classes[m.Class]++
		bySym[m.Class+"/"+m.Symbol] = m
	}
	if classes["GHOST"] != 4 { // CCC, FFF, HHH, MANUALPOS
		t.Fatalf("ghosts: %+v", res.Mismatches)
	}
	// HHH entered 20h ago → settlement caveat; FFF entered 30d ago → none.
	if m := bySym["GHOST/HHH"]; !strings.Contains(m.Caveat, "settlement") {
		t.Fatalf("HHH ghost caveat missing: %+v", m)
	}
	if m := bySym["GHOST/FFF"]; m.Caveat != "" {
		t.Fatalf("FFF (old) ghost must have no caveat: %+v", m)
	}
	if classes["QTY_MISMATCH"] != 1 || classes["UNKNOWN_HOLDING"] != 1 || classes["UNLEDGERED_EXIT"] != 1 {
		t.Fatalf("classes: %+v", classes)
	}
	if _, ok := bySym["UNLEDGERED_EXIT/DDD"]; !ok {
		t.Fatalf("DDD stale stop missing: %+v", res.Mismatches)
	}
	if res.CleanSymbols != 1 { // AAA
		t.Fatalf("clean symbols = %d, want 1", res.CleanSymbols)
	}

	// Dead credential: DB legs still render, broker classes suppressed.
	rsDead := NewReconStore(userKeyedCreds{}, stubBroker{}, fleet)
	resDead, err := rsDead.Reconcile(context.Background(), m2User)
	if err != nil {
		t.Fatalf("reconcile dead: %v", err)
	}
	if resDead.BrokerLeg != "NO_CREDENTIAL" {
		t.Fatalf("dead leg: %+v", resDead)
	}
	for _, m := range resDead.Mismatches {
		if m.Class == "GHOST" || m.Class == "QTY_MISMATCH" || m.Class == "UNKNOWN_HOLDING" {
			t.Fatalf("broker-side class %s fired without a broker leg", m.Class)
		}
	}
}

func TestM5_ScaleCheck_And_HTTP(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)
	seedM5(t, trading, exec, pos, now)
	// Make CCC's trail trigger sit ABOVE market: token + quote at 50 while
	// current_sl=68 (seeded) → TRIGGER_SCALE. CCC has no open SL row, so
	// hang the token on a deferred row to reach the LTP pass… simpler: give
	// CCC a deferred SL row carrying the token.
	mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, trigger_price, exchange_token, created_at)
		VALUES ('tadm-m5-ccc-band', $1, $2, 'CCC', 'SL_SELL_AMO', 'SELL', 10, 0, 'SL_DEFERRED_BAND', 68, '990002', now())`, m2Strat, m2User)

	fleet := NewFleetStore(trading, exec, pos)
	prot := NewProtectionStore(fleet, stubLTP{
		status: livealgos.StatusHealthy,
		quotes: map[string]livealgos.LTPQuote{
			"990001": {Token: "990001", LTP: 100}, // AAA in band (sl 96… ratio .96 ok? sl=96 LTP=100 → 0.96 in band)
			"990002": {Token: "990002", LTP: 50},  // CCC sl 68 → ratio 1.36 → flag
		},
	})
	// Broker: AAA qty 99 ≠ book 10 → QTY_DRIFT; others match or absent.
	recon := NewReconStore(
		userKeyedCreds{m2User: credsOK(m2User)},
		stubBroker{holdings: []indiraClient.Holding{
			nseHolding("AAA", 99), nseHolding("BBB", 10), nseHolding("CCC", 10),
			nseHolding("FFF", 20), nseHolding("HHH", 5),
		}},
		fleet,
	)
	sc := NewScaleChecker(prot, recon)
	findings, err := sc.Run(context.Background())
	if err != nil {
		t.Fatalf("scale run: %v", err)
	}
	mine := map[string]ScaleFinding{}
	for _, f := range findings {
		if f.UserID == m2User {
			mine[f.Kind+"/"+f.Symbol] = f
		}
	}
	if f, ok := mine["TRIGGER_SCALE/CCC"]; !ok || f.CurrentSL != 68 || f.LTP != 50 {
		t.Fatalf("CCC trigger-scale missing/wrong: %+v", mine)
	}
	if f, ok := mine["QTY_DRIFT/AAA"]; !ok || f.BookQty != 10 || f.BrokerQty != 99 {
		t.Fatalf("AAA qty-drift missing/wrong: %+v", mine)
	}
	if _, bad := mine["TRIGGER_SCALE/AAA"]; bad {
		t.Fatalf("AAA in-band trigger wrongly flagged")
	}

	// HTTP: board + scale-check + mirror + reconcile, all through the
	// tier/audit chain; findings surface on the attention queue.
	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M5ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetFleetStore(fleet)
	h.SetProtection(prot)
	h.SetRecon(recon)
	h.SetScaleChecker(sc)
	r := newRouterFor(t, h)
	token := elevateViaHTTP(t, r, "TADM_M5ADMIN")

	rec := m4do(t, r, token, "GET", "/api/v1/admin/protection", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"NAKED"`) {
		t.Fatalf("protection: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "POST", "/api/v1/admin/scale-check", "{}")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "QTY_DRIFT") {
		t.Fatalf("scale-check: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/users/"+m2User+"/reconcile", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"broker_leg":"OK"`) {
		t.Fatalf("reconcile http: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/users/"+m2User+"/broker/holdings", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "freeQty=0") {
		t.Fatalf("mirror holdings must carry the warning: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/users/"+m2User+"/broker/orderbook", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "OB1") {
		t.Fatalf("mirror orderbook: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/attention", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "SCALE_QTY_DRIFT") {
		t.Fatalf("attention must carry scale findings: %d %.300s", rec.Code, rec.Body.String())
	}

	// Mirror for a user with no credential: 200 with the leg named, no data.
	rec = m4do(t, r, token, "GET", "/api/v1/admin/users/NOBODY/broker/holdings", "")
	var mirrorResp struct {
		Data MirrorResult `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &mirrorResp); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("mirror nobody: %d %v", rec.Code, err)
	}
	if mirrorResp.Data.BrokerLeg != "NO_CREDENTIAL" || len(mirrorResp.Data.Data) != 0 {
		t.Fatalf("mirror nobody leg: %+v", mirrorResp.Data)
	}
}

