package admin

// M8–M11 tests — DB-backed with the fleet fixtures; broker/EMA/PM2 stubbed.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

type stubFunds struct{ fl *indiraClient.FundLimit }

func (s stubFunds) GetFundLimit(context.Context, *indiraClient.AuthContext) (*indiraClient.FundLimit, error) {
	return s.fl, nil
}

func TestM8_EODBoard(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)
	// HHH: AMO_PENDING for tomorrow; III: 3 failed overnight attempts.
	mustExec(t, pos, `INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			status, entry_price, entry_time, quantity, invested_amount)
		VALUES ('aaaaaaaa-1111-2222-3333-000000000801', 'MANTHAN', $1, $2, 'aaaaaaaa-1111-4444-3333-000000000801', 'HHH', 'NSE', 'ACTIVE', 200, now(), 5, 1000),
		       ('aaaaaaaa-1111-2222-3333-000000000901', 'MANTHAN', $1, $2, 'aaaaaaaa-1111-4444-3333-000000000902', 'III', 'NSE', 'ACTIVE', 90, now(), 8, 720)`,
		m2User, m2Strat)
	mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, broker_order_id, trade_date, created_at)
		VALUES ('tadm-m8-hhh', $1, $2, 'HHH', 'SL_SELL_AMO', 'SELL', 5, 0, 'AMO_PENDING', 'BRK-M8-H', CURRENT_DATE + 1, now())`, m2Strat, m2User)
	for i := 0; i < 3; i++ {
		mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, last_error, trade_date, created_at)
			VALUES ($3, $1, $2, 'III', 'SL_SELL_AMO', 'SELL', 8, 0, 'CANCELLED', 'DPR rejected', CURRENT_DATE + 1, now())`,
			m2Strat, m2User, fmt.Sprintf("tadm-m8-iii-%d", i))
	}
	// A conversion outcome for AAA this morning: promoted (SL_PLACED on an
	// SL_SELL_AMO row with trade_date=today).
	mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, broker_order_id, trade_date, created_at)
		VALUES ('tadm-m8-aaa-conv', $1, $2, 'AAA', 'SL_SELL_AMO', 'SELL', 10, 0, 'SL_PLACED', 'BRK-M8-CONV', CURRENT_DATE, now() - interval '2 hours')`, m2Strat, m2User)

	// Phase 1: DEAD credential (the fixture seeds it 30h old via INSERT —
	// an UPDATE can't make it stale, the DB trigger auto-bumps updated_at).
	ist, _ := time.LoadLocation("Asia/Kolkata")
	eod := NewEODStore(NewFleetStore(trading, exec, pos), nil, ist)
	deadBoard, err := eod.Board(context.Background())
	if err != nil {
		t.Fatalf("dead board: %v", err)
	}
	dead := map[string]EODRow{}
	for _, r := range deadBoard.Rows {
		if r.UserID == m2User {
			dead[r.Symbol] = r
		}
	}
	if dead["CCC"].State != "USER_BLOCKED" || dead["III"].State != "USER_BLOCKED" {
		t.Fatalf("dead-session escalation: CCC=%+v III=%+v", dead["CCC"], dead["III"])
	}
	var credHint bool
	for _, a := range dead["III"].Actions {
		if strings.Contains(a, "/credential") {
			credHint = true
		}
	}
	if !credHint {
		t.Fatalf("III missing credential hint: %+v", dead["III"].Actions)
	}
	if dead["AAA"].State != "ARMED" { // standing stops untouched by dead session
		t.Fatalf("AAA under dead session: %+v", dead["AAA"])
	}

	// Phase 2: FRESH credential (the trigger bump works in our favour here).
	mustExec(t, exec, `UPDATE user_credentials SET indira_source=indira_source WHERE user_id=$1`, m2User)
	board, err := eod.Board(context.Background())
	if err != nil {
		t.Fatalf("eod board: %v", err)
	}
	got := map[string]EODRow{}
	for _, r := range board.Rows {
		if r.UserID == m2User {
			got[r.Symbol] = r
		}
	}
	if got["AAA"].State != "ARMED" || got["AAA"].Conversion != "PROMOTED" {
		t.Fatalf("AAA: %+v", got["AAA"])
	}
	if got["HHH"].State != "AMO_PENDING" || got["HHH"].BrokerOrderID != "BRK-M8-H" {
		t.Fatalf("HHH: %+v", got["HHH"])
	}
	if r := got["III"]; r.State != "REJECTED" || r.Attempts != 3 || !strings.Contains(r.LastError, "DPR") {
		t.Fatalf("III: %+v", r)
	}
	// REJECTED rows carry a re-arm hint.
	var hint bool
	for _, a := range got["III"].Actions {
		if strings.Contains(a, "rearm-protection") {
			hint = true
		}
	}
	if !hint {
		t.Fatalf("III missing re-arm hint: %+v", got["III"].Actions)
	}
	if got["CCC"].State != "NAKED" {
		t.Fatalf("CCC after fresh cred: %+v", got["CCC"])
	}
	if board.AMOWindow != "OPEN" && board.AMOWindow != "CLOSED" {
		t.Fatalf("window: %+v", board.AMOWindow)
	}
}

func TestM9_CapsAndDrivers(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)
	seedM5(t, trading, exec, pos, now) // manthan_positions AAA+CCC (industry Test, SMALL)
	mustExec(t, trading, `DELETE FROM trade_configs WHERE strategy_id = $1`, m2Strat)
	mustExec(t, trading, `INSERT INTO trade_configs (strategy_id, order_type, product_type, validity, quantity, exchange, order_side, total_capital, max_positions)
		VALUES ($1, 'LIMIT', 'DELIVERY', 'DAY', 0, 'NSE', 'BUY', 100000, 8)`, m2Strat)
	t.Cleanup(func() { _, _ = trading.Exec(`DELETE FROM trade_configs WHERE strategy_id = $1`, m2Strat) })

	rs := NewRiskStore(NewFleetStore(trading, exec, pos),
		userKeyedCreds{m2User: credsOK(m2User)},
		stubFunds{fl: &indiraClient.FundLimit{AvailableCash: 42000, AvailableMargin: 50000}},
		func(context.Context) (string, error) { return `{"NTYSLCP250":0.30,"NIFTY500":0.10}`, nil })

	caps, err := rs.Caps(context.Background())
	if err != nil {
		t.Fatalf("caps: %v", err)
	}
	var mine *StrategyCaps
	for i := range caps {
		if caps[i].StrategyID == m2Strat {
			mine = &caps[i]
		}
	}
	if mine == nil {
		t.Fatalf("strategy missing: %+v", caps)
	}
	// max_positions=8 → sector limit ceil(2)=2, bucket limit 4. Two ACTIVE
	// manthan_positions rows (AAA, CCC) in industry 'Test' → sector FULL.
	if mine.MaxPositions != 8 || mine.TotalCapital != 100000 || mine.OpenPositions != 2 {
		t.Fatalf("config: %+v", mine)
	}
	if len(mine.Sectors) == 0 || mine.Sectors[0].Name != "Test" || mine.Sectors[0].Used != 2 ||
		mine.Sectors[0].Limit != 2 || !mine.Sectors[0].Full {
		t.Fatalf("sector slots: %+v", mine.Sectors)
	}
	if len(mine.McapBuckets) == 0 || mine.McapBuckets[0].Limit != 4 || mine.McapBuckets[0].Full {
		t.Fatalf("bucket slots: %+v", mine.McapBuckets)
	}
	if mine.MarginLeg != "OK" || mine.AvailableCash != 42000 {
		t.Fatalf("margin: %+v", mine)
	}
	if mine.ExposurePct < 1.7 || mine.ExposurePct > 1.9 { // 1800/100000
		t.Fatalf("exposure: %v", mine.ExposurePct)
	}

	d, err := rs.Drivers(context.Background())
	if err != nil || d["ema_status"] != "OK" {
		t.Fatalf("drivers: %v %+v", err, d)
	}
	ema := d["ema_allocations"].(map[string]float64)
	if ema["NTYSLCP250"] != 0.30 {
		t.Fatalf("ema: %+v", ema)
	}
}

func TestM10_M11_HTTP(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M10", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	fleet := NewFleetStore(trading, exec, pos)
	h.SetFleetStore(fleet)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	h.SetEOD(NewEODStore(fleet, nil, ist))
	h.SetRisk(NewRiskStore(fleet, userKeyedCreds{}, stubFunds{fl: &indiraClient.FundLimit{}}, nil))
	ops := NewOpsStore(fleet, "http://127.0.0.1:1", nil)
	ops.run = func(context.Context, string, string, ...string) ([]byte, error) {
		return []byte(`[{"name":"api-gateway","pm2_env":{"status":"online","pm_uptime":1,"restart_time":4}}]`), nil
	}
	h.SetOps(ops)
	h.SetExports(NewExportStore(fleet, NewStore(adminDB)))
	r := newRouterFor(t, h)
	token := elevateViaHTTP(t, r, "TADM_M10")

	rec := m4do(t, r, token, "GET", "/api/v1/admin/eod", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"amo_window"`) {
		t.Fatalf("eod http: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/risk/caps", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "max_positions") {
		t.Fatalf("caps http: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/risk/drivers", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "ema_status") {
		t.Fatalf("drivers http: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/infra", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"api-gateway"`) ||
		!strings.Contains(rec.Body.String(), "trading_db") || !strings.Contains(rec.Body.String(), "kafka consumer lag") {
		t.Fatalf("infra http: %d %.300s", rec.Code, rec.Body.String())
	}

	// Exports: CSV shape + headers + range validation.
	rec = m4do(t, r, token, "GET", "/api/v1/admin/exports/orders?from=bogus", "")
	if rec.Code != 400 {
		t.Fatalf("bad range: %d", rec.Code)
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/exports/orders", "")
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/csv") {
		t.Fatalf("orders export: %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	lines := strings.Split(strings.TrimSpace(rec.Body.String()), "\n")
	if !strings.HasPrefix(lines[0], "id,signal_id,strategy_id,user_id,symbol") {
		t.Fatalf("orders header: %s", lines[0])
	}
	if len(lines) < 2 { // fixtures seeded orders in range
		t.Fatalf("orders export empty")
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/exports/events", "")
	if rec.Code != 200 || !strings.Contains(strings.Split(rec.Body.String(), "\n")[0], "event_type") {
		t.Fatalf("events export: %d %.200s", rec.Code, rec.Body.String())
	}
	rec = m4do(t, r, token, "GET", "/api/v1/admin/exports/admin-actions", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "ELEVATE") {
		t.Fatalf("admin export must contain this session's ELEVATE: %d %.300s", rec.Code, rec.Body.String())
	}
}
