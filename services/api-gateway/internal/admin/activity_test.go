package admin

// Logins + daily activity — DB-backed against the local trading_db /
// execution_db with a dedicated user so fixtures never collide with the
// fleet (TADM_M2) suite.
//
//	TADM_ACT, strategy TADM-ACT-STRAT (active):
//	  logins: token A (today 09:05 IST, SSO), token A again (dedup'd by the
//	          writer — seeded once), token B (today 14:00 IST, APP, expired),
//	          one 10 days ago (outside a 7-day window), one BACKFILL row
//	  buys:   LUM filled today ×9 @2120.50; SHP rejected ×3 today (AU004);
//	          OLD filled yesterday (excluded)
//	  exits:  DEEP exited today TSL_HIT @656.55 with an SL_SELL fill;
//	          GHOST exited today ADMIN_GHOST_CLEANUP (no price);
//	          PEND has a MARKET_SELL PLACED today, not filled → open sell;
//	          YEST exited yesterday (excluded);
//	          a MARKET_SELL fill for ORPHAN today with no book row → unmatched

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	actUser  = "TADM_ACT"
	actStrat = "aaaaaaaa-1111-2222-3333-00000000ac71"
	// A client who LEFT Manthan: strategy stopped, book closed. Has a
	// filled buy and an exit today — none of it may surface.
	exUser  = "TADM_EXACT"
	exStrat = "aaaaaaaa-1111-2222-3333-00000000ac72"
)

func cleanupActivity(trading, exec *sql.DB) {
	for _, u := range []string{actUser, exUser} {
		_, _ = trading.Exec(`DELETE FROM manthan_positions WHERE user_id = $1`, u)
		_, _ = trading.Exec(`DELETE FROM strategies WHERE user_id = $1`, u)
		_, _ = exec.Exec(`DELETE FROM manthan_order_events WHERE order_id IN (SELECT id FROM manthan_orders WHERE user_id = $1)`, u)
		_, _ = exec.Exec(`DELETE FROM manthan_orders WHERE user_id = $1`, u)
		_, _ = exec.Exec(`DELETE FROM user_login_events WHERE user_id = $1`, u)
		_, _ = exec.Exec(`DELETE FROM user_credentials WHERE user_id = $1`, u)
	}
}

func seedActivity(t *testing.T, trading, exec *sql.DB, ist *time.Location, now time.Time) {
	t.Helper()
	mig, err := os.ReadFile(filepath.Join("..", "..", "..", "trade-execution", "migrations", "018_user_login_events.sql"))
	if err != nil {
		t.Fatalf("read 018: %v", err)
	}
	if _, err := exec.Exec(string(mig)); err != nil {
		t.Fatalf("apply 018: %v", err)
	}
	cleanupActivity(trading, exec)
	t.Cleanup(func() { cleanupActivity(trading, exec) })

	n := now.In(ist)
	day := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, ist)
	at := func(h, m int) time.Time { return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute) }
	yest := day.AddDate(0, 0, -1).Add(11 * time.Hour)

	mustExec(t, trading, `INSERT INTO strategies (strategy_id, user_id, strategy_name, active, trading_mode, strategy_type, created_at)
		VALUES ($1, $2, 'activity test', true, 'LIVE', 'MANTHAN', $3)`, actStrat, actUser, now.Add(-72*time.Hour))
	mustExec(t, exec, `INSERT INTO user_credentials (user_id, indira_user_id, indira_app_id, indira_source, indira_bearer_token, updated_at)
		VALUES ($1, $1, 'app', 'SSO', 'enc-token', now())`, actUser)
	mustExec(t, trading, `INSERT INTO strategies (strategy_id, user_id, strategy_name, active, stopped_at, trading_mode, strategy_type, created_at)
		VALUES ($1, $2, 'left manthan', false, $3, 'LIVE', 'MANTHAN', $4)`, exStrat, exUser, now.Add(-24*time.Hour), now.Add(-240*time.Hour))
	mustExec(t, exec, `INSERT INTO user_credentials (user_id, indira_user_id, indira_app_id, indira_source, indira_bearer_token, updated_at)
		VALUES ($1, $1, 'app', 'SSO', 'enc-token-ex', now())`, exUser)

	// Login history.
	ins := func(rec time.Time, iat, exp *time.Time, src, ls, kind, fp string) {
		mustExec(t, exec, `INSERT INTO user_login_events (user_id, indira_user_id, app_id, source, login_source, kind, token_issued_at, token_expires_at, token_fingerprint, recorded_at)
			VALUES ($1, $1, 'app', $2, $3, $4, $5, $6, $7, $8)`, actUser, src, ls, kind, iat, exp, fp, rec)
	}
	iatA, expA := at(9, 4), at(23, 59)
	iatB, expB := at(13, 59), at(14, 5) // already expired by 15:00
	old := day.AddDate(0, 0, -10).Add(10 * time.Hour)
	ins(at(9, 5), &iatA, &expA, "SSO", "SSO", "LOGIN", "fp-a")
	ins(at(14, 0), &iatB, &expB, "AND", "APP", "LOGIN", "fp-b")
	ins(old, &old, nil, "IOS", "", "LOGIN", "fp-old")
	ins(day.AddDate(0, 0, -20), nil, nil, "WEB", "", "BACKFILL", "legacy:x")

	// Buys.
	order := func(sym, otype, side, status string, qty, filled int, avg, lim *float64, created time.Time, filledAt *time.Time, brokerID, lastErr string) {
		mustExec(t, exec, `INSERT INTO manthan_orders (strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, avg_fill_price, limit_price, status, broker_order_id, last_error, created_at, placed_at, filled_at, updated_at)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::numeric, $9::numeric, $10, $11, $12, $13::timestamptz, $13::timestamptz, $14::timestamptz, COALESCE($14::timestamptz, $13::timestamptz))`,
			actStrat, actUser, sym, otype, side, qty, filled, avg, lim, status, brokerID, lastErr, created, filledAt)
	}
	f := func(v float64) *float64 { return &v }
	tp := func(v time.Time) *time.Time { return &v }
	order("LUM", "LIMIT_BUY", "BUY", "FILLED", 9, 9, f(2120.50), f(2116.40), at(10, 55), tp(at(10, 55)), "NYMZX0000DM:", "")
	for i := 0; i < 3; i++ {
		order("SHP", "LIMIT_BUY", "BUY", "REJECTED", 70, 0, nil, f(570.30), at(9, 15).Add(time.Duration(i)*time.Second), nil, "", "AU004: Unauthorized access, invalid token")
	}
	order("OLD", "LIMIT_BUY", "BUY", "FILLED", 5, 5, f(100), f(100), yest, tp(yest), "OLD1:", "")
	// Sells.
	order("DEEP", "SL_SELL", "SELL", "FILLED", 29, 29, f(656.55), nil, at(9, 20), tp(at(9, 20)), "NYMZX0000BL:", "")
	order("ORPHAN", "MARKET_SELL", "SELL", "FILLED", 10, 10, f(50), nil, at(12, 0), tp(at(12, 0)), "ORPH1:", "")
	order("PEND", "MARKET_SELL", "SELL", "PLACED", 8, 0, nil, f(540), at(14, 30), nil, "PEND1:", "")
	order("LUM", "SL_SELL", "SELL", "SL_PLACED", 9, 0, nil, nil, at(10, 56), nil, "LUMSL:", "") // standing stop: NOT an open sell
	// The ex-client's day (must be invisible).
	mustExec(t, exec, `INSERT INTO manthan_orders (strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, avg_fill_price, status, broker_order_id, created_at, placed_at, filled_at, updated_at)
		VALUES ($1::uuid, $2, 'EXBUY', 'LIMIT_BUY', 'BUY', 4, 4, 100::numeric, 'FILLED', 'EXB1:', $3::timestamptz, $3::timestamptz, $3::timestamptz, $3::timestamptz)`, exStrat, exUser, at(10, 0))
	mustExec(t, exec, `INSERT INTO manthan_orders (strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, avg_fill_price, status, broker_order_id, created_at, placed_at, filled_at, updated_at)
		VALUES ($1::uuid, $2, 'EXSELL', 'SL_SELL', 'SELL', 4, 4, 90::numeric, 'FILLED', 'EXS1:', $3::timestamptz, $3::timestamptz, $3::timestamptz, $3::timestamptz)`, exStrat, exUser, at(11, 0))
	mustExec(t, exec, `INSERT INTO manthan_orders (strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, last_error, created_at, placed_at, updated_at)
		VALUES ($1::uuid, $2, 'EXREJ', 'LIMIT_BUY', 'BUY', 4, 0, 'REJECTED', 'AU004', $3::timestamptz, $3::timestamptz, $3::timestamptz)`, exStrat, exUser, at(9, 16))
	mustExec(t, trading, `INSERT INTO manthan_positions (strategy_id, user_id, symbol, entry_price, quantity, invested_amt, status, exit_price, realized_pnl, exit_reason, entry_time, exit_time, updated_at, industry, mcap_bucket, index_name)
		VALUES ($1::uuid, $2, 'EXSELL', 100::numeric, 4, 400::numeric, 'EXITED', 90::numeric, -40::numeric, 'TSL_HIT', $3::timestamptz, $4::timestamptz, $4::timestamptz, 'Test', 'SMALL', 'NIFTY_TEST')`,
		exStrat, exUser, day.AddDate(0, 0, -5), at(11, 0))

	// Book.
	pos := func(sym, status string, qty int, entry float64, entryT time.Time, exitPx, pnl *float64, reason *string, exitT *time.Time) {
		mustExec(t, trading, `INSERT INTO manthan_positions (strategy_id, user_id, symbol, entry_price, quantity, invested_amt, status, exit_price, realized_pnl, exit_reason, entry_time, exit_time, updated_at, industry, mcap_bucket, index_name)
			VALUES ($1::uuid, $2, $3, $4::numeric, $5::int, $4::numeric * $5::int, $6, $7::numeric, $8::numeric, $9::text, $10::timestamptz, $11::timestamptz, COALESCE($11::timestamptz, $10::timestamptz), 'Test', 'SMALL', 'NIFTY_TEST')`,
			actStrat, actUser, sym, entry, qty, status, exitPx, pnl, reason, entryT, exitT)
	}
	s := func(v string) *string { return &v }
	pos("DEEP", "EXITED", 29, 667.40, day.AddDate(0, 0, -3), f(656.55), f(-314.65), s("TSL_HIT"), tp(at(9, 20)))
	pos("GHOST", "EXITED", 15, 2399.70, day.AddDate(0, 0, -30), nil, nil, s("ADMIN_GHOST_CLEANUP"), tp(at(11, 50)))
	pos("PEND", "ACTIVE", 8, 544.10, day.AddDate(0, 0, -2), nil, nil, nil, nil)
	pos("YEST", "EXITED", 51, 381.10, day.AddDate(0, 0, -9), f(372.25), f(-451.35), s("TSL_HIT"), tp(yest))
	pos("LUM", "ACTIVE", 9, 2120.50, at(10, 55), nil, nil, nil, nil)
}

func newActivityStore(trading, exec, pos *sql.DB, now time.Time) *ActivityStore {
	fleet := NewFleetStore(trading, exec, pos)
	fleet.now = func() time.Time { return now }
	a := NewActivityStore(fleet)
	a.now = fleet.now
	return a
}

func TestActivity_Logins(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	n := time.Now().In(ist)
	now := time.Date(n.Year(), n.Month(), n.Day(), 15, 0, 0, 0, ist) // 15:00 IST today
	seedActivity(t, trading, exec, ist, now)
	a := newActivityStore(trading, exec, pos, now)

	res, err := a.Logins(context.Background(), LoginsQuery{UserID: actUser, Days: 7})
	if err != nil {
		t.Fatal(err)
	}
	if res.Source != "login_events" || len(res.Clients) != 1 {
		t.Fatalf("source=%s clients=%d", res.Source, len(res.Clients))
	}
	c := res.Clients[0]
	if !c.ManthanClient || !c.ActiveStrategy || c.OpenPositions != 2 || !c.CredentialPresent { // LUM + PEND are ACTIVE
		t.Fatalf("client flags: %+v", c)
	}
	if c.LoginsToday != 2 || c.LoginsWindow != 2 {
		t.Fatalf("counts today=%d window=%d (old + backfill rows must not count)", c.LoginsToday, c.LoginsWindow)
	}
	if c.LastSource != "AND" || c.TokenState != "EXPIRED" || c.LastTokenIssuedAt == nil || c.LastTokenIssuedAt.In(ist).Format("15:04") != "13:59" {
		t.Fatalf("latest login: %+v", c)
	}
	if len(res.Events) != 2 { // 7-day window: the two of today
		t.Fatalf("events=%d want 2", len(res.Events))
	}
	if res.Events[0].LoginSource != "APP" || res.Events[1].Source != "SSO" {
		t.Fatalf("event order newest-first: %+v", res.Events)
	}

	// 30-day window picks up the old login and the backfill row as events,
	// but the backfill still does not count as a login.
	res, err = a.Logins(context.Background(), LoginsQuery{UserID: actUser, Days: 30})
	if err != nil {
		t.Fatal(err)
	}
	if res.Clients[0].LoginsWindow != 3 || len(res.Events) != 4 {
		t.Fatalf("30d window=%d events=%d", res.Clients[0].LoginsWindow, len(res.Events))
	}

	// Without a user filter: enrolled Manthan clients only.
	res, err = a.Logins(context.Background(), LoginsQuery{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range res.Clients {
		if c.UserID == actUser {
			found = true
		}
		if !c.ManthanClient {
			t.Fatalf("manthan scope leaked a non-Manthan client: %s", c.UserID)
		}
		if c.UserID == exUser {
			t.Fatalf("manthan scope listed a client whose strategy is stopped: %s", c.UserID)
		}
	}
	if !found {
		t.Fatalf("manthan scope missed %s", actUser)
	}

	// There is no whole-platform listing any more; an ex-client is reachable
	// only by asking for them by user_id (the per-user page), flagged as
	// not a Manthan client.
	res, err = a.Logins(context.Background(), LoginsQuery{UserID: exUser})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Clients) != 1 || res.Clients[0].UserID != exUser || res.Clients[0].ManthanClient {
		t.Fatalf("user_id lookup of ex-client: %+v", res.Clients)
	}
}

func TestActivity_Day(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	n := time.Now().In(ist)
	now := time.Date(n.Year(), n.Month(), n.Day(), 15, 0, 0, 0, ist)
	seedActivity(t, trading, exec, ist, now)
	a := newActivityStore(trading, exec, pos, now)

	res, err := a.Activity(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsToday || res.Date != now.Format("2006-01-02") {
		t.Fatalf("date=%s today=%v", res.Date, res.IsToday)
	}
	mine := func(uid string) bool { return uid == actUser }

	// Enrolled-only: the ex-client is not in the client list and none of
	// its rows (buy, rejected buy, sell, exit) leak into the day.
	for _, c := range res.Clients {
		if c == exUser {
			t.Fatalf("ex-client %s listed as enrolled: %v", exUser, res.Clients)
		}
	}
	hasAct := false
	for _, c := range res.Clients {
		hasAct = hasAct || c == actUser
	}
	if !hasAct {
		t.Fatalf("enrolled client %s missing from %v", actUser, res.Clients)
	}
	for _, b := range res.Buys {
		if b.UserID == exUser {
			t.Fatalf("ex-client buy leaked: %+v", b)
		}
	}
	for _, g := range res.RejectedBuys {
		if g.UserID == exUser {
			t.Fatalf("ex-client rejected buy leaked: %+v", g)
		}
	}
	for _, e := range res.Exits {
		if e.UserID == exUser {
			t.Fatalf("ex-client exit leaked: %+v", e)
		}
	}
	for _, u := range res.UnmatchedSells {
		if u.UserID == exUser {
			t.Fatalf("ex-client sell leaked: %+v", u)
		}
	}

	var buys []BuyRow
	for _, b := range res.Buys {
		if mine(b.UserID) {
			buys = append(buys, b)
		}
	}
	if len(buys) != 1 || buys[0].Symbol != "LUM" || buys[0].FilledQty != 9 || buys[0].Value != 19084.5 || buys[0].BrokerOrderID != "NYMZX0000DM" {
		t.Fatalf("buys: %+v", buys)
	}
	var rej []RejectedBuyGroup
	for _, g := range res.RejectedBuys {
		if mine(g.UserID) {
			rej = append(rej, g)
		}
	}
	if len(rej) != 1 || rej[0].Attempts != 3 || rej[0].Qty != 70 || rej[0].Reason != "Broker session expired — client must log in again" {
		t.Fatalf("rejected: %+v", rej)
	}

	exits := map[string]ExitRow{}
	for _, e := range res.Exits {
		if mine(e.UserID) {
			exits[e.Symbol] = e
		}
	}
	if len(exits) != 2 {
		t.Fatalf("exits=%v (YEST must be excluded)", exits)
	}
	deep := exits["DEEP"]
	if !deep.ExitPriceKnown || deep.ExitPrice == nil || *deep.ExitPrice != 656.55 || deep.RealizedPnL == nil || *deep.RealizedPnL != -314.65 ||
		deep.HoldingDays != 3 || deep.SellFill == nil || deep.SellFill.OrderType != "SL_SELL" || deep.SellFill.BrokerOrderID != "NYMZX0000BL" {
		t.Fatalf("DEEP: %+v fill=%+v", deep, deep.SellFill)
	}
	if deep.PnLPct == nil || *deep.PnLPct > -1.6 || *deep.PnLPct < -1.7 {
		t.Fatalf("DEEP pnl_pct=%v", deep.PnLPct)
	}
	ghost := exits["GHOST"]
	if ghost.ExitPriceKnown || ghost.RealizedPnL != nil || ghost.SellFill != nil || ghost.Note == "" {
		t.Fatalf("GHOST: %+v", ghost)
	}
	var open []SellFill
	for _, s := range res.OpenSells {
		if mine(s.UserID) {
			open = append(open, s)
		}
	}
	if len(open) != 1 || open[0].Symbol != "PEND" || open[0].Status != "PLACED" {
		t.Fatalf("open sells: %+v (standing SL_SELL must not count)", open)
	}
	var orphan []SellFill
	for _, s := range res.UnmatchedSells {
		if mine(s.UserID) {
			orphan = append(orphan, s)
		}
	}
	if len(orphan) != 1 || orphan[0].Symbol != "ORPHAN" {
		t.Fatalf("unmatched: %+v", orphan)
	}

	// Yesterday: OLD buy and YEST exit, nothing of today.
	res, err = a.Activity(context.Background(), now.AddDate(0, 0, -1).Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsToday {
		t.Fatal("yesterday flagged as today")
	}
	gotOld, gotYest := false, false
	for _, b := range res.Buys {
		if mine(b.UserID) {
			if b.Symbol != "OLD" {
				t.Fatalf("yesterday buy leak: %+v", b)
			}
			gotOld = true
		}
	}
	for _, e := range res.Exits {
		if mine(e.UserID) {
			if e.Symbol != "YEST" {
				t.Fatalf("yesterday exit leak: %+v", e)
			}
			gotYest = true
		}
	}
	if !gotOld || !gotYest {
		t.Fatalf("yesterday missing rows old=%v yest=%v", gotOld, gotYest)
	}

	if _, err := a.Activity(context.Background(), "29-10-2026"); err == nil {
		t.Fatal("bad date accepted")
	}
}

// HTTP: both routes are READ tier and reachable with a session token.
func TestActivity_HTTP(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	n := time.Now().In(ist)
	now := time.Date(n.Year(), n.Month(), n.Day(), 15, 0, 0, 0, ist)
	seedActivity(t, trading, exec, ist, now)

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_ACT_ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetActivity(newActivityStore(trading, exec, pos, now))
	root := newRouterFor(t, h)
	tok := elevateViaHTTP(t, root, "TADM_ACT_ADMIN")

	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set(TokenHeader, tok)
		rec := httptest.NewRecorder()
		root.ServeHTTP(rec, req)
		return rec
	}
	for _, path := range []string{"/api/v1/admin/logins?user_id=" + actUser, "/api/v1/admin/activity"} {
		rec := get(path)
		var env struct {
			InfoID string          `json:"infoID"`
			Data   json.RawMessage `json:"data"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &env)
		if rec.Code != http.StatusOK || env.InfoID != "0" || len(env.Data) < 10 {
			t.Fatalf("%s → %d %s", path, rec.Code, rec.Body.String())
		}
	}
	if rec := get("/api/v1/admin/activity?date=nope"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad date → %d %s", rec.Code, rec.Body.String())
	}
}
