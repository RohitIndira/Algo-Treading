package admin

// M2 end-to-end tests. Seeds one synthetic user across the three real local
// databases (trading_db / execution_db / positions_db) and verifies the grid
// math and the attention queue against known-truth fixtures:
//
//	TADM_M2 user, strategy TADM-M2-STRAT:
//	  AAA — open, standing SL            → armed
//	  BBB — open, band-deferred SL       → deferred
//	  CCC — open, no protection at all   → NAKED (CRITICAL)
//	  DDD — exited yesterday, pnl −500   → total only
//	  EEE — exited today,     pnl +300   → today + total
//	  CCC duplicate ACTIVE row           → must dedupe (BALUFORGE defect)
//	  FFF — 6 failed AMO attempts, never stood → AMO_GIVEUP (HIGH)
//	  GGG — 6 failed AMO attempts + 1 SL_PLACED → NOT a give-up
//	  one DLQ'd inbox row today          → DLQ_SIGNAL (HIGH)
//	  credential updated 30h ago         → stale → DEAD_SESSION (CRITICAL)

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	m2User  = "TADM_M2"
	m2Strat = "aaaaaaaa-1111-2222-3333-0000000000a2"
)

func openFleetDBs(t *testing.T) (trading, exec, pos *sql.DB) {
	t.Helper()
	open := func(name string) *sql.DB {
		db, err := sql.Open("postgres",
			"host=localhost port=5432 user=postgres password=postgres dbname="+name+" sslmode=disable")
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		if err := db.Ping(); err != nil {
			t.Skipf("%s not reachable — skipping fleet tests (%v)", name, err)
		}
		return db
	}
	trading, exec, pos = open("trading_db"), open("execution_db"), open("positions_db")
	cleanupFleet(t, trading, exec, pos)
	t.Cleanup(func() {
		cleanupFleet(t, trading, exec, pos)
		trading.Close()
		exec.Close()
		pos.Close()
	})
	return trading, exec, pos
}

func cleanupFleet(t *testing.T, trading, exec, pos *sql.DB) {
	t.Helper()
	_, _ = trading.Exec(`DELETE FROM strategies WHERE user_id = $1`, m2User)
	_, _ = exec.Exec(`DELETE FROM manthan_orders WHERE user_id = $1`, m2User)
	_, _ = exec.Exec(`DELETE FROM user_credentials WHERE user_id = $1`, m2User)
	_, _ = exec.Exec(`DELETE FROM signal_inbox WHERE user_id = $1`, m2User)
	_, _ = pos.Exec(`DELETE FROM positions WHERE user_id = $1`, m2User)
}

func seedFleetFixtures(t *testing.T, trading, exec, pos *sql.DB, now time.Time) {
	t.Helper()

	if _, err := trading.Exec(`
		INSERT INTO strategies (strategy_id, user_id, strategy_name, active, trading_mode, strategy_type, created_at)
		VALUES ($1, $2, 'm2 fleet test', true, 'LIVE', 'MANTHAN', $3)`,
		m2Strat, m2User, now.Add(-72*time.Hour)); err != nil {
		t.Fatalf("seed strategy: %v", err)
	}

	// Seed with a DB-side interval — the column is timestamp-without-tz, so
	// only DB-clock arithmetic is timezone-safe (matches credentialAges).
	if _, err := exec.Exec(`
		INSERT INTO user_credentials (user_id, indira_user_id, indira_app_id, indira_source, indira_bearer_token, created_at, updated_at)
		VALUES ($1, $1, 'test-app', 'AND', 'enc-token', now() - interval '30 hours', now() - interval '30 hours')`,
		m2User); err != nil { // 30h old → stale
		t.Fatalf("seed credential: %v", err)
	}

	seedPos := func(id, symbol, status string, exitTime any, pnl any) {
		t.Helper()
		if _, err := pos.Exec(`
			INSERT INTO positions (position_id, origin, user_id, strategy_id, signal_id, symbol, exchange,
			                       status, entry_price, entry_time, quantity, invested_amount,
			                       exit_time, realized_pnl)
			VALUES ($1, 'MANTHAN', $2, $3, $4, $5, 'NSE', $6, 100, $7, 10, 1000, $8, $9)`,
			id, m2User, m2Strat, strings.Replace(id, "-2222-", "-4444-", 1), symbol, status, now.Add(-48*time.Hour), exitTime, pnl); err != nil {
			t.Fatalf("seed position %s: %v", symbol, err)
		}
	}
	seedPos("aaaaaaaa-1111-2222-3333-00000000aaa1", "AAA", "ACTIVE", nil, nil)
	seedPos("aaaaaaaa-1111-2222-3333-00000000bbb1", "BBB", "ACTIVE", nil, nil)
	seedPos("aaaaaaaa-1111-2222-3333-00000000ccc1", "CCC", "ACTIVE", nil, nil)
	// NOTE: a literal duplicate ACTIVE row (the prod BALUFORGE defect) is
	// no longer insertable — uq_positions_natural_key forbids it in the
	// current schema. The DISTINCT in fillPositions still guards prod's
	// pre-constraint legacy rows.
	seedPos("aaaaaaaa-1111-2222-3333-00000000ddd1", "DDD", "EXITED", now.Add(-26*time.Hour), -500.0)
	seedPos("aaaaaaaa-1111-2222-3333-00000000eee1", "EEE", "EXITED", now.Add(-1*time.Hour), 300.0)

	seedOrd := func(sig, symbol, otype, status string, createdAgo time.Duration) {
		t.Helper()
		if _, err := exec.Exec(`
			INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type,
			                            order_side, qty, filled_qty, status, created_at)
			VALUES ($1, $2, $3, $4, $5, 'SELL', 10, 0, $6, $7)`,
			sig, m2Strat, m2User, symbol, otype, status, time.Now().Add(-createdAgo)); err != nil {
			t.Fatalf("seed order %s/%s: %v", symbol, status, err)
		}
	}
	seedOrd("tadm-m2-sl-aaa", "AAA", "SL_SELL", "SL_PLACED", time.Hour)        // armed
	seedOrd("tadm-m2-sl-bbb", "BBB", "SL_SELL", "SL_DEFERRED_BAND", time.Hour) // deferred
	// CCC: nothing — naked.
	// FFF: 6 failed AMO attempts, never stood → give-up.
	for i := 0; i < 6; i++ {
		seedOrd(fmt.Sprintf("tadm-m2-fff-%d", i), "FFF", "SL_SELL_AMO", "CANCELLED", time.Duration(i+1)*time.Hour)
	}
	// GGG: 6 failures BUT one standing AMO → not a give-up.
	for i := 0; i < 6; i++ {
		seedOrd(fmt.Sprintf("tadm-m2-ggg-%d", i), "GGG", "SL_SELL_AMO", "REJECTED", time.Duration(i+1)*time.Hour)
	}
	seedOrd("tadm-m2-ggg-ok", "GGG", "SL_SELL_AMO", "SL_PLACED", 30*time.Minute)

	if _, err := exec.Exec(`
		INSERT INTO signal_inbox (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset, status, last_error)
		VALUES ('tadm-m2-dlq', $1, 'MANTHAN_ENTRY', '{}', 't', 0, 0, 'DLQ', 'expired: entry signals are same-day only')`,
		m2User); err != nil {
		t.Fatalf("seed dlq: %v", err)
	}
}

func m2Row(t *testing.T, rows []FleetRow) FleetRow {
	t.Helper()
	for _, r := range rows {
		if r.StrategyID == m2Strat {
			return r
		}
	}
	t.Fatalf("fixture strategy %s not in fleet (%d rows)", m2Strat, len(rows))
	return FleetRow{}
}

func TestFleet_GridMath(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	now := time.Now()
	seedFleetFixtures(t, trading, exec, pos, now)

	fs := NewFleetStore(trading, exec, pos)
	rows, err := fs.Fleet(context.Background())
	if err != nil {
		t.Fatalf("Fleet: %v", err)
	}
	r := m2Row(t, rows)

	if r.OpenPositions != 3 { // AAA BBB CCC
		t.Errorf("open=%d want 3", r.OpenPositions)
	}
	if r.Armed != 1 || r.Deferred != 1 || r.Naked != 1 {
		t.Errorf("armed/deferred/naked = %d/%d/%d want 1/1/1", r.Armed, r.Deferred, r.Naked)
	}
	if len(r.NakedSymbols) != 1 || r.NakedSymbols[0] != "CCC" {
		t.Errorf("naked_symbols=%v want [CCC]", r.NakedSymbols)
	}
	if r.RealizedPnLTotal != -200 { // -500 + 300
		t.Errorf("pnl_total=%.2f want -200", r.RealizedPnLTotal)
	}
	if r.RealizedPnLToday != 300 {
		t.Errorf("pnl_today=%.2f want 300", r.RealizedPnLToday)
	}
	if !r.CredentialStale || r.CredentialAgeHours < 29 || r.CredentialAgeHours > 31 {
		t.Errorf("credential stale=%v age=%.1fh want stale ~30h", r.CredentialStale, r.CredentialAgeHours)
	}
	if !r.Active || r.TradingMode != "LIVE" || r.StrategyType != "MANTHAN" {
		t.Errorf("identity fields wrong: %+v", r)
	}
}

func TestAttention_QueueClasses(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())

	fs := NewFleetStore(trading, exec, pos)
	items, notWired, err := fs.Attention(context.Background())
	if err != nil {
		t.Fatalf("Attention: %v", err)
	}

	find := func(kind, symbol string) *AttentionItem {
		for i := range items {
			if items[i].Kind == kind && items[i].UserID == m2User &&
				(symbol == "" || items[i].Symbol == symbol) {
				return &items[i]
			}
		}
		return nil
	}

	if it := find("DEAD_SESSION", ""); it == nil || it.Severity != "CRITICAL" {
		t.Errorf("DEAD_SESSION missing or wrong severity: %+v", it)
	}
	if it := find("NAKED_POSITION", "CCC"); it == nil || it.Severity != "CRITICAL" || it.Strategy != m2Strat {
		t.Errorf("NAKED_POSITION CCC missing/wrong: %+v", it)
	}
	if it := find("AMO_GIVEUP", "FFF"); it == nil || it.Severity != "HIGH" {
		t.Errorf("AMO_GIVEUP FFF missing/wrong: %+v", it)
	}
	if it := find("AMO_GIVEUP", "GGG"); it != nil {
		t.Errorf("GGG has a standing AMO — must NOT be a give-up: %+v", it)
	}
	if it := find("DLQ_SIGNAL", ""); it == nil || it.Severity != "HIGH" ||
		!strings.Contains(it.Detail, "tadm-m2-dlq") {
		t.Errorf("DLQ_SIGNAL missing/wrong: %+v", it)
	}

	// Ranking: no HIGH before a CRITICAL.
	seenHigh := false
	for _, it := range items {
		if it.Severity == "HIGH" {
			seenHigh = true
		}
		if it.Severity == "CRITICAL" && seenHigh {
			t.Fatalf("severity ordering broken: CRITICAL after HIGH")
		}
	}
	if len(notWired) != 2 {
		t.Errorf("not_wired=%v want the two deferred classes declared", notWired)
	}
}

func TestHTTP_FleetAndAttention_EndToEnd(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M2ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetFleetStore(NewFleetStore(trading, exec, pos))
	root := newRouterFor(t, h)
	token := elevateViaHTTP(t, root, "TADM_M2ADMIN")

	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set(TokenHeader, token)
		rec := httptest.NewRecorder()
		root.ServeHTTP(rec, req)
		return rec
	}

	rec := get("/api/v1/admin/fleet")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), m2Strat) ||
		!strings.Contains(rec.Body.String(), `"naked":1`) {
		t.Fatalf("fleet http: %d %s", rec.Code, rec.Body.String())
	}
	rec = get("/api/v1/admin/attention")
	if rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), "NAKED_POSITION") ||
		!strings.Contains(rec.Body.String(), "not_wired") {
		t.Fatalf("attention http: %d %s", rec.Code, rec.Body.String())
	}
	// Both reads audited (READ tier still logs nothing extra by design —
	// but the route chain ran; whoami confirms the session stayed valid).
	rec = get("/api/v1/admin/whoami")
	if rec.Code != http.StatusOK {
		t.Fatalf("session died during fleet reads: %d", rec.Code)
	}
}
