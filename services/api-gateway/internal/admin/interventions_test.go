package admin

// M7 Phase A tests. The guardrails ARE the feature: same-day refusal,
// entry-only refusal, hold-class rules, reason-required, ghost resets.
// trade-execution is an httptest stub for the re-arm proxy.

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newM7Router(t *testing.T, rearmURL string) (root http.Handler, token string, exec *sql.DB) {
	t.Helper()
	trading, execDB, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, execDB, pos, time.Now())

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M7ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetFleetStore(NewFleetStore(trading, execDB, pos))
	ist, _ := time.LoadLocation("Asia/Kolkata")
	h.SetInterventions(NewInterventions(h.fleet, ist, rearmURL))
	r := newRouterFor(t, h)
	return r, elevateViaHTTP(t, r, "TADM_M7ADMIN"), execDB
}

// seedInbox inserts one signal_inbox row and returns its id.
func seedInbox(t *testing.T, exec *sql.DB, orderType, status, class string, createdAgo time.Duration, nextAt string) int64 {
	t.Helper()
	var id int64
	err := exec.QueryRow(`
		INSERT INTO signal_inbox (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset,
		                          status, attempts, last_error_class, created_at, next_attempt_at, completed_at)
		VALUES (gen_random_uuid()::text, $1, $2, jsonb_build_object('symbol','M7SYM'), 't', 0, floor(random()*1e9)::bigint,
		        $3, 3, NULLIF($4,''), now() - $5::interval, `+nextAt+`,
		        CASE WHEN $3 IN ('DLQ','DONE') THEN now() ELSE NULL END)
		RETURNING id`,
		m2User, orderType, status, class, fmt.Sprintf("%d seconds", int(createdAgo.Seconds()))).Scan(&id)
	if err != nil {
		t.Fatalf("seed inbox: %v", err)
	}
	t.Cleanup(func() { _, _ = exec.Exec(`DELETE FROM signal_inbox WHERE id=$1`, id) })
	return id
}

func TestM7_Resurrect_Guardrails(t *testing.T) {
	root, token, exec := newM7Router(t, "http://127.0.0.1:1")

	today := seedInbox(t, exec, "MANTHAN_ENTRY", "DLQ", "AUTH_EXPIRED", 2*time.Hour, "now()")
	yesterday := seedInbox(t, exec, "MANTHAN_ENTRY", "DLQ", "AUTH_EXPIRED", 26*time.Hour, "now()")
	slRow := seedInbox(t, exec, "MANTHAN_SL_MODIFY", "DLQ", "AUTH_EXPIRED", 2*time.Hour, "now()")

	// Unconfirmed → 412 gate.
	if rec := m4do(t, root, token, "POST", fmt.Sprintf("/api/v1/admin/inbox/%d/resurrect", today), `{}`); rec.Code != 412 {
		t.Fatalf("unconfirmed: %d", rec.Code)
	}
	// Yesterday's signal → REFUSED (the whole point).
	rec := m4do(t, root, token, "POST", fmt.Sprintf("/api/v1/admin/inbox/%d/resurrect", yesterday), `{"confirmed":true}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "same-trading-day") {
		t.Fatalf("yesterday not refused: %d %s", rec.Code, rec.Body.String())
	}
	// Non-entry row → REFUSED, points at re-arm.
	rec = m4do(t, root, token, "POST", fmt.Sprintf("/api/v1/admin/inbox/%d/resurrect", slRow), `{"confirmed":true}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "re-arm") {
		t.Fatalf("SL row not refused: %d %s", rec.Code, rec.Body.String())
	}
	// Today's DLQ'd entry → resurrected with the EXACT validated update.
	if rec := m4do(t, root, token, "POST", fmt.Sprintf("/api/v1/admin/inbox/%d/resurrect", today), `{"confirmed":true}`); rec.Code != 200 {
		t.Fatalf("resurrect: %d %s", rec.Code, rec.Body.String())
	}
	var status string
	var attempts int
	var completed sql.NullTime
	if err := exec.QueryRow(`SELECT status, attempts, completed_at FROM signal_inbox WHERE id=$1`, today).
		Scan(&status, &attempts, &completed); err != nil {
		t.Fatalf("readback: %v", err)
	}
	if status != "FAILED" || attempts != 0 || completed.Valid {
		t.Fatalf("row not reset: status=%s attempts=%d completed=%v", status, attempts, completed.Valid)
	}
	// Unknown id → 404.
	if rec := m4do(t, root, token, "POST", "/api/v1/admin/inbox/999999999/resurrect", `{"confirmed":true}`); rec.Code != 404 {
		t.Fatalf("missing row: %d", rec.Code)
	}
}

func TestM7_ReleaseHold(t *testing.T) {
	root, token, exec := newM7Router(t, "http://127.0.0.1:1")

	held := seedInbox(t, exec, "MANTHAN_ENTRY", "PENDING", "UPPER_CIRCUIT", time.Hour, "now() + interval '3 hours'")
	dlq := seedInbox(t, exec, "MANTHAN_ENTRY", "DLQ", "POISON", time.Hour, "now()")

	// Reason is mandatory.
	rec := m4do(t, root, token, "POST", fmt.Sprintf("/api/v1/admin/inbox/%d/release", held), `{"confirmed":true}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "reason") {
		t.Fatalf("missing reason accepted: %d %s", rec.Code, rec.Body.String())
	}
	// DLQ row → refused (release is for live holds only).
	rec = m4do(t, root, token, "POST", fmt.Sprintf("/api/v1/admin/inbox/%d/release", dlq),
		`{"confirmed":true,"reason":"testing"}`)
	if rec.Code != 422 {
		t.Fatalf("DLQ release not refused: %d %s", rec.Code, rec.Body.String())
	}
	// Held row → released now.
	rec = m4do(t, root, token, "POST", fmt.Sprintf("/api/v1/admin/inbox/%d/release", held),
		`{"confirmed":true,"reason":"circuit relaxed, operator watching"}`)
	if rec.Code != 200 {
		t.Fatalf("release: %d %s", rec.Code, rec.Body.String())
	}
	var due bool
	if err := exec.QueryRow(`SELECT next_attempt_at <= now() FROM signal_inbox WHERE id=$1`, held).Scan(&due); err != nil || !due {
		t.Fatalf("hold not released: %v due=%v", err, due)
	}
}

func TestM7_CapReset(t *testing.T) {
	root, token, exec := newM7Router(t, "http://127.0.0.1:1")

	// Five failed overnight attempts targeting tomorrow's session.
	for i := 0; i < 5; i++ {
		mustExec(t, exec, `INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side,
				qty, filled_qty, status, trade_date, created_at)
			VALUES ($1, $2, $3, 'M7CAP', 'SL_SELL_AMO', 'SELL', 10, 0, 'CANCELLED', CURRENT_DATE + 1, now() - interval '1 hour')`,
			fmt.Sprintf("tadm-m7-cap-%d", i), m2Strat, m2User)
	}

	// Reason mandatory.
	rec := m4do(t, root, token, "POST", "/api/v1/admin/users/"+m2User+"/amo-cap/reset",
		`{"confirmed":true,"symbol":"M7CAP"}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "reason") {
		t.Fatalf("missing reason accepted: %d %s", rec.Code, rec.Body.String())
	}
	// Reset clears exactly the 5 counted attempts.
	rec = m4do(t, root, token, "POST", "/api/v1/admin/users/"+m2User+"/amo-cap/reset",
		`{"confirmed":true,"symbol":"M7CAP","reason":"broker fixed the DPR band, retry tonight"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"attempts_cleared":5`) {
		t.Fatalf("cap reset: %d %s", rec.Code, rec.Body.String())
	}
	var counted int
	if err := exec.QueryRow(`SELECT COUNT(*) FROM manthan_orders
		WHERE user_id=$1 AND symbol='M7CAP' AND trade_date IS NOT NULL`, m2User).Scan(&counted); err != nil || counted != 0 {
		t.Fatalf("attempts still counted: %v n=%d", err, counted)
	}
	// Rows themselves survive for audit.
	if err := exec.QueryRow(`SELECT COUNT(*) FROM manthan_orders WHERE user_id=$1 AND symbol='M7CAP'`, m2User).Scan(&counted); err != nil || counted != 5 {
		t.Fatalf("attempt rows lost: %v n=%d", err, counted)
	}
	// Second reset → 422 (nothing left to clear).
	rec = m4do(t, root, token, "POST", "/api/v1/admin/users/"+m2User+"/amo-cap/reset",
		`{"confirmed":true,"symbol":"M7CAP","reason":"again"}`)
	if rec.Code != 422 {
		t.Fatalf("double reset: %d", rec.Code)
	}
}

func TestM7_RearmProxy(t *testing.T) {
	var gotPath, gotMethod string
	te := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.String(), r.Method
		w.Write([]byte(`{"ok":true,"user_id":"` + r.URL.Query().Get("user_id") + `"}`))
	}))
	defer te.Close()
	root, token, _ := newM7Router(t, te.URL)

	rec := m4do(t, root, token, "POST", "/api/v1/admin/users/"+m2User+"/rearm-protection", `{"confirmed":true}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"ok":true`) {
		t.Fatalf("rearm: %d %s", rec.Code, rec.Body.String())
	}
	if gotMethod != "POST" || !strings.Contains(gotPath, "/manthan/replay/runOnceForUser?user_id="+m2User) {
		t.Fatalf("proxy shape: %s %s", gotMethod, gotPath)
	}

	// trade-execution down → 502-class with the guardrail error, not a hang.
	rootDown, tokenDown, _ := newM7Router(t, "http://127.0.0.1:1")
	rec = m4do(t, rootDown, tokenDown, "POST", "/api/v1/admin/users/"+m2User+"/rearm-protection", `{"confirmed":true}`)
	if rec.Code != 500 && rec.Code != 502 {
		t.Fatalf("down TE: %d %s", rec.Code, rec.Body.String())
	}
}
