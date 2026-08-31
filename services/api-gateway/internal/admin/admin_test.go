package admin

// M1 foundation tests. DB-backed cases run against local postgres
// (trading_db on localhost:5432, the standard dev harness) and apply
// migration 019 themselves — idempotent DDL — so the suite is
// self-contained. Unreachable DB → those cases skip, pure cases still run.

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

// ── pure: tokens ────────────────────────────────────────────────────────

func TestNewToken_ShapeAndUniqueness(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		raw, hash, err := NewToken()
		if err != nil {
			t.Fatalf("NewToken: %v", err)
		}
		if len(raw) != tokenBytes*2 || len(hash) != 64 {
			t.Fatalf("token shape: raw=%d hash=%d", len(raw), len(hash))
		}
		if seen[raw] {
			t.Fatal("duplicate token generated")
		}
		seen[raw] = true
		if HashToken(raw) != hash {
			t.Fatal("hash not deterministic")
		}
	}
}

func TestHashesEqual(t *testing.T) {
	_, h1, _ := NewToken()
	_, h2, _ := NewToken()
	if !HashesEqual(h1, h1) || HashesEqual(h1, h2) {
		t.Fatal("HashesEqual broken")
	}
}

// ── pure: rate limiter ──────────────────────────────────────────────────

func TestElevationRateLimit_WindowSlides(t *testing.T) {
	s := &Service{attempts: map[string][]time.Time{}}
	clock := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return clock }

	for i := 0; i < elevateMaxAttempts; i++ {
		if !s.allowAttempt("U1") {
			t.Fatalf("attempt %d should be allowed", i+1)
		}
	}
	if s.allowAttempt("U1") {
		t.Fatal("attempt over the cap must be denied")
	}
	if !s.allowAttempt("U2") {
		t.Fatal("limiter must be per-user")
	}
	clock = clock.Add(elevateWindow + time.Second)
	if !s.allowAttempt("U1") {
		t.Fatal("window must slide — old attempts expire")
	}
}

// ── pure: confirmation text ─────────────────────────────────────────────

func TestConfirmationText(t *testing.T) {
	if got := ConfirmationText("SELL 42 SHREEPUSHK", "order-10230", "S4450"); got != "SELL 42 SHREEPUSHK order-10230 FOR S4450" {
		t.Fatalf("unexpected confirmation text: %q", got)
	}
}

// ── DB harness ──────────────────────────────────────────────────────────

func openAdminTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=trading_db sslmode=disable")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("trading_db not reachable — skipping DB tests (%v)", err)
	}
	// Apply migration 019 (idempotent) so the suite owns its schema.
	mig, err := os.ReadFile(filepath.Join("..", "..", "..", "user-config", "migrations", "019_admin_foundation.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := db.Exec(string(mig)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	cleanupAdminTestRows(t, db)
	t.Cleanup(func() { cleanupAdminTestRows(t, db); db.Close() })
	return db
}

func cleanupAdminTestRows(t *testing.T, db *sql.DB) {
	t.Helper()
	// The migration-020 append-only trigger (rightly) blocks DELETE for
	// everyone. Test cleanup uses the one legitimate escape hatch — a
	// superuser deliberately disabling the trigger — which is exactly the
	// loud, intentional act the design demands for any purge.
	_, _ = db.Exec(`ALTER TABLE admin_audit DISABLE TRIGGER trg_admin_audit_append_only`)
	_, _ = db.Exec(`DELETE FROM admin_audit WHERE admin_id LIKE 'TADM%' OR admin_id = 'UNKNOWN'`)
	_, _ = db.Exec(`ALTER TABLE admin_audit ENABLE TRIGGER trg_admin_audit_append_only`)
	_, _ = db.Exec(`DELETE FROM admin_sessions WHERE admin_id LIKE 'TADM%'`)
	_, _ = db.Exec(`DELETE FROM admin_users    WHERE user_id  LIKE 'TADM%'`)
}

func seedAdmin(t *testing.T, db *sql.DB, id string, active bool) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO admin_users (user_id, active, added_by) VALUES ($1,$2,'TEST')
		ON CONFLICT (user_id) DO UPDATE SET active=$2`, id, active); err != nil {
		t.Fatalf("seed admin %s: %v", id, err)
	}
}

func auditCount(t *testing.T, db *sql.DB, adminID, action, result string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM admin_audit WHERE admin_id=$1 AND action=$2 AND result=$3`,
		adminID, action, result).Scan(&n); err != nil {
		t.Fatalf("audit count: %v", err)
	}
	return n
}

// ── DB: elevation lifecycle ─────────────────────────────────────────────

func TestElevate_FullLifecycle(t *testing.T) {
	db := openAdminTestDB(t)
	svc := NewService(NewStore(db))
	ctx := context.Background()

	// 1. Unknown user → denied + audited, no session.
	if _, err := svc.Elevate(ctx, "TADM_NOBODY", "1.2.3.4"); err != ErrNotAdmin {
		t.Fatalf("unknown user: err=%v want ErrNotAdmin", err)
	}
	if auditCount(t, db, "TADM_NOBODY", "ELEVATE_DENIED", "DENIED") != 1 {
		t.Fatal("denied elevation must be audited")
	}

	// 2. Deactivated admin → denied identically (no oracle).
	seedAdmin(t, db, "TADM_OFF", false)
	if _, err := svc.Elevate(ctx, "TADM_OFF", ""); err != ErrNotAdmin {
		t.Fatalf("deactivated admin: err=%v want ErrNotAdmin", err)
	}

	// 3. Active admin → token + session + audit.
	seedAdmin(t, db, "TADM_A", true)
	res, err := svc.Elevate(ctx, "TADM_A", "9.9.9.9")
	if err != nil {
		t.Fatalf("elevate: %v", err)
	}
	if res.AdminID != "TADM_A" || len(res.Token) != tokenBytes*2 {
		t.Fatalf("bad result: %+v", res)
	}
	if auditCount(t, db, "TADM_A", "ELEVATE", "OK") != 1 {
		t.Fatal("successful elevation must be audited")
	}

	// 4. The token validates end-to-end…
	sess, err := svc.Validate(ctx, res.Token)
	if err != nil || sess.AdminID != "TADM_A" {
		t.Fatalf("validate: sess=%+v err=%v", sess, err)
	}
	// …junk and truncated tokens do not.
	if _, err := svc.Validate(ctx, "deadbeef"); err != ErrSessionInvalid {
		t.Fatalf("short token: %v", err)
	}
	if _, err := svc.Validate(ctx, strings.Repeat("a", tokenBytes*2)); err != ErrSessionInvalid {
		t.Fatalf("forged token: %v", err)
	}

	// 5. Logout revokes — token dies immediately.
	if err := svc.Logout(ctx, sess, ""); err != nil {
		t.Fatalf("logout: %v", err)
	}
	if _, err := svc.Validate(ctx, res.Token); err != ErrSessionInvalid {
		t.Fatal("revoked token must be invalid")
	}
}

func TestValidate_DeactivationKillsLiveSessions(t *testing.T) {
	db := openAdminTestDB(t)
	svc := NewService(NewStore(db))
	ctx := context.Background()

	seedAdmin(t, db, "TADM_KILL", true)
	res, err := svc.Elevate(ctx, "TADM_KILL", "")
	if err != nil {
		t.Fatalf("elevate: %v", err)
	}
	seedAdmin(t, db, "TADM_KILL", false) // deactivate mid-session
	if _, err := svc.Validate(ctx, res.Token); err != ErrSessionInvalid {
		t.Fatal("deactivating an admin must kill their live sessions on next request")
	}
}

func TestValidate_ExpiredSessionRejected(t *testing.T) {
	db := openAdminTestDB(t)
	svc := NewService(NewStore(db))
	ctx := context.Background()

	seedAdmin(t, db, "TADM_EXP", true)
	res, err := svc.Elevate(ctx, "TADM_EXP", "")
	if err != nil {
		t.Fatalf("elevate: %v", err)
	}
	if _, err := db.Exec(`UPDATE admin_sessions SET expires_at = now() - interval '1 minute'
		WHERE admin_id='TADM_EXP'`); err != nil {
		t.Fatalf("age session: %v", err)
	}
	if _, err := svc.Validate(ctx, res.Token); err != ErrSessionInvalid {
		t.Fatal("expired session must be invalid")
	}
}

// ── HTTP: middleware + tiers over a real store ──────────────────────────

func newTestRouter(t *testing.T, db *sql.DB) (*mux.Router, *HTTP) {
	t.Helper()
	h := NewHTTP(NewService(NewStore(db)))
	root := mux.NewRouter()
	adminSub := root.PathPrefix("/api/v1/admin").Subrouter()
	// Fake platform auth: trusts header X-Test-User (test double for the
	// introspection middleware — production wiring injects the real one).
	fakeAuth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			uid := r.Header.Get("X-Test-User")
			if uid == "" {
				http.Error(w, "unauthenticated", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r.WithContext(testAuthContext(r.Context(), uid)))
		})
	}
	h.Register(adminSub, fakeAuth)
	return root, h
}

func elevateViaHTTP(t *testing.T, r *mux.Router, user string) string {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/admin/elevate", nil)
	req.Header.Set("X-Test-User", user)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("elevate HTTP %d: %s", rec.Code, rec.Body.String())
	}
	var env struct {
		Data struct {
			Token string `json:"admin_token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil || env.Data.Token == "" {
		t.Fatalf("elevate response parse: %v / %s", err, rec.Body.String())
	}
	return env.Data.Token
}

func TestHTTP_AdminZoneDeniesWithoutToken(t *testing.T) {
	db := openAdminTestDB(t)
	r, _ := newTestRouter(t, db)

	for _, path := range []string{"/api/v1/admin/whoami", "/api/v1/admin/audit"} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s without token: HTTP %d, want 403", path, rec.Code)
		}
	}
}

func TestHTTP_ElevateThenWhoamiThenAudit(t *testing.T) {
	db := openAdminTestDB(t)
	seedAdmin(t, db, "TADM_HTTP", true)
	r, _ := newTestRouter(t, db)

	// Non-admin platform user cannot elevate.
	req := httptest.NewRequest("POST", "/api/v1/admin/elevate", nil)
	req.Header.Set("X-Test-User", "TADM_RANDO")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin elevate: HTTP %d want 403", rec.Code)
	}

	token := elevateViaHTTP(t, r, "TADM_HTTP")

	// whoami with the token.
	req = httptest.NewRequest("GET", "/api/v1/admin/whoami", nil)
	req.Header.Set(TokenHeader, token)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "TADM_HTTP") {
		t.Fatalf("whoami: HTTP %d %s", rec.Code, rec.Body.String())
	}

	// audit viewer shows the elevation.
	req = httptest.NewRequest("GET", "/api/v1/admin/audit?admin_id=TADM_HTTP", nil)
	req.Header.Set(TokenHeader, token)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ELEVATE"`) {
		t.Fatalf("audit view: HTTP %d %s", rec.Code, rec.Body.String())
	}
}

func TestHTTP_TierEnforcement(t *testing.T) {
	db := openAdminTestDB(t)
	seedAdmin(t, db, "TADM_TIER", true)
	r, h := newTestRouter(t, db)

	// Register one CONFIRM and one TYPED test endpoint through Route()
	// exactly as real features will.
	sub := r.PathPrefix("/api/v1/admin").Subrouter()
	sub.Use(h.Required)
	h.Route(sub, "POST", "/t-confirm", "T_CONFIRM", TierConfirm,
		func(w http.ResponseWriter, ar *AdminRequest) { writeOK(w, "done") })
	h.Route(sub, "POST", "/t-typed", "T_TYPED", TierTyped,
		func(w http.ResponseWriter, ar *AdminRequest) {
			expected := ConfirmationText("SELL 42 TESTSYM", "order-1", "S9999")
			if ar.IsPreview() {
				writeOK(w, map[string]string{"confirmation_text": expected})
				return
			}
			if err := ar.RequireTyped(expected); err != nil {
				writeErr(w, http.StatusPreconditionFailed, "E_ADMIN_CONFIRMATION", err.Error())
				return
			}
			writeOK(w, "executed")
		})

	token := elevateViaHTTP(t, r, "TADM_TIER")
	post := func(path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Header.Set(TokenHeader, token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec
	}

	// CONFIRM: bare call refused; confirmed call passes.
	if rec := post("/api/v1/admin/t-confirm", `{}`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("unconfirmed CONFIRM: HTTP %d want 412", rec.Code)
	}
	if rec := post("/api/v1/admin/t-confirm", `{"confirmed":true}`); rec.Code != http.StatusOK {
		t.Fatalf("confirmed CONFIRM: HTTP %d body=%s", rec.Code, rec.Body.String())
	}

	// TYPED: bare call refused; preview returns the text; wrong text refused;
	// exact text executes.
	if rec := post("/api/v1/admin/t-typed", `{}`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("bare TYPED: HTTP %d want 412", rec.Code)
	}
	rec := post("/api/v1/admin/t-typed", `{"preview":true}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "SELL 42 TESTSYM order-1 FOR S9999") {
		t.Fatalf("TYPED preview: HTTP %d %s", rec.Code, rec.Body.String())
	}
	if rec := post("/api/v1/admin/t-typed", `{"confirmation_text":"SELL WRONG"}`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("wrong typed text: HTTP %d want 412", rec.Code)
	}
	if rec := post("/api/v1/admin/t-typed", `{"confirmation_text":"SELL 42 TESTSYM order-1 FOR S9999"}`); rec.Code != http.StatusOK {
		t.Fatalf("exact typed text: HTTP %d %s", rec.Code, rec.Body.String())
	}
}

func TestHTTP_AuditCSVExport(t *testing.T) {
	db := openAdminTestDB(t)
	seedAdmin(t, db, "TADM_CSV", true)
	r, _ := newTestRouter(t, db)
	token := elevateViaHTTP(t, r, "TADM_CSV")

	req := httptest.NewRequest("GET", "/api/v1/admin/audit?admin_id=TADM_CSV&format=csv", nil)
	req.Header.Set(TokenHeader, token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("csv export: HTTP %d %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/csv") {
		t.Fatalf("content-type = %q, want text/csv", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "admin_audit_") {
		t.Fatalf("content-disposition = %q", cd)
	}
	lines := strings.Split(strings.TrimSpace(rec.Body.String()), "\n")
	if len(lines) < 2 { // header + at least the ELEVATE row
		t.Fatalf("csv rows = %d, want >= 2:\n%s", len(lines), rec.Body.String())
	}
	if !strings.HasPrefix(lines[0], "id,created_at_utc,admin_id,action") {
		t.Fatalf("csv header wrong: %s", lines[0])
	}
	if !strings.Contains(rec.Body.String(), "TADM_CSV,ELEVATE") {
		t.Fatalf("elevation row missing from csv:\n%s", rec.Body.String())
	}
}

func TestAuditAppendOnly_TriggerBlocksRewrites(t *testing.T) {
	db := openAdminTestDB(t)
	// Apply migration 020 (idempotent) — the trigger under test.
	mig, err := os.ReadFile(filepath.Join("..", "..", "..", "user-config", "migrations", "020_admin_hardening.sql"))
	if err != nil {
		t.Fatalf("read migration 020: %v", err)
	}
	if _, err := db.Exec(string(mig)); err != nil {
		t.Fatalf("apply migration 020: %v", err)
	}

	store := NewStore(db)
	if err := store.Audit(context.Background(), AuditEntry{
		AdminID: "TADM_IMMUT", Action: "TEST", Tier: "READ", Result: "OK",
	}); err != nil {
		t.Fatalf("audit insert: %v", err)
	}

	// UPDATE and DELETE must be refused for EVERYONE — superuser included
	// (this test connects as postgres, the most privileged case).
	if _, err := db.Exec(`UPDATE admin_audit SET result='TAMPERED' WHERE admin_id='TADM_IMMUT'`); err == nil {
		t.Fatal("UPDATE on admin_audit must be refused by the append-only trigger")
	} else if !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("unexpected UPDATE error: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM admin_audit WHERE admin_id='TADM_IMMUT'`); err == nil {
		t.Fatal("DELETE on admin_audit must be refused by the append-only trigger")
	}
	// Inserts still work — append-only, not read-only.
	if err := store.Audit(context.Background(), AuditEntry{
		AdminID: "TADM_IMMUT", Action: "TEST2", Tier: "READ", Result: "OK",
	}); err != nil {
		t.Fatalf("insert after trigger: %v", err)
	}
}
