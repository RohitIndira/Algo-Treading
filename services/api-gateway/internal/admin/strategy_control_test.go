package admin

// M4 tests. gRPC is stubbed (recording calls for on-behalf verification);
// timeline and blocks run against the real local trading_db — the SAME DB
// rules-engine writes manthan_cooldown / manthan_signal_decisions to on
// prod (its main pool; the raw signal feed lives elsewhere — the
// 2026-08-31 wrong-DB deploy bug this pins down). The HTTP chain exercises
// CONFIRM pause/resume, the TYPED delete preview→retype handshake, and the
// audited block-clear.

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
)

type stubStrategyRPC struct {
	calls []string // "verb:strategy:user"
	fail  bool
}

func (s *stubStrategyRPC) record(verb, sid, uid string) {
	s.calls = append(s.calls, fmt.Sprintf("%s:%s:%s", verb, sid, uid))
}
func (s *stubStrategyRPC) ActivateStrategy(_ context.Context, r *pb.ActivateStrategyRequest) (*pb.ActivateStrategyResponse, error) {
	s.record("activate", r.StrategyId, r.UserId)
	return &pb.ActivateStrategyResponse{Success: !s.fail}, nil
}
func (s *stubStrategyRPC) DeactivateStrategy(_ context.Context, r *pb.DeactivateStrategyRequest) (*pb.DeactivateStrategyResponse, error) {
	s.record("deactivate", r.StrategyId, r.UserId)
	return &pb.DeactivateStrategyResponse{Success: !s.fail}, nil
}
func (s *stubStrategyRPC) DeleteStrategy(_ context.Context, r *pb.DeleteStrategyRequest) (*pb.DeleteStrategyResponse, error) {
	s.record(fmt.Sprintf("delete[%s]", r.PositionHandling), r.StrategyId, r.UserId)
	return &pb.DeleteStrategyResponse{Success: !s.fail}, nil
}

func cleanupM4(t *testing.T, trading *sql.DB) {
	t.Helper()
	_, _ = trading.Exec(`DELETE FROM manthan_cooldown WHERE strategy_id = $1::uuid`, m2Strat)
	_, _ = trading.Exec(`DELETE FROM manthan_signal_decisions WHERE strategy_id = $1::uuid`, m2Strat)
}

func newM4Router(t *testing.T) (root http.Handler, rpc *stubStrategyRPC, token string, trading *sql.DB) {
	t.Helper()
	tradingDB, execDB, posDB := openFleetDBs(t)
	seedFleetFixtures(t, tradingDB, execDB, posDB, time.Now())
	cleanupM4(t, tradingDB)
	t.Cleanup(func() { cleanupM4(t, tradingDB) })

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M4ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	fleet := NewFleetStore(tradingDB, execDB, posDB)
	h.SetFleetStore(fleet)
	rpc = &stubStrategyRPC{}
	h.SetStrategyControl(NewStrategyControl(rpc, fleet))
	r := newRouterFor(t, h)
	return r, rpc, elevateViaHTTP(t, r, "TADM_M4ADMIN"), tradingDB
}

func m4do(t *testing.T, root http.Handler, token, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	req.Header.Set(TokenHeader, token)
	rec := httptest.NewRecorder()
	root.ServeHTTP(rec, req)
	return rec
}

func TestM4_PauseResume_OnBehalf(t *testing.T) {
	root, rpc, token, _ := newM4Router(t)
	base := "/api/v1/admin/strategies/" + m2Strat

	// Unconfirmed → 412, no RPC fired.
	if rec := m4do(t, root, token, "POST", base+"/pause", `{}`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("unconfirmed pause: %d", rec.Code)
	}
	if len(rpc.calls) != 0 {
		t.Fatalf("rpc fired without confirmation: %v", rpc.calls)
	}
	// Confirmed pause → deactivate carries the TARGET user's id (on-behalf).
	if rec := m4do(t, root, token, "POST", base+"/pause", `{"confirmed":true}`); rec.Code != http.StatusOK {
		t.Fatalf("pause: %d %s", rec.Code, rec.Body.String())
	}
	if len(rpc.calls) != 1 || rpc.calls[0] != "deactivate:"+m2Strat+":"+m2User {
		t.Fatalf("pause rpc = %v, want deactivate on behalf of %s", rpc.calls, m2User)
	}
	if rec := m4do(t, root, token, "POST", base+"/resume", `{"confirmed":true}`); rec.Code != http.StatusOK {
		t.Fatalf("resume: %d %s", rec.Code, rec.Body.String())
	}
	if rpc.calls[1] != "activate:"+m2Strat+":"+m2User {
		t.Fatalf("resume rpc = %v", rpc.calls)
	}

	// Unknown strategy → 404, no RPC.
	if rec := m4do(t, root, token, "POST",
		"/api/v1/admin/strategies/aaaaaaaa-9999-9999-9999-999999999999/pause", `{"confirmed":true}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown strategy pause: %d", rec.Code)
	}
}

func TestM4_Delete_TypedHandshake(t *testing.T) {
	root, rpc, token, _ := newM4Router(t)
	base := "/api/v1/admin/strategies/" + m2Strat

	// Bare delete → 412 (TYPED tier demands the handshake).
	if rec := m4do(t, root, token, "DELETE", base, `{}`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("bare delete: %d", rec.Code)
	}
	// Preview returns the blast radius with the real open-position count (3).
	rec := m4do(t, root, token, "DELETE", base, `{"preview":true}`)
	want := "DELETE STRATEGY AAAAAAAA FOR " + m2User + " — 3 OPEN POSITIONS STAY OPEN"
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("preview: %d %s (want %q)", rec.Code, rec.Body.String(), want)
	}
	// Wrong text → 412 + DENIED audit; no RPC.
	if rec := m4do(t, root, token, "DELETE", base, `{"confirmation_text":"DELETE WRONG"}`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("wrong text: %d", rec.Code)
	}
	if len(rpc.calls) != 0 {
		t.Fatalf("rpc fired on denied delete: %v", rpc.calls)
	}
	// Exact text → delete fires with KEEP_POSITIONS_OPEN on behalf.
	body := fmt.Sprintf(`{"confirmation_text":%q}`, want)
	if rec := m4do(t, root, token, "DELETE", base, body); rec.Code != http.StatusOK {
		t.Fatalf("typed delete: %d %s", rec.Code, rec.Body.String())
	}
	if len(rpc.calls) != 1 || rpc.calls[0] != "delete[KEEP_POSITIONS_OPEN]:"+m2Strat+":"+m2User {
		t.Fatalf("delete rpc = %v", rpc.calls)
	}
}

func TestM4_TimelineAndBlocks(t *testing.T) {
	root, _, token, trading := newM4Router(t)
	base := "/api/v1/admin/strategies/" + m2Strat

	// Seed lifecycle events + both block kinds.
	if _, err := trading.Exec(`
		INSERT INTO strategy_lifecycle_events (strategy_id, user_id, event_type, details)
		VALUES ($1,$2,'DEPLOYED','{"capital":500000}'), ($1,$2,'PAUSED','{}')`,
		m2Strat, m2User); err != nil {
		t.Fatalf("seed lifecycle: %v", err)
	}
	t.Cleanup(func() {
		_, _ = trading.Exec(`DELETE FROM strategy_lifecycle_events WHERE strategy_id=$1::uuid`, m2Strat)
	})
	if _, err := trading.Exec(`
		INSERT INTO manthan_cooldown (strategy_id, symbol, ath_at_exit, exit_price, reentry_below)
		VALUES ($1::uuid, 'COOLSYM', 500, 400, 400)`, m2Strat); err != nil {
		t.Fatalf("seed cooldown: %v", err)
	}
	if _, err := trading.Exec(`
		INSERT INTO manthan_signal_decisions
			(signal_id, user_id, strategy_id, symbol, ltp_at_decision,
			 intended_qty, intended_invested, initial_sl_target,
			 status, user_override_until)
		VALUES ($2::uuid, $3, $1::uuid, 'EMBSYM', 100, 10, 1000, 80,
			'CLOSED', now() + interval '2 days')`,
		m2Strat, "aaaaaaaa-5555-2222-3333-000000000eb1", m2User); err != nil {
		t.Fatalf("seed override: %v", err)
	}

	rec := m4do(t, root, token, "GET", base+"/timeline", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"DEPLOYED"`) ||
		!strings.Contains(rec.Body.String(), `"capital":500000`) {
		t.Fatalf("timeline: %d %s", rec.Code, rec.Body.String())
	}

	rec = m4do(t, root, token, "GET", base+"/blocks", "")
	body := rec.Body.String()
	if rec.Code != http.StatusOK ||
		!strings.Contains(body, `"COOLDOWN"`) || !strings.Contains(body, "COOLSYM") ||
		!strings.Contains(body, `"OVERRIDE"`) || !strings.Contains(body, "EMBSYM") {
		t.Fatalf("blocks (want both kinds): %d %s", rec.Code, body)
	}

	// Clear the cooldown (CONFIRM tier) → row flips, second clear refused.
	if rec := m4do(t, root, token, "POST", base+"/blocks/clear",
		`{"confirmed":true,"symbol":"COOLSYM","kind":"COOLDOWN"}`); rec.Code != http.StatusOK {
		t.Fatalf("clear: %d %s", rec.Code, rec.Body.String())
	}
	var cleared bool
	if err := trading.QueryRow(`SELECT cleared FROM manthan_cooldown WHERE strategy_id=$1::uuid AND symbol='COOLSYM'`,
		m2Strat).Scan(&cleared); err != nil || !cleared {
		t.Fatalf("cooldown not cleared: %v cleared=%v", err, cleared)
	}
	if rec := m4do(t, root, token, "POST", base+"/blocks/clear",
		`{"confirmed":true,"symbol":"COOLSYM","kind":"COOLDOWN"}`); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("double clear: %d want 422", rec.Code)
	}

	// Clear the manual-exit embargo → override nulled, blocks list drains.
	if rec := m4do(t, root, token, "POST", base+"/blocks/clear",
		`{"confirmed":true,"symbol":"EMBSYM","kind":"OVERRIDE"}`); rec.Code != http.StatusOK {
		t.Fatalf("clear override: %d %s", rec.Code, rec.Body.String())
	}
	var overrides int
	if err := trading.QueryRow(`SELECT COUNT(*) FROM manthan_signal_decisions
		WHERE strategy_id=$1::uuid AND user_override_until IS NOT NULL`, m2Strat).Scan(&overrides); err != nil || overrides != 0 {
		t.Fatalf("override not nulled: %v n=%d", err, overrides)
	}
	if rec := m4do(t, root, token, "GET", base+"/blocks", ""); strings.Contains(rec.Body.String(), "EMBSYM") ||
		strings.Contains(rec.Body.String(), "COOLSYM") {
		t.Fatalf("blocks not drained after clears: %s", rec.Body.String())
	}
	// Unknown kind → 400-class refusal, not a silent no-op.
	if rec := m4do(t, root, token, "POST", base+"/blocks/clear",
		`{"confirmed":true,"symbol":"X","kind":"BOGUS"}`); rec.Code < 400 {
		t.Fatalf("bogus kind accepted: %d", rec.Code)
	}
}
