package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

func TestEvalSession_NewestEvidenceWins(t *testing.T) {
	h := time.Hour
	cases := []struct {
		name       string
		e          sessionEvidence
		wantState  string
		wantReason string // substring
		needsProbe bool
	}{
		{"no credential", sessionEvidence{HasCred: false}, "EXPIRED", "no broker credential", false},
		{"sweep said expired, no login since", sessionEvidence{HasCred: true, CredAge: 40 * h, ProbeVerdict: "AUTH_EXPIRED", ProbeAge: 4 * h, AU004Age: -1, BrokerOKAge: -1}, "EXPIRED", "live probe AUTH_EXPIRED", false},
		{"sweep said expired, logged in after → verify", sessionEvidence{HasCred: true, CredAge: 10 * time.Minute, ProbeVerdict: "AUTH_EXPIRED", ProbeAge: 4 * h, AU004Age: -1, BrokerOKAge: -1}, "EXPIRED", "live probe AUTH_EXPIRED", true},
		{"probe WORKS after login", sessionEvidence{HasCred: true, CredAge: 10 * time.Minute, ProbeVerdict: "WORKS", ProbeAge: time.Minute, AU004Age: 4 * h, BrokerOKAge: -1}, "OK", "probe WORKS", false},
		{"AU004 after the login", sessionEvidence{HasCred: true, CredAge: 3 * h, ProbeVerdict: "WORKS", ProbeAge: 5 * h, AU004Age: 30 * time.Minute, AU004Symbol: "KILITCH", BrokerOKAge: 6 * h}, "EXPIRED", "rejected KILITCH with AU004", false},
		{"broker accepted an order after the AU004", sessionEvidence{HasCred: true, CredAge: 3 * h, AU004Age: 2 * h, BrokerOKAge: 20 * time.Minute}, "OK", "broker accepted", false},
		{"nothing known, old token", sessionEvidence{HasCred: true, CredAge: 30 * h, AU004Age: -1, BrokerOKAge: -1}, "STALE", "no broker activity", false},
		{"nothing known, fresh token", sessionEvidence{HasCred: true, CredAge: 5 * time.Minute, AU004Age: -1, BrokerOKAge: -1}, "UNKNOWN", "not yet verified", true},
		{"probe ERROR, old token", sessionEvidence{HasCred: true, CredAge: 30 * h, ProbeVerdict: "ERROR", ProbeAge: time.Hour, AU004Age: -1, BrokerOKAge: -1}, "STALE", "could not reach", false},
	}
	for _, c := range cases {
		got := evalSession(c.e)
		if got.State != c.wantState || !strings.Contains(got.Reason, c.wantReason) || got.NeedsProbe != c.needsProbe {
			t.Errorf("%s: got %+v, want state=%s reason~%q needsProbe=%v", c.name, got, c.wantState, c.wantReason, c.needsProbe)
		}
	}
}

// Round trip against the real local DBs: sweep says dead → CRITICAL; the
// client logs in (credential refreshed, probe now works) → item clears on the
// next poll; an AU004 after that login → CRITICAL again; a second login →
// clears again. Also proves the fleet queue no longer doubles up with the
// age heuristic.
func TestSessionHealth_ClearsOnLoginAndReturnsOnAU004(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())
	fs := NewFleetStore(trading, exec, pos)

	probeErr := error(indiraClient.ErrAuthExpired)
	p := &Prober{
		creds:     stubCreds{resp: credsOK(m2User)},
		probe:     func(context.Context, *indiraClient.AuthContext) error { return probeErr },
		lastSweep: map[string]ProbeVerdict{},
	}
	sh := NewSessionHealth(fs, p)
	sh.minGap = 0 // the dashboard's rate limit is irrelevant here
	fs.sessionItems = sh.Items
	ctx := context.Background()

	itemsFor := func() []AttentionItem {
		t.Helper()
		items, _, err := fs.Attention(ctx)
		if err != nil {
			t.Fatalf("Attention: %v", err)
		}
		var out []AttentionItem
		for _, it := range items {
			if it.UserID == m2User && (it.Kind == "AUTH_EXPIRED" || it.Kind == "STALE_CREDENTIAL" || it.Kind == "DEAD_SESSION") {
				out = append(out, it)
			}
		}
		return out
	}

	// 1. Pre-market sweep finds the session dead (fixture token is 30 h old).
	p.Sweep(ctx, []string{m2User})
	got := itemsFor()
	if len(got) != 1 || got[0].Kind != "AUTH_EXPIRED" || got[0].Severity != "CRITICAL" || got[0].Module != "session" {
		t.Fatalf("after dead sweep: %+v, want exactly one CRITICAL AUTH_EXPIRED", got)
	}

	// 2. The client logs in: fresh token stored, broker accepts it.
	probeErr = nil
	if _, err := exec.Exec(`UPDATE user_credentials SET updated_at = now() WHERE user_id = $1`, m2User); err != nil {
		t.Fatalf("simulate login: %v", err)
	}
	if got = itemsFor(); len(got) != 0 {
		t.Fatalf("after login the item must clear, got %+v", got)
	}
	if v, ok := p.LastVerdict(m2User); !ok || v.Verdict != "WORKS" {
		t.Fatalf("login must have triggered a live re-probe, got %+v", v)
	}

	// 3. The broker rejects an order with AU004 after that login.
	if _, err := exec.Exec(`
		INSERT INTO manthan_orders (signal_id, strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, status, last_error, created_at)
		VALUES ('tadm-m2-au004', $1, $2, 'ZZZ', 'LIMIT_BUY', 'BUY', 1, 0, 'REJECTED',
		        'place order request failed: indira: session expired (AU0xx): AU004 Session expired', now() + interval '1 second')`,
		m2Strat, m2User); err != nil {
		t.Fatalf("seed AU004: %v", err)
	}
	t.Cleanup(func() { _, _ = exec.Exec(`DELETE FROM manthan_orders WHERE signal_id = 'tadm-m2-au004'`) })
	time.Sleep(1200 * time.Millisecond) // AU004 row is now the newest evidence
	got = itemsFor()
	if len(got) != 1 || got[0].Kind != "AUTH_EXPIRED" || !strings.Contains(got[0].Detail, "AU004") {
		t.Fatalf("AU004 after login must re-raise: %+v", got)
	}

	// 4. Second login after the AU004 → verified live → clears again.
	if _, err := exec.Exec(`UPDATE user_credentials SET updated_at = now() + interval '3 seconds' WHERE user_id = $1`, m2User); err != nil {
		t.Fatalf("simulate second login: %v", err)
	}
	time.Sleep(3100 * time.Millisecond)
	if got = itemsFor(); len(got) != 0 {
		t.Fatalf("second login must clear the item, got %+v", got)
	}
}

// The HTTP surface: severities use the dashboard enum and the session item
// carries the user_id the UI joins on.
func TestHTTP_AttentionUsesDashboardEnum(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())
	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_SESS", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetFleetStore(NewFleetStore(trading, exec, pos))
	h.SetProber(proberWith(stubCreds{resp: credsOK(m2User)}, errors.New("fund limit: "+indiraClient.ErrAuthExpired.Error())))
	root := newRouterFor(t, h)
	token := elevateViaHTTP(t, root, "TADM_SESS")

	req := httptest.NewRequest("GET", "/api/v1/admin/attention", nil)
	req.Header.Set(TokenHeader, token)
	rec := httptest.NewRecorder()
	root.ServeHTTP(rec, req)
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("attention: %d %s", rec.Code, body)
	}
	if strings.Contains(body, `"severity":"HIGH"`) || strings.Contains(body, `"module":"M`) {
		t.Fatalf("legacy severity/module leaked: %s", body)
	}
	if !strings.Contains(body, `"severity":"WARN"`) || !strings.Contains(body, `"kind":"AMO_GIVEUP"`) {
		t.Fatalf("WARN items missing: %s", body)
	}
	if strings.Contains(body, `"kind":"DEAD_SESSION"`) {
		t.Fatalf("age heuristic must not run when session health is wired: %s", body)
	}
}
