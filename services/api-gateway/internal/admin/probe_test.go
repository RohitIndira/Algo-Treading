package admin

// M3 tests. The prober's two dependencies (user-config gRPC, the broker
// call) are injectable, so classification is tested hermetically; the
// expire path and the HTTP chain run against the real local databases.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	commonpb "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

type stubCreds struct {
	resp *pb.GetUserCredentialsResponse
	err  error
}

func (s stubCreds) GetUserCredentials(context.Context, *pb.GetUserCredentialsRequest) (*pb.GetUserCredentialsResponse, error) {
	return s.resp, s.err
}

func credsOK(user string) *pb.GetUserCredentialsResponse {
	return &pb.GetUserCredentialsResponse{
		Success: true,
		IndiraAuth: &commonpb.IndiraAuthContext{
			UserId: user, AppId: "app", Source: "AND", BearerToken: "tok-" + user,
		},
	}
}

func proberWith(creds credentialsFetcher, probeErr error) *Prober {
	return &Prober{
		creds:     creds,
		probe:     func(context.Context, *indiraClient.AuthContext) error { return probeErr },
		lastSweep: map[string]ProbeVerdict{},
	}
}

func TestProbe_Classification(t *testing.T) {
	cases := []struct {
		name     string
		creds    stubCreds
		probeErr error
		want     string
	}{
		{"live session", stubCreds{resp: credsOK("U1")}, nil, "WORKS"},
		{"dead session (AU004 chain)", stubCreds{resp: credsOK("U1")},
			fmt.Errorf("fund limit request failed: %w", indiraClient.ErrAuthExpired), "AUTH_EXPIRED"},
		{"broker/infra error", stubCreds{resp: credsOK("U1")},
			errors.New("dial tcp: i/o timeout"), "ERROR"},
		{"nothing stored", stubCreds{resp: &pb.GetUserCredentialsResponse{Success: false}}, nil, "NO_CREDENTIAL"},
		{"empty token", stubCreds{resp: &pb.GetUserCredentialsResponse{
			Success: true, IndiraAuth: &commonpb.IndiraAuthContext{UserId: "U1"}}}, nil, "NO_CREDENTIAL"},
		{"grpc down", stubCreds{err: errors.New("connection refused")}, nil, "ERROR"},
	}
	for _, c := range cases {
		v := proberWith(c.creds, c.probeErr).Probe(context.Background(), "U1")
		if v.Verdict != c.want {
			t.Errorf("%s: verdict=%s want %s (detail=%s)", c.name, v.Verdict, c.want, v.Detail)
		}
	}
}

func TestSweep_FeedsAttention(t *testing.T) {
	// Two users: one alive, one dead — only the dead one becomes CRITICAL.
	p := &Prober{
		creds: stubCreds{resp: credsOK("any")},
		probe: func(_ context.Context, a *indiraClient.AuthContext) error {
			return nil // classified per-user below via creds override
		},
		lastSweep: map[string]ProbeVerdict{},
	}
	// Simulate a completed sweep directly (the Sweep plumbing itself is
	// exercised in the HTTP test); AttentionItems must reflect it.
	p.mu.Lock()
	p.sweptAt = time.Now()
	p.lastSweep["ALIVE1"] = ProbeVerdict{UserID: "ALIVE1", Verdict: "WORKS"}
	p.lastSweep["DEAD1"] = ProbeVerdict{UserID: "DEAD1", Verdict: "AUTH_EXPIRED", Detail: "broker rejected the session"}
	p.mu.Unlock()

	items := p.sweepFailuresAsAttention()
	if len(items) != 1 || items[0].UserID != "DEAD1" ||
		items[0].Severity != "CRITICAL" || items[0].Kind != "CREDENTIAL_PROBE_FAILED" {
		t.Fatalf("attention from sweep = %+v, want one CRITICAL for DEAD1", items)
	}

	// No sweep yet → no items (absence of data must not fabricate health OR sickness).
	empty := &Prober{lastSweep: map[string]ProbeVerdict{}}
	if got := empty.sweepFailuresAsAttention(); got != nil {
		t.Fatalf("pre-sweep attention = %+v, want nil", got)
	}
}

func TestHTTP_CredentialProbeExpireSweep_EndToEnd(t *testing.T) {
	trading, exec, pos := openFleetDBs(t)
	seedFleetFixtures(t, trading, exec, pos, time.Now())

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_M3ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetFleetStore(NewFleetStore(trading, exec, pos))
	// Prober: credentials come from the stub; the broker call reports dead.
	h.SetProber(proberWith(stubCreds{resp: credsOK(m2User)},
		fmt.Errorf("verify: %w", indiraClient.ErrAuthExpired)))
	root := newRouterFor(t, h)
	token := elevateViaHTTP(t, root, "TADM_M3ADMIN")

	do := func(method, path, body string) *httptest.ResponseRecorder {
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

	// 1. Probe: stored facts (fixture cred, ~30h) + live verdict AUTH_EXPIRED.
	rec := do("GET", "/api/v1/admin/users/"+m2User+"/credential", "")
	if rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"verdict":"AUTH_EXPIRED"`) ||
		!strings.Contains(rec.Body.String(), `"stored":true`) {
		t.Fatalf("probe: %d %s", rec.Code, rec.Body.String())
	}

	// 2. Expire without confirmation → 412 (CONFIRM tier, first real use).
	rec = do("POST", "/api/v1/admin/users/"+m2User+"/credential/expire", `{}`)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("unconfirmed expire: %d want 412", rec.Code)
	}

	// 3. Expire confirmed → token blanked, timestamp epoch'd.
	rec = do("POST", "/api/v1/admin/users/"+m2User+"/credential/expire", `{"confirmed":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("confirmed expire: %d %s", rec.Code, rec.Body.String())
	}
	var tok string
	if err := exec.QueryRow(`SELECT indira_bearer_token FROM user_credentials WHERE user_id=$1`,
		m2User).Scan(&tok); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if tok != "" {
		t.Fatalf("expire didn't blank the token: %q", tok)
	}
	// Blank token = absent everywhere: facts flip to stored=false…
	rec = do("GET", "/api/v1/admin/users/"+m2User+"/credential", "")
	if !strings.Contains(rec.Body.String(), `"stored":false`) {
		t.Fatalf("facts after expire should be stored=false: %s", rec.Body.String())
	}

	// 4. Expiring again → 422 (already blank counts as nothing to expire),
	//    and a user with no row at all is refused identically.
	rec = do("POST", "/api/v1/admin/users/"+m2User+"/credential/expire", `{"confirmed":true}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("double expire: %d want 422", rec.Code)
	}
	rec = do("POST", "/api/v1/admin/users/NOBODY_XYZ/credential/expire", `{"confirmed":true}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expire missing user: %d want 422", rec.Code)
	}

	// 5. Sweep: probes the fixture user (active strategy), stores results;
	//    attention now carries the CREDENTIAL_PROBE_FAILED CRITICAL.
	rec = do("POST", "/api/v1/admin/credential-sweep", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"count":`) {
		t.Fatalf("sweep: %d %s", rec.Code, rec.Body.String())
	}
	rec = do("GET", "/api/v1/admin/attention", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "CREDENTIAL_PROBE_FAILED") {
		t.Fatalf("attention after sweep: %d %s", rec.Code, rec.Body.String())
	}

	// 6. Audit captured the mutating action.
	rec = do("GET", "/api/v1/admin/audit?admin_id=TADM_M3ADMIN&limit=20", "")
	if !strings.Contains(rec.Body.String(), "CREDENTIAL_EXPIRE") {
		t.Fatalf("expire not audited: %s", rec.Body.String())
	}
}
