package middleware

// TokenCapture — the "user logs in anywhere → backend session refreshed"
// invariant. Guards the 2026-08-05 incident class: app validated requests all
// morning but the stored broker token stayed stale → AU004 → no SLs placed.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sync"
	"testing"
	"time"

	pbc "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
)

func jsonMarshal(m map[string]interface{}) ([]byte, error) { return json.Marshal(m) }
func base64RawURL(b []byte) string                         { return base64.RawURLEncoding.EncodeToString(b) }

type fakeCredsClient struct {
	mu         sync.Mutex
	storedTok  string // token GetUserCredentials returns
	pushed     []*pb.UpdateUserCredentialsRequest
	getCalls   int
	failUpdate bool
}

func (f *fakeCredsClient) GetUserCredentials(_ context.Context, _ *pb.GetUserCredentialsRequest) (*pb.GetUserCredentialsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return &pb.GetUserCredentialsResponse{
		Success:    true,
		IndiraAuth: &pbc.IndiraAuthContext{BearerToken: f.storedTok},
	}, nil
}

func (f *fakeCredsClient) UpdateUserCredentials(_ context.Context, req *pb.UpdateUserCredentialsRequest) (*pb.UpdateUserCredentialsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failUpdate {
		return &pb.UpdateUserCredentialsResponse{Success: false}, nil
	}
	f.pushed = append(f.pushed, req)
	return &pb.UpdateUserCredentialsResponse{Success: true}, nil
}

func (f *fakeCredsClient) pushCount() int  { f.mu.Lock(); defer f.mu.Unlock(); return len(f.pushed) }
func (f *fakeCredsClient) getCount() int   { f.mu.Lock(); defer f.mu.Unlock(); return f.getCalls }

// mkJWT builds an unsigned but parseable JWT with the given claims — the
// capture path only reads the payload (Codifi verification already happened
// upstream in the middleware).
func mkJWT(t *testing.T, claims map[string]interface{}) (string, *auth.Claims) {
	t.Helper()
	jwt := buildJWT(claims)
	raw, err := auth.ParsePayload(jwt)
	if err != nil {
		t.Fatalf("test jwt does not parse: %v", err)
	}
	uid, _ := raw["userId"].(string)
	return jwt, &auth.Claims{UserID: uid, Raw: raw}
}

func buildJWT(claims map[string]interface{}) string {
	enc := func(m map[string]interface{}) string {
		b, _ := jsonMarshal(m)
		return base64RawURL(b)
	}
	return enc(map[string]interface{}{"alg": "HS512"}) + "." + enc(claims) + ".sig"
}

// waitFor polls until cond or 2s timeout — reconcile runs on a goroutine.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestMaybeCapture_PushesOnlyWhenNewer(t *testing.T) {
	oldTok := buildJWT(map[string]interface{}{"userId": "S4450", "iat": float64(1000)})
	fake := &fakeCredsClient{storedTok: oldTok}
	tc := NewTokenCapture(fake)

	// Header token NEWER than stored → must push, with SSO source mapping.
	jwt, claims := mkJWT(t, map[string]interface{}{
		"userId": "S4450", "appId": "app-1", "loginSource": "SSO", "iat": float64(2000),
	})
	tc.MaybeCapture(claims, jwt)
	waitFor(t, "push of newer token", func() bool { return fake.pushCount() == 1 })

	fake.mu.Lock()
	req := fake.pushed[0]
	fake.mu.Unlock()
	if req.GetUserId() != "S4450" || req.GetIndiraAuth().GetBearerToken() != jwt {
		t.Fatalf("pushed wrong payload: %+v", req)
	}
	if req.GetIndiraAuth().GetSource() != "SSO" || req.GetIndiraAuth().GetAppId() != "app-1" {
		t.Errorf("source/appId mapping wrong: %+v", req.GetIndiraAuth())
	}

	// Same token again → fast path, no extra Get/Update.
	gets := fake.getCount()
	tc.MaybeCapture(claims, jwt)
	time.Sleep(50 * time.Millisecond)
	if fake.pushCount() != 1 || fake.getCount() != gets {
		t.Errorf("re-capture of same token must be a no-op (pushes=%d gets=%d)", fake.pushCount(), fake.getCount())
	}
}

func TestMaybeCapture_NeverDowngrades(t *testing.T) {
	newTok := buildJWT(map[string]interface{}{"userId": "S4450", "iat": float64(5000)})
	fake := &fakeCredsClient{storedTok: newTok}
	tc := NewTokenCapture(fake)

	// Header token OLDER than stored (stale tab) → must NOT push.
	jwt, claims := mkJWT(t, map[string]interface{}{
		"userId": "S4450", "appId": "app-1", "loginSource": "SSO", "iat": float64(4000),
	})
	tc.MaybeCapture(claims, jwt)
	waitFor(t, "reconcile to seed knownIat", func() bool { return fake.getCount() >= 1 })
	time.Sleep(50 * time.Millisecond)
	if fake.pushCount() != 0 {
		t.Fatalf("older token must never replace newer stored session (pushes=%d)", fake.pushCount())
	}
}

func TestMaybeCapture_APPSourceMapsToAND(t *testing.T) {
	fake := &fakeCredsClient{storedTok: ""} // nothing stored → any token pushes
	tc := NewTokenCapture(fake)
	jwt, claims := mkJWT(t, map[string]interface{}{
		"userId": "S4450", "appId": "app-2", "loginSource": "APP", "iat": float64(3000),
	})
	tc.MaybeCapture(claims, jwt)
	waitFor(t, "APP push", func() bool { return fake.pushCount() == 1 })
	fake.mu.Lock()
	src := fake.pushed[0].GetIndiraAuth().GetSource()
	fake.mu.Unlock()
	if src != "AND" {
		t.Errorf("APP token source = %q, want AND (proven sso:False path)", src)
	}
}

func TestMaybeCapture_NilAndNoIatSafe(t *testing.T) {
	var nilTC *TokenCapture
	nilTC.MaybeCapture(&auth.Claims{UserID: "X"}, "t") // must not panic

	fake := &fakeCredsClient{}
	tc := NewTokenCapture(fake)
	_, claims := mkJWT(t, map[string]interface{}{"userId": "S4450"}) // no iat
	tc.MaybeCapture(claims, "tok")
	time.Sleep(30 * time.Millisecond)
	if fake.pushCount() != 0 || fake.getCount() != 0 {
		t.Error("token without iat must never auto-push")
	}
}
