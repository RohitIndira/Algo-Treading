package middleware

// TokenCapture — auto-refresh of stored broker credentials from validated
// request tokens (2026-08-05).
//
// WHY: server-side order/SL placement (morning arm, EOD AMO, safety-monitor)
// authenticates with the token STORED in user_credentials — not the request
// header. The frontend is supposed to POST /api/v1/auth/credentials after
// every login, but in practice it only does so on a full SSO login: an
// app-open with a cached session validates fine per-request yet never
// refreshes the stored token. Overnight the broker session dies (proven
// 2026-08-05: AU004 from 00:00, five positions left unprotected until a
// manual push at 07:16).
//
// WHAT: every request that passes unified verification carries a
// Codifi-validated token. If that token is NEWER (higher `iat`) than the one
// stored for the user, push it through the exact same user-config RPC the
// /auth/credentials handler uses → store + USER_CREDENTIALS_UPDATED event →
// trade-execution cache invalidation + ArmRetryWorker wake → SL re-arm.
// Any authenticated app activity after the morning login refreshes the
// backend session automatically — no frontend cooperation required.
//
// COST: one in-memory compare per request (lock-free fast path). The slow
// path (read stored creds → maybe push) runs at most once per user at a
// time, in a background goroutine, and only when the header token's iat is
// newer than the last iat this process has seen for the user.
//
// SAFETY: never replaces a newer stored token with an older header token —
// the slow path re-reads the stored token and compares iat before pushing
// (a stale-but-still-valid token from an old tab is a no-op).

import (
	"context"
	"log"
	"sync"
	"time"

	pbc "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
)

// CredentialsClient is the slice of the user-config gRPC client TokenCapture
// needs. *grpc_clients.UserConfigClient satisfies it; tests plug a fake.
type CredentialsClient interface {
	GetUserCredentials(ctx context.Context, req *pb.GetUserCredentialsRequest) (*pb.GetUserCredentialsResponse, error)
	UpdateUserCredentials(ctx context.Context, req *pb.UpdateUserCredentialsRequest) (*pb.UpdateUserCredentialsResponse, error)
}

// TokenCapture is safe for concurrent use. Zero value is NOT usable — build
// with NewTokenCapture.
type TokenCapture struct {
	client CredentialsClient

	// knownIat: map[userID]int64 — the newest token iat this process has
	// either observed in storage or successfully pushed. Fast-path gate.
	knownIat sync.Map
	// inflight: map[userID]struct{} — at most one slow-path per user.
	inflight sync.Map
}

func NewTokenCapture(client CredentialsClient) *TokenCapture {
	return &TokenCapture{client: client}
}

// MaybeCapture is called by AuthRequired after a SUCCESSFUL verification.
// Non-blocking: the fast path is a map lookup; the slow path runs in its own
// goroutine. Nil-receiver safe so wiring stays optional.
func (tc *TokenCapture) MaybeCapture(claims *auth.Claims, rawJWT string) {
	if tc == nil || claims == nil || rawJWT == "" {
		return
	}
	iat := claimInt64(claims.Raw, "iat")
	if iat == 0 || claims.UserID == "" {
		return // no issue-time → cannot order tokens → never auto-push
	}
	if known, ok := tc.knownIat.Load(claims.UserID); ok && iat <= known.(int64) {
		return // fast path: nothing newer than what we've already reconciled
	}
	if _, busy := tc.inflight.LoadOrStore(claims.UserID, struct{}{}); busy {
		return // a slow path for this user is already running
	}
	go tc.reconcile(claims, rawJWT, iat)
}

// reconcile is the slow path: read stored creds, compare iat, push if newer.
func (tc *TokenCapture) reconcile(claims *auth.Claims, rawJWT string, headerIat int64) {
	userID := claims.UserID
	defer tc.inflight.Delete(userID)

	// Background context — the originating request may already be done.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// What's stored right now? Missing/unreadable creds → storedIat 0 → push.
	var storedIat int64
	if resp, err := tc.client.GetUserCredentials(ctx, &pb.GetUserCredentialsRequest{UserId: userID}); err == nil &&
		resp.GetSuccess() && resp.GetIndiraAuth().GetBearerToken() != "" {
		if raw, perr := auth.ParsePayload(resp.GetIndiraAuth().GetBearerToken()); perr == nil {
			storedIat = claimInt64(raw, "iat")
		}
	}
	if storedIat >= headerIat {
		// Stored token is same or newer — remember and stop. Also covers the
		// stale-old-tab case: never downgrade the stored session.
		tc.knownIat.Store(userID, storedIat)
		return
	}

	// Header token is strictly newer → push it. Same RPC + field mapping as
	// the /auth/credentials handler. Source uses the values proven live:
	// loginSource=SSO → "SSO" (sso:True middleware), APP → "AND" (sso:False).
	appID, _ := claims.Raw["appId"].(string)
	loginSource, _ := claims.Raw["loginSource"].(string)
	source := "SSO"
	if loginSource == "APP" {
		source = "AND"
	}
	resp, err := tc.client.UpdateUserCredentials(ctx, &pb.UpdateUserCredentialsRequest{
		UserId: userID,
		IndiraAuth: &pbc.IndiraAuthContext{
			UserId:      userID,
			AppId:       appID,
			Source:      source,
			BearerToken: rawJWT,
		},
	})
	if err != nil || !resp.GetSuccess() {
		// Leave knownIat untouched — the next request retries.
		log.Printf("auth: token auto-capture push FAILED user=%s iat=%d err=%v", userID, headerIat, err)
		return
	}
	tc.knownIat.Store(userID, headerIat)
	log.Printf("auth: auto-captured fresh broker token user=%s iat=%d (stored was %d) — USER_CREDENTIALS_UPDATED published", userID, headerIat, storedIat)
}

// claimInt64 reads a numeric JWT claim (JSON numbers unmarshal as float64).
func claimInt64(raw map[string]interface{}, key string) int64 {
	if raw == nil {
		return 0
	}
	if f, ok := raw[key].(float64); ok {
		return int64(f)
	}
	return 0
}
