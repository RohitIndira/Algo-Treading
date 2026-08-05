package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// IntrospectionVerifier implements Verifier by delegating JWT validity
// checks to Codifi's /auth-services/api/auth/verify/token endpoint,
// with an in-memory cache to keep the network round-trip off the hot
// path and a blacklist to support server-side revocation on /logout.
//
// This is Pattern 4 — "cached introspection" — and is intended as a
// TEMPORARY BRIDGE until Codifi shares the HS512 signing secret.
// When that arrives, LocalKeyVerifier (Pattern 2) replaces this file
// with a one-line change in main.go. See project_api_gateway_auth_strategy.md.
//
// Verify flow, per request:
//
//	 1. Blacklist check — has this JWT been /logout'd?     (fast, in-memory)
//	 2. ParsePayload    — extract userId + appId locally    (fast, no IO)
//	 3. Cache check     — have we recently seen this JWT?  (fast, in-memory)
//	     hit  → return cached Claims           ~1 ms total
//	     miss → step 4
//	 4. Codifi call     — GET /verify/token?...            ~100-200 ms
//	 5. Parse response  — code=="200" → cache + return Claims
//	                    else → negative-cache + return ErrTokenInvalid
//
// With a 5-minute cache TTL and typical user click patterns, ~95% of
// requests hit the cache and cost ~1 ms; the other ~5% cost ~200 ms
// (the once-per-5-min re-verification).

// IntrospectionConfig configures IntrospectionVerifier. All fields
// have sensible zero-value defaults except VerifyURL, which MUST be
// set (there's no sane default that isn't environment-specific).
type IntrospectionConfig struct {
	// VerifyURL is the base URL of Codifi's SSO verify endpoint. Query
	// string is added at Verify time. Called for tokens whose
	// `loginSource` claim is "SSO" (web login via SSO gateway). Example:
	//   https://livemiddleware.indiratrade.com/auth-services/api/auth/verify/token
	VerifyURL string

	// AppVerifyURL is the endpoint we probe for MOBILE-APP token
	// verification (tokens whose `loginSource` claim is "APP").
	//
	// PIGGYBACK APPROACH (2026-07-02): we empirically discovered that
	// Codifi's dedicated /auth-services/api/auth/v1/verify/token
	// endpoint is over-strict — it rejects tokens (with AU004
	// "Session expired") that Codifi's own DATA endpoints accept as
	// valid. This means /verify/token cannot be used to validate the
	// same JWTs the mobile app uses successfully in production.
	//
	// Workaround: we call a lightweight authenticated data endpoint
	// (/user-services/api/user/v1/AccountInfo) as our validity probe.
	// If the response is infoID:"0" → JWT is valid. Otherwise rejected.
	// This matches the exact auth check the mobile app itself relies
	// on for every real API call.
	//
	// This endpoint requires the JWT as `Authorization: Bearer` header
	// (NOT as ?token= query param — unlike the SSO verify endpoint).
	// See the callAPPVerify helper below for the request shape.
	//
	// Long-term fix: obtain the HS512 signing secret from Codifi and
	// switch to LocalKeyVerifier (Pattern 2). Then we verify signature
	// locally in ~1ms and stop depending on any Codifi endpoint's
	// auth semantics.
	//
	// Example: https://livemiddleware.indiratrade.com/user-services/api/user/v1/AccountInfo
	AppVerifyURL string

	// UnifiedVerifyURL is Codifi's session-validation endpoint that accepts
	// BOTH web-SSO and mobile-APP tokens (2026-08-05, per Codifi guidance):
	//   GET  Authorization: Bearer <jwt>   source: API
	// Response envelope matches the legacy SSO verify endpoint
	// ({"code":"200","message":"Valid Token",...}), so
	// parseSSOVerifyResponse applies. This is now the PRIMARY verify path
	// for every token; AppVerifyURL remains the defensive fallback for APP
	// tokens (see Verify step 5). Defaulted when empty:
	//   https://livemiddleware.indiratrade.com/auth-services/api/auth/v1/verify/token
	UnifiedVerifyURL string

	// HTTPTimeout is the per-request timeout when calling Codifi.
	// Default 3s. If set to 0, defaults are applied.
	HTTPTimeout time.Duration

	// CacheTTL is how long a successful verification is cached.
	// Default 5 min. Balance: longer = fewer Codifi calls, wider
	// revocation gap. 5 min is the industry standard.
	CacheTTL time.Duration

	// NegativeTTL is how long a rejection is cached. Default 30s.
	// Short by design — prevents brute-forcing garbage tokens from
	// hitting Codifi on every attempt, but doesn't hold onto
	// negative results long enough to matter if Codifi rotates keys.
	NegativeTTL time.Duration

	// CleanupPeriod is how often the sweep goroutine runs. Default 1 min.
	CleanupPeriod time.Duration
}

// cacheEntry is one record in the in-memory cache. Both valid and
// negative entries use this shape; the `valid` bool distinguishes
// them.
type cacheEntry struct {
	claims    *Claims   // nil for negative entries
	valid     bool      // true = cached success; false = cached rejection
	expiresAt time.Time // when this cache entry becomes stale
}

// IntrospectionVerifier is the concrete Verifier implementation
// backed by Codifi's introspection endpoint. Safe for concurrent
// use — all state is behind sync.Map.
type IntrospectionVerifier struct {
	cfg        IntrospectionConfig
	httpClient *http.Client

	// cache : map[jwtString]cacheEntry — remembers verification results
	cache sync.Map

	// blacklist : map[jwtString]time.Time — revocation list populated
	// by Revoke() on /logout. Key is the JWT, value is when the
	// blacklist entry itself expires (typically the JWT's own exp).
	blacklist sync.Map

	stopCh chan struct{} // signals cleanup goroutine to exit
}

// NewIntrospectionVerifier builds an IntrospectionVerifier from cfg
// and starts a background sweep goroutine. Zero-value cfg fields
// receive sane defaults. VerifyURL + AppVerifyURL are BOTH required
// and panic if empty (a missing verify URL is a startup mis-
// configuration, not a runtime condition worth returning an error for).
func NewIntrospectionVerifier(cfg IntrospectionConfig) *IntrospectionVerifier {
	if cfg.VerifyURL == "" {
		panic("auth.NewIntrospectionVerifier: cfg.VerifyURL is required")
	}
	if cfg.AppVerifyURL == "" {
		panic("auth.NewIntrospectionVerifier: cfg.AppVerifyURL is required")
	}
	if cfg.UnifiedVerifyURL == "" {
		// Safe default: the unified endpoint is environment-stable (prod
		// Codifi); callers can still override via CODIFI_UNIFIED_VERIFY_URL.
		cfg.UnifiedVerifyURL = "https://livemiddleware.indiratrade.com/auth-services/api/auth/v1/verify/token"
	}
	if cfg.HTTPTimeout == 0 {
		cfg.HTTPTimeout = 3 * time.Second
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 5 * time.Minute
	}
	if cfg.NegativeTTL == 0 {
		cfg.NegativeTTL = 30 * time.Second
	}
	if cfg.CleanupPeriod == 0 {
		cfg.CleanupPeriod = 1 * time.Minute
	}

	iv := &IntrospectionVerifier{
		cfg: cfg,
		httpClient: &http.Client{
			// NEVER use http.DefaultClient in production — it has
			// no timeout. A slow/hung Codifi would block us forever.
			Timeout: cfg.HTTPTimeout,
		},
		stopCh: make(chan struct{}),
	}
	go iv.cleanupLoop()
	return iv
}

// Verify implements the Verifier interface. See the file-level doc
// for the 5-step flow.
func (iv *IntrospectionVerifier) Verify(ctx context.Context, jwt string) (*Claims, error) {
	// ── Step 1 ── Blacklist check ────────────────────────────────
	// If the JWT has been /logout'd on our side, refuse it BEFORE
	// even looking at the cache — otherwise a still-cached entry
	// could accidentally re-authorize a revoked session.
	if v, ok := iv.blacklist.Load(jwt); ok {
		blacklistedUntil := v.(time.Time)
		if time.Now().Before(blacklistedUntil) {
			return nil, fmt.Errorf("%w: token revoked", ErrTokenInvalid)
		}
		// Expired blacklist entry — evict lazily.
		iv.blacklist.Delete(jwt)
	}

	// ── Step 2 ── Parse the JWT payload locally ──────────────────
	// Codifi's verify endpoint requires userId + appId as HTTP
	// headers (they cross-check them against the JWT's own claims).
	// So we MUST extract them from the payload ourselves BEFORE
	// calling Codifi. We also read loginSource here to decide WHICH
	// Codifi verify endpoint to route to (SSO vs APP use different
	// endpoints with different signing secrets on Codifi's side).
	raw, err := ParsePayload(jwt)
	if err != nil {
		return nil, err // already wrapped with ErrTokenMalformed
	}
	userID, _ := raw["userId"].(string)
	appID, _ := raw["appId"].(string)
	loginSource, _ := raw["loginSource"].(string)
	if userID == "" || appID == "" {
		return nil, fmt.Errorf("%w: payload missing userId or appId", ErrNoUserID)
	}

	// ── Step 3 ── Cache check ────────────────────────────────────
	if v, ok := iv.cache.Load(jwt); ok {
		e := v.(cacheEntry)
		if time.Now().Before(e.expiresAt) {
			if e.valid {
				return e.claims, nil
			}
			// Cached rejection — respect it, don't re-hit Codifi.
			return nil, fmt.Errorf("%w: cached rejection", ErrTokenInvalid)
		}
		// Cache expired — fall through to Codifi. Sweep will evict.
	}

	// ── Step 4 ── Fail-closed on unknown loginSource ─────────────
	// If Codifi ever adds a new flow (PARTNER, API_KEY, etc), tokens
	// from that flow return 401 until we teach our verifier about it.
	if loginSource != "SSO" && loginSource != "APP" {
		return nil, fmt.Errorf("%w: unknown loginSource %q",
			ErrTokenInvalid, loginSource)
	}

	// ── Step 5 ── Unified verify (2026-08-05 switch) ─────────────
	// Codifi's /auth-services/api/auth/v1/verify/token now validates
	// BOTH web-SSO and mobile-APP sessions with one call shape:
	// Authorization: Bearer <jwt> + source header. Response envelope
	// is the same {"code","message","plain-message"} as the legacy
	// SSO endpoint, so parseSSOVerifyResponse applies to both.
	body, err2 := iv.callUnifiedVerify(ctx, iv.cfg.UnifiedVerifyURL, jwt)
	if err2 != nil {
		return nil, err2 // already wrapped with ErrTokenInvalid
	}
	verdictErr := parseSSOVerifyResponse(body)

	// APP fallback: the dedicated verify endpoint has HISTORICALLY been
	// over-strict for mobile tokens (rejected JWTs the app itself used
	// successfully — the reason the AccountInfo piggyback exists). If
	// the unified endpoint rejects an APP token, double-check against
	// the AccountInfo probe before treating the session as dead.
	if verdictErr != nil && loginSource == "APP" {
		if body2, err3 := iv.callAPPVerify(ctx, iv.cfg.AppVerifyURL, jwt, userID, appID); err3 == nil {
			if appErr := parseAPPVerifyResponse(body2); appErr == nil {
				verdictErr = nil // AccountInfo accepts the token — session is live
			}
		}
	}

	// ── Step 6 ── Verdict ────────────────────────────────────────
	if verdictErr != nil {
		// Negative-cache the rejection so brute-force garbage tokens
		// don't hammer Codifi on every attempt.
		iv.cache.Store(jwt, cacheEntry{
			valid:     false,
			expiresAt: time.Now().Add(iv.cfg.NegativeTTL),
		})
		return nil, verdictErr
	}

	// ── Step 7 ── Success — build Claims ─────────────────────────
	// SECURITY: use the userId we extracted from the JWT payload,
	// NOT any field the verify response may echo back. Codifi's
	// verify response `ucc` field is a MIRROR of the header we
	// sent, not a verified value — trusting it would let a wrong
	// header override the token's actual identity.
	var expiresAt int64
	if expF, ok := raw["exp"].(float64); ok {
		expiresAt = int64(expF)
	}

	claims := &Claims{
		UserID:    userID,
		ExpiresAt: expiresAt,
		Raw:       raw,
	}

	// Cache the success for CacheTTL. Subsequent requests with this
	// same JWT (within TTL) hit cache and skip Codifi entirely.
	iv.cache.Store(jwt, cacheEntry{
		claims:    claims,
		valid:     true,
		expiresAt: time.Now().Add(iv.cfg.CacheTTL),
	})

	return claims, nil
}

// callUnifiedVerify makes the HTTP GET to Codifi's unified session-verify
// endpoint (/auth-services/api/auth/v1/verify/token). Contract (verified
// live 2026-08-05 with a fresh SSO token → {"code":"200","message":"Valid
// Token"}):
//   - JWT in `Authorization: Bearer` header (NOT ?token= query param)
//   - `source: API` header identifies a server-side verification call
// Works for both loginSource=SSO and loginSource=APP tokens.
func (iv *IntrospectionVerifier) callUnifiedVerify(ctx context.Context, verifyURL, jwt string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, verifyURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: unified request build: %v", ErrTokenInvalid, err)
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("source", "API")
	return iv.doAndReadBody(req, verifyURL)
}

// callSSOVerify makes the HTTP GET to Codifi's SSO verify endpoint
// (/auth-services/api/auth/verify/token). The SSO endpoint takes the
// JWT as the `?token=` QUERY PARAMETER — NOT an Authorization header.
// That's a Codifi choice; passing the JWT the other way returns 404.
//
// LEGACY (2026-08-05): superseded by callUnifiedVerify as the primary
// path; kept for rollback via CODIFI_VERIFY_URL if ever needed.
func (iv *IntrospectionVerifier) callSSOVerify(ctx context.Context, verifyURL, jwt, userID, appID string) ([]byte, error) {
	reqURL, err := url.Parse(verifyURL)
	if err != nil {
		log.Printf("auth: bad SSO verify URL config %q: %v", verifyURL, err)
		return nil, fmt.Errorf("%w: verify URL misconfigured", ErrTokenInvalid)
	}
	q := reqURL.Query()
	q.Set("token", jwt)
	reqURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: SSO request build: %v", ErrTokenInvalid, err)
	}
	req.Header.Set("userId", userID)
	req.Header.Set("appId", appID)
	return iv.doAndReadBody(req, verifyURL)
}

// callAPPVerify makes the HTTP GET to Codifi's mobile-app probe endpoint.
//
// PIGGYBACK: Codifi's dedicated /auth/v1/verify/token endpoint is
// over-strict for mobile tokens (rejects with AU004 even when the JWT
// is valid on real data endpoints). So we instead probe an authenticated
// DATA endpoint (default: /user-services/api/user/v1/AccountInfo). If
// Codifi accepts the JWT there, it accepts it everywhere.
//
// The probe endpoint follows the MOBILE APP contract: JWT goes in
// `Authorization: Bearer ...` header (NOT a query param). We also send
// userId, appId, and source headers to match what the mobile app itself
// sends — matches the exact request shape that Codifi validates.
//
// Response shape is the Indira envelope ({infoID, infoMsg, ...}) — the
// SAME shape as /auth/v1/verify/token — so parseAPPVerifyResponse works
// unchanged.
//
// Side-effect note: the /AccountInfo response body contains user PII
// (name, PAN, email, mobile, bank details). We parse ONLY infoID/
// infoMsg via a narrow struct in parseAPPVerifyResponse — the PII fields
// are discarded during JSON unmarshal. Never log the response body.
func (iv *IntrospectionVerifier) callAPPVerify(ctx context.Context, verifyURL, jwt, userID, appID string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, verifyURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: APP request build: %v", ErrTokenInvalid, err)
	}
	// Mobile app contract: JWT in Authorization header, userId + appId
	// echoed as headers, source identifying the caller platform. We
	// pick "AND" (Android) as the source since Codifi's endpoints accept
	// it regardless of what the real caller is — the value primarily
	// gates certain platform-specific response paths we don't invoke.
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("userId", userID)
	req.Header.Set("appId", appID)
	req.Header.Set("source", "AND")
	return iv.doAndReadBody(req, verifyURL)
}

// doAndReadBody executes the request through iv.httpClient, ensures
// the response body is closed, and returns the raw bytes. Shared by
// callSSOVerify and callAPPVerify — the only differences between the
// two are URL + header shape, both handled by the caller.
func (iv *IntrospectionVerifier) doAndReadBody(req *http.Request, verifyURL string) ([]byte, error) {
	resp, err := iv.httpClient.Do(req)
	if err != nil {
		// Network error — Codifi unreachable, DNS failure, timeout,
		// TLS error, etc. Log for ops (this is how we'd notice a
		// Codifi outage), fail closed (don't authorize).
		log.Printf("auth: Codifi verify network error (%s): %v", verifyURL, err)
		return nil, fmt.Errorf("%w: verify network error", ErrTokenInvalid)
	}
	defer resp.Body.Close() // MANDATORY — leaks the TCP connection otherwise.

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: verify body read: %v", ErrTokenInvalid, err)
	}
	return body, nil
}

// parseSSOVerifyResponse decodes the SSO-endpoint response shape:
//
//	{"code":"200","message":"Valid Token","plain-message":{"ucc":"...","timestamp":...,"callback-url":""}}
//
// Returns nil on success (code == "200") or a wrapped ErrTokenInvalid
// with the human-readable Codifi message on any rejection. Note that
// the HTTP status is ALWAYS 200 for this endpoint even on rejection;
// only the body `code` field distinguishes valid from invalid.
func parseSSOVerifyResponse(body []byte) error {
	var r struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("%w: SSO verify response parse: %v",
			ErrTokenInvalid, err)
	}
	if r.Code == "200" {
		return nil
	}
	return fmt.Errorf("%w: Codifi SSO rejected: %s (code=%s)",
		ErrTokenInvalid, r.Message, r.Code)
}

// parseAPPVerifyResponse decodes the APP-endpoint response shape
// (Indira-envelope style):
//
//	{"infoID":"0"|"AU004"|..., "infoMsg":"...", "data":{}, "timestamp":...}
//
// Returns:
//   - nil                          when infoID == "0" (valid token)
//   - ErrTokenSessionExpired-wrap  when infoID == "AU004" (server-side logout)
//   - ErrTokenInvalid-wrap         for any other non-"0" infoID
//
// The AU004 distinction lets middleware surface E_AUTH_EXPIRED to the
// frontend (which typically prompts silent re-login) vs the generic
// E_AUTH_INVALID_SIGNATURE (which typically prompts full logout).
func parseAPPVerifyResponse(body []byte) error {
	var r struct {
		InfoID  string `json:"infoID"`
		InfoMsg string `json:"infoMsg"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("%w: APP verify response parse: %v",
			ErrTokenInvalid, err)
	}
	switch r.InfoID {
	case "0":
		return nil
	case "AU004":
		return fmt.Errorf("%w: %s", ErrTokenSessionExpired, r.InfoMsg)
	default:
		return fmt.Errorf("%w: Codifi APP rejected: %s (infoID=%s)",
			ErrTokenInvalid, r.InfoMsg, r.InfoID)
	}
}

// Revoke removes the given JWT from the cache and adds it to the
// blacklist until the JWT's own `exp` (or 24h fallback if we can't
// parse it). Called by the /logout handler.
//
// This is the mechanism that closes the "revocation gap": Codifi
// keeps trusting the JWT until its natural expiry, but OUR gateway
// blocks it instantly on user logout. Attacker with a stolen JWT
// can't reach anything through our API even though Codifi still
// vouches for it.
func (iv *IntrospectionVerifier) Revoke(jwt string) {
	// Remove from cache first so any concurrent request in step 3
	// misses and falls through to blacklist-check-then-Codifi.
	iv.cache.Delete(jwt)

	// Compute how long to keep the blacklist entry.
	// Ideal: until the JWT's own exp. That's the exact window during
	// which Codifi would still accept it. After exp, blacklist entry
	// is useless (Codifi rejects anyway), so we drop it.
	blacklistUntil := time.Now().Add(24 * time.Hour) // conservative fallback
	if raw, err := ParsePayload(jwt); err == nil {
		if expF, ok := raw["exp"].(float64); ok {
			jwtExp := time.Unix(int64(expF), 0)
			if jwtExp.After(time.Now()) {
				blacklistUntil = jwtExp
			}
		}
	}
	iv.blacklist.Store(jwt, blacklistUntil)
	log.Printf("auth: revoked token (blacklist until %s)",
		blacklistUntil.Format(time.RFC3339))
}

// cleanupLoop periodically evicts expired cache and blacklist
// entries. Started once by NewIntrospectionVerifier. Cancelled via
// Close() (or by process exit).
func (iv *IntrospectionVerifier) cleanupLoop() {
	t := time.NewTicker(iv.cfg.CleanupPeriod)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			iv.sweep()
		case <-iv.stopCh:
			return
		}
	}
}

// sweep walks the cache and blacklist maps once, deleting any entry
// whose expiresAt is in the past. Cheap because sync.Map.Range does
// not hold any global lock — concurrent Verify/Revoke calls proceed
// undisturbed.
func (iv *IntrospectionVerifier) sweep() {
	now := time.Now()
	var (
		removedCache, removedBL int
	)

	iv.cache.Range(func(k, v interface{}) bool {
		if e := v.(cacheEntry); now.After(e.expiresAt) {
			iv.cache.Delete(k)
			removedCache++
		}
		return true
	})
	iv.blacklist.Range(func(k, v interface{}) bool {
		if exp := v.(time.Time); now.After(exp) {
			iv.blacklist.Delete(k)
			removedBL++
		}
		return true
	})

	if removedCache > 0 || removedBL > 0 {
		// Ops signal — helps confirm the sweeper is working. Debug-
		// level, not error-level; healthy operation, not incident.
		log.Printf("auth: sweep evicted %d cache + %d blacklist entries",
			removedCache, removedBL)
	}
}

// Close stops the background cleanup goroutine. Safe to call once;
// subsequent calls block forever (as usual with closed channels).
// Currently unused — process exit is our shutdown path — but kept
// for tests and future graceful-shutdown wiring.
func (iv *IntrospectionVerifier) Close() {
	close(iv.stopCh)
}

// Compile-time assertion: *IntrospectionVerifier satisfies Verifier.
var _ Verifier = (*IntrospectionVerifier)(nil)
