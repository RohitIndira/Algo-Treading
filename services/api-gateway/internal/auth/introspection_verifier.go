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

	// AppVerifyURL is the base URL of Codifi's MOBILE-APP verify
	// endpoint. Called for tokens whose `loginSource` claim is "APP"
	// (mPin, biometric, or other mobile app login flows). SSO and
	// APP flows use DIFFERENT signing secrets on Codifi's side and
	// DIFFERENT response shapes, so we route each to its correct
	// endpoint. Example:
	//   https://livemiddleware.indiratrade.com/auth-services/api/auth/v1/verify/token
	//
	// Empirically discovered response shape for APP endpoint (Indira
	// envelope): {"infoID":"0"|"AU004"|..., "infoMsg":"...", "data":{}, "timestamp":...}
	AppVerifyURL string

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

	// ── Step 4 ── Pick the correct Codifi endpoint by loginSource ─
	// SSO tokens (web login) verify against /auth/verify/token,
	// APP tokens (mobile mPin/biometric) verify against
	// /auth/v1/verify/token. They use DIFFERENT signing secrets
	// AND DIFFERENT response shapes on Codifi's side. Fail-closed
	// on any unknown loginSource — if Codifi ever adds a new flow
	// (PARTNER, API_KEY, etc), tokens from that flow return 401
	// until we teach our verifier about it. Safer than silently
	// picking a default endpoint that would reject anyway.
	var verifyURL string
	var parseVerdict func(body []byte) error
	switch loginSource {
	case "SSO":
		verifyURL = iv.cfg.VerifyURL
		parseVerdict = parseSSOVerifyResponse
	case "APP":
		verifyURL = iv.cfg.AppVerifyURL
		parseVerdict = parseAPPVerifyResponse
	default:
		return nil, fmt.Errorf("%w: unknown loginSource %q",
			ErrTokenInvalid, loginSource)
	}

	// ── Step 5 ── Call the picked endpoint ───────────────────────
	body, err := iv.callCodifi(ctx, verifyURL, jwt, userID, appID)
	if err != nil {
		return nil, err // already wrapped with ErrTokenInvalid
	}

	// ── Step 6 ── Interpret response with the shape-appropriate parser
	if verdictErr := parseVerdict(body); verdictErr != nil {
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

// callCodifi builds an HTTP GET to verifyURL with ?token=<jwt> query
// param and userId/appId headers, returns the raw response body.
// Extracted from Verify so both the SSO and APP branches share the
// same HTTP-plumbing code — response INTERPRETATION differs, but the
// request shape is identical.
//
// Returns errors wrapped with ErrTokenInvalid so middleware maps them
// to a generic 401. Network errors are logged for ops so a Codifi
// outage surfaces in our own log stream.
func (iv *IntrospectionVerifier) callCodifi(ctx context.Context, verifyURL, jwt, userID, appID string) ([]byte, error) {
	reqURL, err := url.Parse(verifyURL)
	if err != nil {
		// Config-time bug, not a runtime auth problem.
		log.Printf("auth: bad verify URL config %q: %v", verifyURL, err)
		return nil, fmt.Errorf("%w: verify URL misconfigured", ErrTokenInvalid)
	}
	q := reqURL.Query()
	q.Set("token", jwt)
	reqURL.RawQuery = q.Encode()

	// http.NewRequestWithContext ties the outbound call to the caller's
	// ctx — if the client disconnects mid-flight, the Codifi call is
	// cancelled too, freeing the goroutine.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: request build: %v", ErrTokenInvalid, err)
	}
	req.Header.Set("userId", userID)
	req.Header.Set("appId", appID)

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
