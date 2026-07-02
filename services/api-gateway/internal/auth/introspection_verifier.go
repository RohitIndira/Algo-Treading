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
	// VerifyURL is the base URL of Codifi's verify endpoint. Query
	// string is added at Verify time. Example:
	//   https://livemiddleware.indiratrade.com/auth-services/api/auth/verify/token
	VerifyURL string

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
// receive sane defaults. VerifyURL is REQUIRED and panics if empty
// (a missing verify URL is a startup misconfiguration, not a runtime
// condition worth returning an error for).
func NewIntrospectionVerifier(cfg IntrospectionConfig) *IntrospectionVerifier {
	if cfg.VerifyURL == "" {
		panic("auth.NewIntrospectionVerifier: cfg.VerifyURL is required")
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
	// calling Codifi.
	raw, err := ParsePayload(jwt)
	if err != nil {
		return nil, err // already wrapped with ErrTokenMalformed
	}
	userID, _ := raw["userId"].(string)
	appID, _ := raw["appId"].(string)
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

	// ── Step 4 ── Call Codifi verify endpoint ────────────────────
	reqURL, err := url.Parse(iv.cfg.VerifyURL)
	if err != nil {
		// Config-time bug, not a runtime auth problem.
		log.Printf("auth: bad VerifyURL config: %v", err)
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
		log.Printf("auth: Codifi verify network error: %v", err)
		return nil, fmt.Errorf("%w: verify network error", ErrTokenInvalid)
	}
	defer resp.Body.Close() // MANDATORY — leaks the TCP connection otherwise.

	// ── Step 5 ── Parse Codifi's response ────────────────────────
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: verify body read: %v", ErrTokenInvalid, err)
	}

	var vresp struct {
		Code         string `json:"code"`
		Message      string `json:"message"`
		PlainMessage struct {
			UCC         string `json:"ucc"`
			Timestamp   int64  `json:"timestamp"`
			CallbackURL string `json:"callback-url"`
		} `json:"plain-message"`
	}
	if err := json.Unmarshal(body, &vresp); err != nil {
		return nil, fmt.Errorf("%w: verify response parse: %v", ErrTokenInvalid, err)
	}

	// Codifi's convention: HTTP is always 200, real status is in body.code.
	// "200" (STRING, not int) = valid; anything else = rejected.
	if vresp.Code != "200" {
		// Negative-cache the rejection so brute-force garbage tokens
		// don't hammer Codifi on every attempt.
		iv.cache.Store(jwt, cacheEntry{
			valid:     false,
			expiresAt: time.Now().Add(iv.cfg.NegativeTTL),
		})
		return nil, fmt.Errorf("%w: Codifi rejected: %s (code=%s)",
			ErrTokenInvalid, vresp.Message, vresp.Code)
	}

	// ── Success — build Claims ───────────────────────────────────
	// SECURITY: use the userId we extracted from the JWT payload,
	// NOT vresp.PlainMessage.UCC. The UCC field is Codifi's echo of
	// the header we sent, not a verified value — trusting it would
	// let a wrong header override the token's actual identity.
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
