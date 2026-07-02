// Package auth defines the abstractions that the api-gateway uses to decide
// "is this request coming from a real, logged-in user, and if so, who are
// they?" The package is intentionally split into:
//
//	verifier.go      ← this file — the contracts (interface + types + errors)
//	noop_verifier.go ← the placeholder used today (no signature math)
//	context.go       ← helpers for putting/pulling Claims on a request context
//
// The middleware that wires these into the HTTP pipeline lives in
// internal/middleware/auth.go — it depends ONLY on this package's
// interface, not on any concrete implementation. That separation is what
// will let us swap the placeholder for a real RSA-signature-verifying
// implementation later without touching middleware, routes, or handlers.
package auth

import (
	"context"
	"errors"
)

// Claims is the verified, trusted snapshot of user information that the
// Verifier extracts from a JWT. It is the ONLY thing the rest of the
// codebase should trust about "who is calling this API right now".
//
// Mental model: when an immigration officer reads your passport, they
// don't hand the passport itself to every gate agent. They produce a
// short slip — "Name: X. Citizenship: Y. Expires: Z." — and the gate
// agents act on that slip. Claims is that slip.
//
// Why a struct instead of a plain map[string]interface{}?
//
//   - COMPILE-TIME SAFETY. Handlers reach for claims.UserID; a typo like
//     claims.UserIid is a compile error, not a runtime "empty string" bug
//     that silently authorizes the wrong account.
//
//   - DOCUMENTATION. Reading this file tells you exactly what the rest of
//     the codebase is allowed to assume about an authenticated request.
//     If a field isn't here, handlers can't accidentally depend on it.
//
//   - FORWARD-COMPATIBLE. The day Codifi's JWT starts carrying additional
//     fields (email, role, account_id), we add them here as typed fields
//     once, and every handler gets them with no extra parsing.
//
// The Raw field is the escape hatch — if some one-off handler needs a
// custom claim we haven't promoted to a typed field yet, it can dip into
// Raw["custom_claim"]. Most handlers will never touch Raw.
type Claims struct {
	// UserID is the identifier Codifi puts in the JWT to say "this token
	// belongs to user X". Every protected handler uses this to scope its
	// queries (WHERE user_id = $1). It is the single most important field
	// in this struct.
	UserID string

	// ExpiresAt is the JWT's "exp" claim, in Unix seconds. Read but NOT
	// enforced today (NoopVerifier intentionally skips expiry checks so
	// dev/staging tokens don't constantly bounce). LocalKeyVerifier will
	// enforce it as part of full verification.
	//
	// Kept on Claims today for logging, debugging, and so future code
	// (like a cache layer) can use it without re-parsing the JWT.
	ExpiresAt int64

	// Raw holds the entire decoded JWT payload — every claim Codifi
	// included, named or otherwise. Reserved for the rare case where a
	// handler genuinely needs a one-off claim. Promote frequently-used
	// fields to typed fields above; don't let the codebase grow lots of
	// raw["someThing"] lookups.
	Raw map[string]interface{}
}

// Verifier is the single abstraction every auth check in the gateway
// flows through. It takes a raw JWT string and returns either a trusted
// *Claims or an error explaining why the token was rejected.
//
// Today the only implementation is NoopVerifier (parses JWT shape,
// extracts the userId field, skips the cryptographic signature math).
//
// Tomorrow — the moment Codifi gives us their public key — we'll add
// LocalKeyVerifier (full RSA-SHA256 signature verification) and swap it
// in with a one-line change in main.go. Every handler, every route,
// every middleware test, every frontend integration stays identical.
// That is the entire point of having an interface here.
//
// You have already seen this exact pattern: algos.Catalog ↔
// algos.StaticCatalog. Same shape, different domain.
type Verifier interface {
	// Verify validates the given JWT string and, on success, returns the
	// Claims contained in it.
	//
	// The jwt argument is the RAW token — middleware strips the "Bearer "
	// prefix from the Authorization header before calling Verify.
	//
	// ctx is reserved for future implementations that may need network
	// calls (e.g., fetching a rotating JWKS, hitting a cached
	// introspection endpoint). NoopVerifier ignores it, which is fine:
	// the interface promises a ctx, so all callers already pass one, and
	// future implementations can rely on it without changing any call
	// site.
	//
	// On failure, Verify returns one of the sentinel errors below (or
	// something wrapping it) so middleware can map the cause to a
	// specific HTTP response code via errors.Is.
	Verify(ctx context.Context, jwt string) (*Claims, error)

	// Revoke marks the given JWT as no-longer-valid on OUR side, called
	// by the /logout handler. A conforming implementation should:
	//   - Remove the JWT from any cache it maintains (so any concurrent
	//     Verify does not return the stale success)
	//   - Add the JWT to a short-lived blacklist that Verify checks
	//     BEFORE any cache, blocking use even if Codifi still trusts it
	//   - Keep the blacklist entry until the JWT's own `exp` (or a safe
	//     upper bound), then evict — after exp, the JWT is useless anyway
	//
	// For a Verifier with no cache or blacklist (e.g., NoopVerifier),
	// Revoke is a legal no-op. Callers should always call Revoke on
	// /logout regardless — the interface guarantees the call is safe.
	Revoke(jwt string)
}

// The sentinel errors below are the SHARED VOCABULARY every Verifier
// implementation uses to explain why it rejected a token. Middleware
// inspects them with errors.Is and maps each to a frontend-facing
// infoID + HTTP status:
//
//	ErrTokenMalformed → 401 "E_AUTH_BAD_FORMAT"
//	ErrTokenExpired   → 401 "E_AUTH_EXPIRED"
//	ErrTokenInvalid   → 401 "E_AUTH_INVALID_SIGNATURE"
//	ErrNoUserID       → 401 "E_AUTH_NO_USER"
//
// Defining them here (rather than per-implementation) means middleware
// doesn't need to know WHICH Verifier is plugged in — it just maps
// errors to responses. When LocalKeyVerifier lands, it returns the same
// vocabulary, and middleware keeps working unchanged.
var (
	// ErrTokenMalformed is returned when the input does not even look
	// like a JWT — i.e., it isn't three base64url-encoded segments
	// separated by dots. Garbage strings, partial tokens, completely
	// wrong headers all surface here.
	ErrTokenMalformed = errors.New("auth: token is not a valid JWT")

	// ErrTokenExpired is returned when the JWT's "exp" claim is in the
	// past. NoopVerifier intentionally does NOT return this today (so
	// stale dev tokens still work); it's defined here so the contract is
	// stable for the day LocalKeyVerifier starts enforcing expiry.
	ErrTokenExpired = errors.New("auth: token has expired")

	// ErrTokenInvalid is returned when the JWT's signature fails to
	// verify against the expected public key. This is THE big rejection
	// reason once LocalKeyVerifier is live; NoopVerifier never returns
	// it today (because it doesn't check signatures at all).
	ErrTokenInvalid = errors.New("auth: token signature is invalid")

	// ErrNoUserID is returned when the JWT payload parses successfully
	// but contains no "userId" claim — meaning we can't tell WHICH user
	// is calling. Rejecting here is safer than silently defaulting to
	// some sentinel user ID, which would be a critical security bug.
	ErrNoUserID = errors.New("auth: token has no userId claim")
)
