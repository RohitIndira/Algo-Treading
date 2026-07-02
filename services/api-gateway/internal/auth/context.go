package auth

import "context"

// contextKey is an unexported type used ONLY as a key for storing
// Claims on a request Context. Using an unexported custom type is
// the STANDARD Go pattern for context keys — it prevents cross-
// package key collisions.
//
// Why NOT just use a string like "userID" as the key?
//
//	If any other package in the codebase (or any third-party library)
//	stores something under ctx.WithValue("userID", ...), whichever
//	call runs LAST silently overwrites the earlier one. The handler
//	then reads the wrong data with no error, no warning — just
//	broken behavior in production. Impossible to catch in unit tests.
//
// Why does an unexported int type fix this?
//
//	1. The type `contextKey` is unexported (lowercase). No package
//	   outside `auth` can create a value of this type.
//
//	2. Even if another package defines its OWN `type contextKey int`
//	   and uses value 0, the two types are DISTINCT in Go's type
//	   system. context.Value compares by (type identity, value)
//	   tuple — same value + different type = miss.
//
//	3. Zero runtime cost. contextKey is just an int under the hood.
//
// This pattern is used by the Go standard library itself (see
// net/http.ServerContextKey), by every major Go web framework, and
// by every serious production Go codebase. Adopt it here and you
// never have to think about context-key collisions again.
type contextKey int

const (
	// claimsContextKey is the single key we use to store verified
	// *Claims on a request context. If we ever add more auth-related
	// context values (request-id, trace-id, etc.), each gets its own
	// constant right below this one — iota auto-numbers them.
	claimsContextKey contextKey = iota

	// rawJWTContextKey stores the raw JWT string. Needed by the
	// /logout handler, which passes the token to Verifier.Revoke(jwt)
	// to add it to the blacklist. Regular handlers should NEVER read
	// this — they only need UserIDFromContext.
	rawJWTContextKey
)

// WithClaims returns a NEW context derived from ctx, carrying the
// given claims. The AuthMiddleware calls this immediately after a
// successful Verify(), so downstream handlers can retrieve the
// authenticated user's identity.
//
// Idiomatic naming: functions that ATTACH a value to a context are
// named With<Thing>. See also http.NewRequestWithContext,
// log.WithContext, slog.With, tracing libraries, etc. — the Go
// ecosystem is consistent about this.
//
// Note that contexts are IMMUTABLE — WithValue returns a new one
// wrapping the old. This is why the caller must reassign:
//
//	ctx = auth.WithClaims(ctx, claims)   ← correct
//	auth.WithClaims(ctx, claims)          ← useless, discards new ctx
func WithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

// ClaimsFromContext extracts the *Claims previously attached by
// WithClaims. Returns (nil, false) if the context carries no
// Claims — which happens on unauthenticated (public) routes, or
// if middleware wasn't applied where the developer expected it.
//
// The (value, ok) return shape mirrors Go's map access syntax:
//
//	claims, ok := auth.ClaimsFromContext(ctx)
//	if !ok {
//	    // no claims on this ctx — public route, or middleware missing
//	}
//
// Handlers rarely call this directly; most reach for the smaller
// UserIDFromContext helper below.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	// ctx.Value returns interface{}. We type-assert to *Claims.
	// If the value is missing OR of some other type, the two-value
	// form of the assertion gives us (zero-value, false) — no panic.
	claims, ok := ctx.Value(claimsContextKey).(*Claims)
	return claims, ok
}

// UserIDFromContext is the convenience wrapper for the single
// question 99% of handlers ask: "which user is calling me?".
//
// Returns:
//   - (userID, true) — an authenticated caller was found on ctx
//   - ("", false)    — no claims present, OR claims present but
//                      UserID somehow empty (shouldn't happen if
//                      NoopVerifier ran, but defensive coding is
//                      free in a single-line helper)
//
// Usage in a handler:
//
//	userID, ok := auth.UserIDFromContext(r.Context())
//	if !ok {
//	    respondIndiraError(w, 401, "E_AUTH", "not authenticated")
//	    return
//	}
//	// ... use userID
//
// The extra !ok check inside handlers is DEFENSE IN DEPTH — if
// someone accidentally mounts a route as public but the handler
// expects a user, this catches it instead of using an empty
// userID (which could silently return another user's data on a
// LIKE query, or hit "" as the primary key on a lookup, etc.).
func UserIDFromContext(ctx context.Context) (string, bool) {
	claims, ok := ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.UserID == "" {
		return "", false
	}
	return claims.UserID, true
}

// WithRawJWT attaches the raw JWT string to the context. Called by
// AuthMiddleware immediately after a successful Verify, so downstream
// handlers (specifically /logout) can retrieve it and call
// Verifier.Revoke(jwt).
//
// Regular handlers should NOT reach for this — they should use
// UserIDFromContext instead. Passing the raw token around widens the
// blast radius if a handler ever mishandles logging.
func WithRawJWT(ctx context.Context, jwt string) context.Context {
	return context.WithValue(ctx, rawJWTContextKey, jwt)
}

// RawJWTFromContext returns the raw JWT string previously attached
// by WithRawJWT, or ("", false) if none is present.
//
// Restrict use to auth-related endpoints (/logout, potentially
// /refresh in future). Reading it elsewhere is a code smell.
func RawJWTFromContext(ctx context.Context) (string, bool) {
	jwt, ok := ctx.Value(rawJWTContextKey).(string)
	if !ok || jwt == "" {
		return "", false
	}
	return jwt, true
}
