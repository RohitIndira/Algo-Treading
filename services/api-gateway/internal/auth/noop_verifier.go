package auth

import (
	"context"
)

// NoopVerifier is the temporary placeholder implementation of the Verifier
// interface, deliberately written so that:
//
//   - The HTTP middleware, the router grouping (public vs protected), and
//     the frontend's "Authorization: Bearer ..." integration are all wired
//     up end-to-end TODAY, while we wait on Codifi to send us the real
//     HS512 signing secret.
//
//   - Every check that DOES NOT need the secret is fully enforced. So
//     handlers receive a real userID from a structurally-valid JWT, and
//     malformed tokens / missing tokens / tokens with no userId claim all
//     get rejected with a precise reason.
//
//   - The ONE check that needs the secret — cryptographic signature
//     verification — is skipped. That is the "noop" part of the name.
//     Anyone who can construct three base64-url-encoded chunks separated
//     by dots, with a valid JSON payload containing userId, gets through.
//
// What NoopVerifier enforces (real security from day 1):
//
//	✓ Token is structurally a JWT: header.payload.signature, 3 parts
//	✓ Payload is base64url decodable
//	✓ Payload is valid JSON
//	✓ Payload has a "userId" claim (or "sub" as a fallback)
//
// What NoopVerifier SKIPS (until LocalKeyVerifier replaces it):
//
//	✗ Signature math (HS512-HMAC of header.payload using Codifi's secret)
//	✗ Expiry check (the "exp" claim is READ and stored, NOT enforced)
//	✗ Issuer/audience check (the inspected Codifi JWT omits iss/aud anyway)
//
// The day Codifi shares the real signing secret, we create LocalKeyVerifier
// in a new file, swap one line in main.go (NewNoopVerifier() →
// NewLocalKeyVerifier(secret)), and every signature check turns ON for
// every protected route at once. No handler, no middleware, no frontend
// code changes. That's the entire point of the Verifier interface.
//
// Why an empty struct instead of a function?
//
//   - Mirrors LocalKeyVerifier's shape, which WILL hold a secret []byte
//     field. Keeping the struct form means swapping types in main.go
//     produces a single-line diff.
//
//   - Lets us add config later (e.g., "should the noop verifier reject
//     expired tokens in staging but not in dev?") without changing the
//     call sites.
type NoopVerifier struct{}

// NewNoopVerifier returns the placeholder verifier. Callers store the
// result behind the Verifier interface, never as a *NoopVerifier — that
// keeps the swap to LocalKeyVerifier ergonomic later.
func NewNoopVerifier() *NoopVerifier {
	return &NoopVerifier{}
}

// Verify decomposes the JWT into its three segments, decodes the payload,
// parses the JSON claims, and extracts userId / exp into a *Claims. It
// performs NO cryptographic signature check.
//
// Returns:
//   - (*Claims, nil)              — JWT shape OK, userId present.
//   - (nil, ErrTokenMalformed)    — wrong number of segments, bad base64,
//                                   or bad JSON. The wrapped %w message
//                                   contains the precise diagnostic.
//   - (nil, ErrNoUserID)          — JSON parsed, but no userId or sub
//                                   field. We refuse to guess.
//
// The ctx is currently unused — the noop check is pure CPU. It's kept on
// the signature because the Verifier INTERFACE promises one, and the
// future LocalKeyVerifier may use it (e.g., for a JWKS HTTP fetch on
// first-use, when Codifi eventually moves to RS256).
func (n *NoopVerifier) Verify(ctx context.Context, jwt string) (*Claims, error) {
	_ = ctx // intentionally unused today; see method comment

	// Steps 1-3 — Delegate to the shared ParsePayload helper. It
	// handles the split/base64/JSON dance and returns the raw claims
	// map (or a wrapped ErrTokenMalformed). See parser.go for details.
	// IntrospectionVerifier and (future) LocalKeyVerifier use the
	// exact same helper, so shape-check behavior stays identical
	// across all Verifier implementations.
	raw, err := ParsePayload(jwt)
	if err != nil {
		return nil, err
	}

	// Step 4 — Extract userId.
	//
	// From the empirical inspection of a real Codifi JWT (the diagnostic
	// we ran on 2026-06-29), both "userId" and "sub" carry the same
	// value (e.g., "S4450"). We prefer "userId" because that's the field
	// Codifi explicitly names; "sub" is the JWT standard subject claim
	// and serves as a fallback in case Codifi drops "userId" in some
	// future token revision.
	//
	// The two-value form of a type assertion is `(value, ok)`. We
	// discard `ok` with the blank identifier `_` because a missing or
	// non-string value gives the zero value ("" empty string), which we
	// detect with the `userID == ""` check below — semantically
	// equivalent and slightly shorter than checking ok twice.
	userID, _ := raw["userId"].(string)
	if userID == "" {
		userID, _ = raw["sub"].(string)
	}
	if userID == "" {
		return nil, ErrNoUserID
	}

	// Step 5 — Read exp (optional, NOT enforced today).
	//
	// JSON-unmarshalled numbers always arrive as float64, even when
	// they look like integers in the source ("exp":1782820762 is
	// float64(1782820762.0) in Go). We assert as float64 and then cast
	// to int64. Missing exp → ok=false → expiresAt stays 0, which
	// downstream code (handlers, logging) treats as "unknown".
	//
	// LocalKeyVerifier will additionally compare expiresAt against
	// time.Now() and return ErrTokenExpired if past. NoopVerifier
	// intentionally accepts stale tokens so developers can hand-craft
	// fixed JWTs for tests without needing to refresh them.
	var expiresAt int64
	if expF, ok := raw["exp"].(float64); ok {
		expiresAt = int64(expF)
	}

	return &Claims{
		UserID:    userID,
		ExpiresAt: expiresAt,
		Raw:       raw,
	}, nil
}

// Revoke is a NO-OP on NoopVerifier because there is no cache and no
// blacklist to update — NoopVerifier accepts anything that parses,
// signature or not. Once real verification (IntrospectionVerifier or
// LocalKeyVerifier) is wired in, Revoke starts doing real work; the
// interface is stable in the meantime so /logout handler code doesn't
// need conditional branches based on which verifier is plugged in.
func (n *NoopVerifier) Revoke(jwt string) {
	_ = jwt
}

// Compile-time assertion: if *NoopVerifier ever stops satisfying the
// Verifier interface (e.g., someone renames Verify or changes its
// signature), THIS LINE will fail to compile. Catches the mismatch at
// build time, not as a confusing runtime panic when the middleware tries
// to use it.
//
// Read it as: "I claim that nil-of-type-*NoopVerifier is a valid
// Verifier. Compiler, prove me wrong if it isn't."
var _ Verifier = (*NoopVerifier)(nil)
