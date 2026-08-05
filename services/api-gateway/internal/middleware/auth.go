package middleware

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
)

// AuthRequired returns HTTP middleware that gates every wrapped route
// behind a valid JWT.
//
// Wire-up (in router.go):
//
//	protected := api.PathPrefix("").Subrouter()
//	protected.Use(middleware.AuthRequired(verifier))
//	protected.HandleFunc("/dashboard-stats", ...)
//	protected.HandleFunc("/live-orders",     ...)
//	// ...
//
// Every route registered on `protected` inherits the middleware.
// Routes registered on the parent `api` router stay public.
//
// The `v auth.Verifier` argument is INJECTED — this middleware
// contains no JWT signature math itself. Today the verifier is
// NoopVerifier (shape checks only); tomorrow it's LocalKeyVerifier
// (full HS512 HMAC verification with Codifi's secret). Same
// middleware code, either verifier. That is the entire point of
// depending on the interface, not the concrete type.
//
// The returned value has the signature `func(http.Handler)
// http.Handler`, which is exactly what gorilla/mux's Router.Use
// method accepts. We don't import gorilla/mux here — the raw
// function shape keeps this middleware framework-agnostic (would
// work identically with net/http, chi, gin, echo, etc.).
//
// capture (optional, may be nil): TokenCapture auto-refreshes the STORED
// broker credentials whenever a request carries a token newer than the one
// on file — see token_capture.go for why this is production-critical.
func AuthRequired(v auth.Verifier, capture *TokenCapture) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// ── Step 1 ── Read the Authorization header ─────────────
			// r.Header.Get is case-insensitive on the header NAME
			// (per the HTTP spec), so "Authorization", "authorization",
			// and "AUTHORIZATION" all work. Missing header → empty
			// string, which we treat as "not authenticated at all".
			//
			// SECURITY LOG on every rejection — critical for detecting
			// probing attacks (e.g., a burst of no-header requests may
			// indicate someone scanning for unprotected endpoints).
			headerValue := r.Header.Get("Authorization")
			if headerValue == "" {
				log.Printf("auth: reject %s %s → E_AUTH_MISSING: no Authorization header",
					r.Method, r.URL.Path)
				writeAuthError(w, "E_AUTH_MISSING",
					"Authorization header required")
				return
			}

			// ── Step 2 ── Strip the "Bearer " prefix ────────────────
			// The Bearer token scheme, per RFC 6750, expects the
			// header value to look like: "Bearer eyJhbGc..."
			//
			// We check the prefix strictly (case-sensitive on the
			// scheme name — the spec allows case-insensitive here
			// but the vast majority of real-world tokens use
			// exactly "Bearer" with a capital B, so being strict
			// catches typos in frontend code faster).
			const bearerPrefix = "Bearer "
			if !strings.HasPrefix(headerValue, bearerPrefix) {
				log.Printf("auth: reject %s %s → E_AUTH_BAD_HEADER: header missing Bearer prefix",
					r.Method, r.URL.Path)
				writeAuthError(w, "E_AUTH_BAD_HEADER",
					`Authorization header must start with "Bearer "`)
				return
			}
			// TrimSpace defends against a trailing newline or extra
			// whitespace that some HTTP clients accidentally add.
			token := strings.TrimSpace(headerValue[len(bearerPrefix):])
			if token == "" {
				log.Printf("auth: reject %s %s → E_AUTH_BAD_HEADER: Bearer token is empty",
					r.Method, r.URL.Path)
				writeAuthError(w, "E_AUTH_BAD_HEADER",
					"Bearer token is empty")
				return
			}

			// ── Step 3 ── Verify the token ──────────────────────────
			// Delegate to the injected Verifier. It returns either a
			// populated *Claims, or one of the sentinel errors from
			// the auth package. We use errors.Is to map each sentinel
			// to a distinct frontend-facing infoID (see mapVerifyError
			// below).
			//
			// Note we pass r.Context() (not context.Background()) so
			// that if the client disconnects, any network calls the
			// future LocalKeyVerifier makes (e.g., JWKS fetch on a
			// cache miss) get cancelled immediately.
			claims, err := v.Verify(r.Context(), token)
			if err != nil {
				infoID, msg := mapVerifyError(err)
				// SECURITY LOG: every auth rejection is logged with
				// the requested path + reason. This is critical for
				// detecting attacks (e.g., a burst of malformed
				// tokens = someone probing for weaknesses).
				//
				// We do NOT log the token itself — that would
				// leak credentials into the log stream. Log the
				// DECISION, not the SECRET.
				log.Printf("auth: reject %s %s → %s: %v",
					r.Method, r.URL.Path, infoID, err)
				writeAuthError(w, infoID, msg)
				return
			}

			// ── Step 4 ── Attach claims + raw JWT to ctx, call next ─
			// Claims are what handlers should read (via
			// auth.UserIDFromContext). The raw JWT is also stored so
			// the /logout handler can call Verifier.Revoke(jwt).
			// Regular handlers should NOT read the raw JWT — passing
			// tokens around widens the blast radius if any handler
			// mislogs them.
			ctx := auth.WithClaims(r.Context(), claims)
			ctx = auth.WithRawJWT(ctx, token)

			// ── Step 5 ── Auto-capture (non-blocking, nil-safe) ─────
			// The token just passed Codifi verification. If it's newer
			// than the stored broker session, push it so server-side
			// order/SL placement stays authenticated all day.
			capture.MaybeCapture(claims, token)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// mapVerifyError converts a sentinel error from the auth package into
// a (frontend-facing infoID, human-readable message) pair.
//
// The infoID is a stable machine-readable code the frontend can key
// off. The message is a fallback string a UI can show verbatim if it
// doesn't have a translation for the code yet.
//
// The switch order matters slightly: we check more specific errors
// before the generic default. errors.Is walks the wrap chain, so it
// will match even if a Verifier wrapped the sentinel with %w.
func mapVerifyError(err error) (infoID, message string) {
	switch {
	case errors.Is(err, auth.ErrTokenExpired),
		errors.Is(err, auth.ErrTokenSessionExpired):
		// Both "exp timestamp passed" and "server-side session
		// revoked" surface as the same frontend action: log in again.
		// Distinct sentinels for log clarity; same UX response.
		return "E_AUTH_EXPIRED", "token has expired — please log in again"
	case errors.Is(err, auth.ErrTokenInvalid):
		return "E_AUTH_INVALID_SIGNATURE", "token signature is invalid"
	case errors.Is(err, auth.ErrTokenMalformed):
		return "E_AUTH_BAD_FORMAT", "token is malformed"
	case errors.Is(err, auth.ErrNoUserID):
		return "E_AUTH_NO_USER", "token does not identify a user"
	default:
		// Any error we don't specifically recognise — better to be
		// terse than to leak internal error details to the frontend.
		return "E_AUTH", "authentication failed"
	}
}

// writeAuthError writes a 401 response with an Indira-style envelope
// (infoID, infoMsg, timestamp). Data field is intentionally omitted —
// the response is an error, there's no payload to send.
//
// This duplicates a slice of handlers/envelope.go on purpose:
//
//   - Middleware cannot import from handlers (would risk a future
//     circular dependency).
//   - Auth error responses are ~5 lines. Not worth extracting a
//     shared package until we have a 3rd caller (rule of three).
//   - When that 3rd caller appears, we lift this + the handlers
//     equivalents into a new internal/httpx package. Until then,
//     living with two identical copies is cheaper than the abstraction.
func writeAuthError(w http.ResponseWriter, infoID, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	body := struct {
		InfoID    string `json:"infoID"`
		InfoMsg   string `json:"infoMsg"`
		Timestamp int64  `json:"timestamp"`
	}{
		InfoID:    infoID,
		InfoMsg:   message,
		Timestamp: time.Now().UnixMilli(),
	}
	// If encoding fails (should never happen — inputs are all
	// simple strings), we've already sent 401 + Content-Type, so
	// there's nothing recoverable. Log and move on.
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("writeAuthError: JSON encode failed: %v", err)
	}
}
