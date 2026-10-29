package repository

// Client login history (migration trade-execution/018, execution_db).
//
// Every credential store is a login seen by the platform — the SSO flow
// posts the fresh token, and the gateway auto-captures a newer JWT it sees
// on any request. Both land in StoreIndiraCredentials, so one insert here
// gives the admin console a complete history without touching either
// caller. The JWT's iat/exp claims are read WITHOUT verification: this is
// bookkeeping about a token the caller already authenticated with, never
// an authorization decision.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

// loginClaims is what the history keeps from the token itself.
type loginClaims struct {
	IssuedAt    time.Time // zero when the token carries no numeric iat
	ExpiresAt   time.Time // zero when no numeric exp
	LoginSource string    // loginSource claim (SSO | APP | ...), '' when absent
}

// parseLoginClaims decodes the JWT payload segment. Any malformed input
// yields empty claims and no error: a token the broker accepted but we
// cannot parse is still a login worth recording.
func parseLoginClaims(token string) loginClaims {
	var out loginClaims
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return out
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		// Some issuers pad; tolerate StdEncoding/URLEncoding with padding.
		if payload, err = base64.URLEncoding.DecodeString(parts[1]); err != nil {
			return out
		}
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return out
	}
	if v, ok := raw["iat"].(float64); ok && v > 0 {
		out.IssuedAt = time.Unix(int64(v), 0).UTC()
	}
	if v, ok := raw["exp"].(float64); ok && v > 0 {
		out.ExpiresAt = time.Unix(int64(v), 0).UTC()
	}
	if s, ok := raw["loginSource"].(string); ok {
		out.LoginSource = strings.ToUpper(strings.TrimSpace(s))
	}
	return out
}

// tokenFingerprint is a stable, non-reversible id for one token: the same
// token pushed twice (the web app re-posts on every page load) is one login.
func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])[:24]
}

// recordLogin inserts the history row. Best-effort by design: the
// credential upsert that precedes it is on the trading hot path (a failed
// store means a client cannot trade), so a history problem is logged and
// never surfaced to the caller. A missing table (migration not yet
// applied) is one such problem — the warning says so.
func (r *credentialsRepository) recordLogin(ctx context.Context, userID, indiraUserID, appID, source, token string) {
	claims := parseLoginClaims(token)
	var iat, exp *time.Time
	if !claims.IssuedAt.IsZero() {
		iat = &claims.IssuedAt
	}
	if !claims.ExpiresAt.IsZero() {
		exp = &claims.ExpiresAt
	}
	const q = `
        INSERT INTO user_login_events
            (user_id, indira_user_id, app_id, source, login_source, kind,
             token_issued_at, token_expires_at, token_fingerprint)
        VALUES ($1, $2, $3, $4, $5, 'LOGIN', $6, $7, $8)
        ON CONFLICT (user_id, token_fingerprint) DO NOTHING`
	if _, err := r.db.ExecContext(ctx, q, userID, indiraUserID, appID, source,
		claims.LoginSource, iat, exp, tokenFingerprint(token)); err != nil {
		log.Printf("credentials: login history row NOT written for user=%s (credential itself is stored): %v — is migration trade-execution/018_user_login_events.sql applied to execution_db?", userID, err)
	}
}

// String form used in tests and logs.
func (c loginClaims) String() string {
	return fmt.Sprintf("iat=%s exp=%s loginSource=%q", c.IssuedAt.Format(time.RFC3339), c.ExpiresAt.Format(time.RFC3339), c.LoginSource)
}
