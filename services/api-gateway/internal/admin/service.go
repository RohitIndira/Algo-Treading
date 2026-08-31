package admin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Tier is the confirmation level an admin endpoint demands (spec M1.3).
type Tier string

const (
	TierRead    Tier = "READ"    // no confirmation
	TierConfirm Tier = "CONFIRM" // UI confirm dialog; server needs confirmed=true
	TierTyped   Tier = "TYPED"   // server-issued blast-radius string, retyped verbatim
	tierAuth    Tier = "AUTH"    // internal: elevation/logout events in the audit log
)

// Elevation rate limit: a user_id gets elevateMaxAttempts tries per
// elevateWindow, successes included. Misuse probes show up as audit rows
// long before they get a real shot at anything.
const (
	elevateMaxAttempts = 3
	elevateWindow      = 15 * time.Minute
)

// ErrRateLimited — too many elevation attempts.
var ErrRateLimited = errors.New("too many elevation attempts — try later")

// Service owns admin policy: elevation, session validation, tier
// enforcement, rate limiting. HTTP concerns stay in http.go; SQL in store.go.
type Service struct {
	store *Store

	mu       sync.Mutex
	attempts map[string][]time.Time // user_id → recent elevation attempts
	now      func() time.Time       // injectable clock for tests
}

func NewService(store *Store) *Service {
	return &Service{
		store:    store,
		attempts: make(map[string][]time.Time),
		now:      time.Now,
	}
}

// allowAttempt implements the sliding-window limiter. It records the attempt
// unconditionally (a denied attempt still consumes budget — probing costs).
func (s *Service) allowAttempt(userID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-elevateWindow)
	kept := s.attempts[userID][:0]
	for _, t := range s.attempts[userID] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	allowed := len(kept) < elevateMaxAttempts
	if allowed {
		kept = append(kept, s.now())
	}
	s.attempts[userID] = kept
	return allowed
}

// ElevateResult is what a successful elevation returns to the client.
type ElevateResult struct {
	Token     string    `json:"admin_token"` // shown once; never stored or logged
	ExpiresAt time.Time `json:"expires_at"`
	AdminID   string    `json:"admin_id"`
}

// Elevate turns an authenticated platform user into an admin session.
// The caller (http.go) guarantees userID came from the gateway's verified
// auth context — introspection-checked against the broker — never from a
// request body. Every path through here writes an audit row.
func (s *Service) Elevate(ctx context.Context, userID, ip string) (*ElevateResult, error) {
	deny := func(result, detail string) error {
		// Audit the denial. If even the audit fails, fail shut with the
		// audit error — an unrecorded denial is worse than a 500.
		if aerr := s.store.Audit(ctx, AuditEntry{
			AdminID: userID, Action: "ELEVATE_DENIED", Tier: string(tierAuth),
			Result: result, Detail: detail, IP: ip,
		}); aerr != nil {
			return fmt.Errorf("audit failure during denial (%s): %w", detail, aerr)
		}
		return nil
	}

	if !s.allowAttempt(userID) {
		if err := deny("DENIED", "rate limited"); err != nil {
			return nil, err
		}
		return nil, ErrRateLimited
	}

	isAdmin, err := s.store.IsActiveAdmin(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("elevate: %w", err)
	}
	if !isAdmin {
		if err := deny("DENIED", "not on admin allow-list"); err != nil {
			return nil, err
		}
		return nil, ErrNotAdmin
	}

	raw, hash, err := NewToken()
	if err != nil {
		return nil, err
	}
	sessionID, expires, err := s.store.CreateSession(ctx, userID, hash, ip)
	if err != nil {
		return nil, err
	}
	if err := s.store.Audit(ctx, AuditEntry{
		AdminID: userID, Action: "ELEVATE", Tier: string(tierAuth),
		Result: "OK", IP: ip, SessionID: sessionID,
	}); err != nil {
		// The session exists but is unrecorded — revoke it and fail. An
		// auditable trail is a hard invariant, not best-effort.
		_ = s.store.RevokeSession(ctx, sessionID)
		return nil, fmt.Errorf("elevation aborted — audit write failed: %w", err)
	}
	return &ElevateResult{Token: raw, ExpiresAt: expires, AdminID: userID}, nil
}

// Validate resolves a presented raw token to a live Session (or
// ErrSessionInvalid). Constant-shape work either way.
func (s *Service) Validate(ctx context.Context, rawToken string) (*Session, error) {
	if len(rawToken) != tokenBytes*2 { // hex length check — cheap junk filter
		return nil, ErrSessionInvalid
	}
	return s.store.LookupSession(ctx, HashToken(rawToken))
}

// Logout revokes the presented session and audits it.
func (s *Service) Logout(ctx context.Context, sess *Session, ip string) error {
	if err := s.store.RevokeSession(ctx, sess.ID); err != nil {
		return err
	}
	return s.store.Audit(ctx, AuditEntry{
		AdminID: sess.AdminID, Action: "LOGOUT", Tier: string(tierAuth),
		Result: "OK", IP: ip, SessionID: sess.ID,
	})
}

// ConfirmationText is the server-computed blast-radius string for a TYPED
// action. The server generates it, the UI displays it, the human retypes
// it, and enforceTier compares verbatim — a curl bypassing the UI changes
// nothing because the required string is never client-chosen.
func ConfirmationText(action, targetRef, targetUser string) string {
	return fmt.Sprintf("%s %s FOR %s", action, targetRef, targetUser)
}
