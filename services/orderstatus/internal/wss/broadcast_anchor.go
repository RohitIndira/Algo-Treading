package wss

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// UserSource is where the broadcast listener learns which clients we manage
// and whose session can anchor the feed. It is the same user-config gRPC
// client the per-user subscriptions use (userconfig.Client satisfies it), so
// the anchor is always a session we already hold for a managed user.
type UserSource interface {
	ListActiveUserIDs(ctx context.Context) ([]string, error)
	FetchUserCredentials(ctx context.Context, userID string) (*indira.AuthContext, error)
}

// anchorCandidate is one live session that could anchor the broadcast.
type anchorCandidate struct {
	auth *indira.AuthContext
	iat  time.Time // JWT issued-at; zero when the token is not a readable JWT
	exp  time.Time // JWT expiry; zero when unknown
}

// sessionTimes reads iat/exp from a JWT without verifying it. The broadcast
// only needs ordering ("newest login") and an expiry pre-check; the broker
// does the real verification when the token is used. Unreadable tokens return
// zero times and stay eligible, ranked last.
func sessionTimes(bearer string) (iat, exp time.Time) {
	parts := strings.Split(strings.TrimSpace(bearer), ".")
	if len(parts) != 3 {
		return time.Time{}, time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		if payload, err = base64.URLEncoding.DecodeString(parts[1]); err != nil {
			return time.Time{}, time.Time{}
		}
	}
	var claims struct {
		Iat int64 `json:"iat"`
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return time.Time{}, time.Time{}
	}
	if claims.Iat > 0 {
		iat = time.Unix(claims.Iat, 0)
	}
	if claims.Exp > 0 {
		exp = time.Unix(claims.Exp, 0)
	}
	return iat, exp
}

// isAuthFailure reports whether a token-endpoint error means "this session
// is no longer valid at the broker" (AU0xx body, HTTP 401/403) as opposed to
// a network blip. Only auth failures retire an anchor.
func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, indira.ErrAuthExpired) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "HTTP error 401") || strings.Contains(msg, "HTTP error 403")
}

// pickAnchor chooses the session that will fetch the broadcast token:
//
//  1. the configured dealer login (BROADCAST_ANCHOR_USER) when it has a live,
//     non-expired, not-recently-failed session;
//  2. otherwise the newest (largest JWT iat) live session among managed users.
//
// Sessions marked dead by a previous auth failure are skipped until the user
// logs in again (their JWT iat moves past the dead mark) or DeadAnchorTTL
// passes. Returns nil when nothing is usable.
func (b *BroadcastListener) pickAnchor(ctx context.Context) *indira.AuthContext {
	now := b.cfg.Clock.Now()

	if cfgUser := strings.TrimSpace(b.cfg.AnchorUser); cfgUser != "" {
		if c := b.candidate(ctx, cfgUser, now); c != nil {
			return c.auth
		}
		b.logger.Warn("configured broadcast anchor unusable — falling back to newest client session",
			zap.String("anchor_user", cfgUser))
	}

	ids, err := b.users.ListActiveUserIDs(ctx)
	if err != nil {
		b.logger.Warn("broadcast anchor: ListActiveUserIDs failed", zap.Error(err))
		return nil
	}
	var cands []anchorCandidate
	for _, id := range ids {
		if c := b.candidate(ctx, id, now); c != nil {
			cands = append(cands, *c)
		}
	}
	if len(cands) == 0 {
		return nil
	}
	// Newest login first; unknown iat (zero) sorts last. Stable on user id so
	// the choice is deterministic when iat ties.
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].iat.Equal(cands[j].iat) {
			return cands[i].auth.UserId < cands[j].auth.UserId
		}
		return cands[i].iat.After(cands[j].iat)
	})
	return cands[0].auth
}

// candidate fetches userID's credentials and applies the eligibility rules.
func (b *BroadcastListener) candidate(ctx context.Context, userID string, now time.Time) *anchorCandidate {
	auth, err := b.users.FetchUserCredentials(ctx, userID)
	if err != nil {
		b.logger.Debug("broadcast anchor: credentials fetch failed", zap.String("user_id", userID), zap.Error(err))
		return nil
	}
	if auth == nil || strings.TrimSpace(auth.BearerToken) == "" {
		return nil
	}
	if auth.UserId == "" {
		auth.UserId = userID
	}
	iat, exp := sessionTimes(auth.BearerToken)
	if !exp.IsZero() && !exp.After(now) {
		return nil // expired — never pick
	}
	b.anchorMu.Lock()
	deadAt, dead := b.deadAnchors[userID]
	if dead {
		relogin := !iat.IsZero() && iat.After(deadAt)
		expired := now.Sub(deadAt) > b.cfg.DeadAnchorTTL
		if relogin || expired {
			delete(b.deadAnchors, userID)
			delete(b.alertedDead, userID)
			dead = false
		}
	}
	b.anchorMu.Unlock()
	if dead {
		return nil
	}
	return &anchorCandidate{auth: auth, iat: iat, exp: exp}
}

// markAnchorDead retires a session after the broker rejected it, and alerts
// once per session (not once per retry).
func (b *BroadcastListener) markAnchorDead(ctx context.Context, userID string, cause error) {
	b.anchorMu.Lock()
	b.deadAnchors[userID] = b.cfg.Clock.Now()
	alerted := b.alertedDead[userID]
	b.alertedDead[userID] = true
	b.anchorMu.Unlock()
	b.stats.AnchorSwitches.Add(1)
	mBroadcastAnchorSwitches.Inc()
	b.logger.Error("broadcast anchor session rejected by broker — switching anchor",
		zap.String("user_id", userID), zap.Error(cause))
	if !alerted {
		b.alerter.Alert(ctx, "BROADCAST_ANCHOR_LOST", userID,
			"The session anchoring the dealer broadcast feed was rejected by the broker ("+cause.Error()+"). Trying the next live session.")
	}
}
