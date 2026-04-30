package manthan

import "context"

// AuthExpiryNotifier is satisfied by JWTExpiryNotifier and lets every
// AU004-aware loop publish a runtime SESSION_EXPIRED event without
// importing the full notifier struct (and without having to wire a Kafka
// writer per component). Optional everywhere — nil-safe via the helper.
//
// IsSessionExpired is the gate consulted by tick-loops BEFORE they attempt
// any broker call. Returns true while the per-user dedup window is active
// (default 5 min after the last AU004). This stops every loop from
// re-hitting Indira while the user is mid re-login.
type AuthExpiryNotifier interface {
	PublishSessionExpired(ctx context.Context, userID, reason string)
	IsSessionExpired(userID string) bool
	ClearSessionExpired(userID string)
}

// notifyAuthExpired is a nil-safe wrapper so AU004 branches can call it
// unconditionally without inline nil checks.
func notifyAuthExpired(n AuthExpiryNotifier, ctx context.Context, userID, reason string) {
	if n == nil || userID == "" {
		return
	}
	n.PublishSessionExpired(ctx, userID, reason)
}

// authGated is the pre-check loops should call before doing broker work.
// Returns true when the user is gated (loop should skip this tick). Nil-safe.
func authGated(n AuthExpiryNotifier, userID string) bool {
	if n == nil || userID == "" {
		return false
	}
	return n.IsSessionExpired(userID)
}
