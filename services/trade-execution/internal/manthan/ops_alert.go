package manthan

import "context"

// OpsAlerter is implemented by notifiers that can carry operator alerts.
// JWTExpiryNotifier publishes them on manthan.notifications as OPS_ALERT —
// the same stream the app and the admin console already consume for
// SESSION_EXPIRED — so a condition that needs a human (a stop that could not
// be re-placed, an exit lost at the broker, a chase that gave up) is no longer
// a log line only (2026-10-22 review).
type OpsAlerter interface {
	PublishOpsAlert(ctx context.Context, userID, symbol, kind, message string)
}

// notifyOps routes an operator alert through the session notifier when it
// supports alerts. nil-safe; never changes the caller's decision.
func notifyOps(n AuthExpiryNotifier, ctx context.Context, userID, symbol, kind, message string) {
	if n == nil {
		return
	}
	if o, ok := n.(OpsAlerter); ok {
		o.PublishOpsAlert(ctx, userID, symbol, kind, message)
	}
}
