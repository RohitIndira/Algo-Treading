package decisions

import "testing"

// These codes are emitted in the ORDER_REJECTED log line's "reason" field and
// are now also stored in signal_decisions.reason_code. They replaced inline
// string literals in handler.go; any drift would silently break existing
// log-based alerting, so pin the exact wire values.
func TestReasonCodeWireValues(t *testing.T) {
	for _, c := range []struct{ got, want string }{
		{ReasonBannedToken, "BANNED_TOKEN"},
		{ReasonQtyLimit, "QTY_LIMIT_EXCEEDED"},
		{ReasonOrderValueLimit, "ORDER_VALUE_LIMIT_EXCEEDED"},
		{ReasonExposureLimit, "EXPOSURE_LIMIT_EXCEEDED"},
		{ReasonDPRLower, "DPR_LOWER_BREACH"},
		{ReasonDPRUpper, "DPR_UPPER_BREACH"},
		{ReasonVelocity, "VELOCITY_BREACH"},
	} {
		if c.got != c.want {
			t.Errorf("reason code = %q, want %q", c.got, c.want)
		}
	}
}
