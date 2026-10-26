package manthan

import (
	"testing"
	"time"
)

func TestClassifyAMOAttemptErrors(t *testing.T) {
	hard, lock, ghost, auth := classifyAMOAttemptErrors([]string{
		"broker: Limit exceeded Set= 239, N.Used= 239 ,Prev= 239, T.Free Qty= 0,today free qty=0",
		"broker: Limit exceeded Set= 15, N.Used= 15 ,Prev= 15, T.Free Qty= 0",
		"broker: Limit exceeded Set= 0, N.Used= 121 ,Prev= 0, T.Free Qty= 0",
		"AMO rejected at conversion: Order entered has invalid data.",
		"place order request failed: indira: session expired (AU0xx): AU004 Session expired",
		"", // still pending
	})
	if hard != 1 || lock != 2 || ghost != 1 || auth != 1 {
		t.Fatalf("hard=%d lock=%d ghost=%d auth=%d want 1 2 1 1", hard, lock, ghost, auth)
	}
}

// An AMO queued at 16:00 must not match a converted order born at 08:50 the
// same morning (the previous night's conversion); it matches one born after.
func TestPlacedAfterIST(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	amoQueued := time.Date(2026, 10, 23, 16, 0, 56, 0, ist)
	if placedAfterIST("2026-10-23 08:50:12", amoQueued) {
		t.Fatal("this morning's conversion must not match tonight's AMO")
	}
	if !placedAfterIST("2026-10-26 08:50:04", amoQueued) {
		t.Fatal("the next session's conversion must match")
	}
	if !placedAfterIST("26-Oct-2026 08:50:04", amoQueued) {
		t.Fatal("alternate date layout")
	}
	if placedAfterIST("2026-10-23", amoQueued) || !placedAfterIST("2026-10-26", amoQueued) {
		t.Fatal("date-only strings order by day")
	}
	if placedAfterIST("garbage", amoQueued) {
		t.Fatal("unparseable must never match")
	}
	if !placedAfterIST("2026-10-23 08:50:12", time.Time{}) {
		t.Fatal("zero created_at must not block")
	}
}
