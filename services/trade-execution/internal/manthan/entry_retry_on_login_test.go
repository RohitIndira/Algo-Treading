package manthan

// Regressions for the 2026-08-27 SHREEPUSHK incident: S4450's broker session
// was expired when the day's signal arrived, the entry was AU004-rejected,
// and the inbox dedup then treated the REJECTED order row as "already placed"
// — the row went DONE and the day's signal died even though the user could
// have logged in an hour later. Requirement (operator, 2026-08-27):
//   - a signal blocked by dead credentials retries until credentials return,
//   - but only within its own trading day: yesterday's signal never places
//     today, today's never places tomorrow.

import (
	"context"
	"fmt"
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// TestAU004ChainClassifiesAsAuthHold proves the production wrapping chain
// preserves the ErrAuthExpired sentinel end-to-end: pkg/indira doRequest
// ("%w: AU004 ...") -> orders.go ("place order request failed: %w") ->
// entry_handler executeLive ("place limit buy: %w") -> inbox classifier.
// A single %v anywhere in that chain would silently demote a dead session
// to TRANSIENT (attempts burned, DLQ in ~25 min).
func TestAU004ChainClassifiesAsAuthHold(t *testing.T) {
	chain := fmt.Errorf("place limit buy: %w",
		fmt.Errorf("place order request failed: %w",
			fmt.Errorf("%w: %s %s", indiraClient.ErrAuthExpired, "AU004", "Session expired")))
	if got := classifyHandlerErr(chain); got != InboxErrAuthExpired {
		t.Fatalf("classified %q, want %q", got, InboxErrAuthExpired)
	}
}

func TestEntryAttemptTerminalFailureClassification(t *testing.T) {
	// Failed attempts → retry allowed.
	for _, s := range []OrderStatus{StatusRejected, StatusCancelled, StatusExpired} {
		if !entryAttemptIsTerminalFailure(s) {
			t.Errorf("%s must count as a failed attempt (retryable)", s)
		}
	}
	// Live or completed → dedup must hold (re-placing would double-buy).
	for _, s := range []OrderStatus{StatusPending, StatusPlaced, StatusPartial, StatusFilled, StatusSLPlaced} {
		if entryAttemptIsTerminalFailure(s) {
			t.Errorf("%s must dedup, not retry — retrying a live/filled entry double-buys", s)
		}
	}
}

func TestAuthExpiredIsAHoldClass(t *testing.T) {
	// 50 attempts × 30 s burned the budget in 25 min — a dead session lasts
	// hours. AUTH_EXPIRED must hold without burning attempts, like the
	// upper-circuit and pre-open holds.
	for _, c := range []string{InboxErrAuthExpired, InboxErrUpperCircuit, InboxErrPreOpen} {
		if !isHoldClass(c) {
			t.Errorf("%s must be a hold class", c)
		}
	}
	for _, c := range []string{InboxErrPoison, InboxErrTransient, InboxErrBrokerReject} {
		if isHoldClass(c) {
			t.Errorf("%s must NOT be a hold class", c)
		}
	}
}

func TestExpireStaleEntrySignals_SameDayOnly(t *testing.T) {
	db := openExecTestDB(t)
	defer db.Close()
	repo := &Repository{db: db}
	ctx := context.Background()

	defer func() {
		_, _ = db.Exec(`DELETE FROM signal_inbox WHERE signal_id LIKE 'exp-test-%'`)
	}()
	_, _ = db.Exec(`DELETE FROM signal_inbox WHERE signal_id LIKE 'exp-test-%'`)

	seed := func(id, otype, status string, createdAt time.Time) {
		t.Helper()
		if _, err := db.Exec(`
			INSERT INTO signal_inbox (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset, status, created_at)
			VALUES ($1,'S4450',$2,'{}','t',0,0,$3,$4)`, id, otype, status, createdAt); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	ist := istLocation()
	now := time.Now().In(ist)
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, ist)
	yesterday := todayStart.Add(-10 * time.Hour) // yesterday 14:00 IST
	today := todayStart.Add(11 * time.Hour)      // today 11:00 IST

	seed("exp-test-y-pending", "MANTHAN_ENTRY", "PENDING", yesterday) // → expire
	seed("exp-test-y-failed", "MANTHAN_ENTRY", "FAILED", yesterday)   // → expire
	seed("exp-test-y-done", "MANTHAN_ENTRY", "DONE", yesterday)       // terminal — untouched
	seed("exp-test-t-pending", "MANTHAN_ENTRY", "PENDING", today)     // today — keep
	seed("exp-test-y-slmod", "MANTHAN_SL_MODIFY", "PENDING", yesterday) // SL — never expired

	n, err := repo.ExpireStaleEntrySignals(ctx, todayStart)
	if err != nil {
		t.Fatalf("ExpireStaleEntrySignals: %v", err)
	}
	if n != 2 {
		t.Fatalf("expired %d rows, want exactly 2 (yesterday's PENDING+FAILED entries)", n)
	}

	assertStatus := func(id, want string) {
		t.Helper()
		var got string
		if err := db.QueryRow(`SELECT status FROM signal_inbox WHERE signal_id=$1`, id).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		if got != want {
			t.Errorf("%s: status=%s want %s", id, got, want)
		}
	}
	assertStatus("exp-test-y-pending", "DLQ")
	assertStatus("exp-test-y-failed", "DLQ")
	assertStatus("exp-test-y-done", "DONE")
	assertStatus("exp-test-t-pending", "PENDING")
	assertStatus("exp-test-y-slmod", "PENDING")
}
