package manthan

// Regression for the 2026-08-18 MODISONLTD incident: an entry blocked only by
// an UPPER circuit was classified POISON (via the generic "pre-check:" rule)
// and DLQ'd with zero retries. An upper-circuit block is a WAIT state — no
// sellers exist at the band — and must retry until the band releases, then
// place. Lower circuit never blocks entries and gets no special handling.

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestClassify_UpperCircuitHoldRetries(t *testing.T) {
	// The exact error shape ExecuteEntry returns for a UC block.
	err := fmt.Errorf("%w: %s", ErrUpperCircuitHold, "stock at upper circuit — skip entry")
	if got := classifyHandlerErr(err); got != InboxErrUpperCircuit {
		t.Fatalf("UC hold classified %q, want %q (the DLQ bug is back)", got, InboxErrUpperCircuit)
	}
	// Wrapping deeper must still classify.
	if got := classifyHandlerErr(fmt.Errorf("dispatch: %w", err)); got != InboxErrUpperCircuit {
		t.Fatalf("wrapped UC hold classified %q", got)
	}
	// Sanity: message carries no classifier tokens that would shadow it.
	if got := classifyHandlerErr(errors.New(err.Error())); got == InboxErrPoison {
		t.Fatal("UC hold message text alone must not be POISON")
	}
}

func TestClassify_OtherPreChecksStayPoison(t *testing.T) {
	for _, msg := range []string{
		"pre-check: too close to market close (after 15:20)",
		"pre-check: market not open yet (before 9:15)",
		"pre-check: duplicate signal_id: abc",
	} {
		if got := classifyHandlerErr(errors.New(msg)); got != InboxErrPoison {
			t.Errorf("%q classified %q, want POISON", msg, got)
		}
	}
}

func TestBackoff_UpperCircuitFlatCadence(t *testing.T) {
	for _, attempts := range []int{1, 5, 20, 49} {
		if d := backoffFor(InboxErrUpperCircuit, attempts); d != 3*time.Minute {
			t.Errorf("attempt %d backoff = %v, want flat 3m (exponential would nap past the band release)", attempts, d)
		}
	}
}

func TestEvalCircuit_ZeroGuards(t *testing.T) {
	// Missing LTP → error, never a band verdict.
	if _, _, err := evalCircuit(0, 100, 90); err == nil {
		t.Error("ltp=0 must error, not report a circuit")
	}
	// Missing dpr_upper (0) previously faked an upper circuit for ANY ltp.
	up, low, err := evalCircuit(250, 0, 0)
	if err != nil || up || low {
		t.Errorf("missing bands: got up=%v low=%v err=%v, want all false/nil", up, low, err)
	}
	// Real upper circuit.
	if up, _, _ := evalCircuit(275, 275, 225); !up {
		t.Error("ltp==dpr_upper must report upper circuit")
	}
	// Real lower circuit.
	if _, low, _ := evalCircuit(225, 275, 225); !low {
		t.Error("ltp==dpr_lower must report lower circuit")
	}
	// Mid-band: neither.
	if up, low, _ := evalCircuit(250, 275, 225); up || low {
		t.Error("mid-band must be neither")
	}
}

// The review blocker: the worker computed "don't count this attempt" locally
// while the SQL ran attempts=attempts+1 unconditionally — the DB overrode the
// policy and a UC hold DLQ'd after ~2.5h anyway. MarkInboxFailed now persists
// the EXACT worker-computed value; this test pins that at the DB layer.
func TestMarkInboxFailed_PersistsWorkerComputedAttempts(t *testing.T) {
	db := openExecTestDB(t)
	r := NewRepository(db)
	ctx := t.Context()

	var id int64
	err := db.QueryRow(`
		INSERT INTO signal_inbox (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset, status, attempts)
		VALUES ('uc-persist-test', 'S4450', 'MANTHAN_ENTRY', '{}', 't', 0, 0, 'RUNNING', 7)
		RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("seed inbox row: %v", err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM signal_inbox WHERE id=$1`, id) })

	// UC hold: worker passes the UNCHANGED count (7). DB must store 7, not 8.
	if err := r.MarkInboxFailed(ctx, id, InboxErrUpperCircuit, "hold", time.Now().Add(time.Minute), 7); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	var got int
	if err := db.QueryRow(`SELECT attempts FROM signal_inbox WHERE id=$1`, id).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("attempts = %d, want 7 (SQL must not self-increment — the 2.5h-DLQ blocker)", got)
	}

	// Normal failure: worker passes the incremented count (8). DB stores 8.
	if err := r.MarkInboxFailed(ctx, id, InboxErrTransient, "boom", time.Now().Add(time.Second), 8); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT attempts, last_error_class FROM signal_inbox WHERE id=$1`, id).Scan(&got, new(string)); err != nil {
		t.Fatal(err)
	}
	if got != 8 {
		t.Fatalf("attempts = %d, want 8", got)
	}
}

// Reason string and sentinel routing are tied through the shared constant —
// a reworded pre-check literal can no longer silently revert to DLQ.
func TestUpperCircuitReasonConstantTiesLayers(t *testing.T) {
	err := fmt.Errorf("%w: %s", ErrUpperCircuitHold, ReasonUpperCircuit)
	if got := classifyHandlerErr(err); got != InboxErrUpperCircuit {
		t.Fatalf("constant-built error classified %q", got)
	}
}
