package store

// Broker-verdict columns (migration 007) round-trip through the store, and
// the NOT_HELD_AT_BROKER exit reason passes the exit_reason CHECK. Runs
// against the local positions_db; skips if unreachable.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBrokerState_RoundTripAndNotHeldExit(t *testing.T) {
	s := openPosTestDB(t)
	ctx := context.Background()
	posID, sig := uuid.New(), uuid.New()
	bid := "TESTBID-BS-" + posID.String()[:8]
	t.Cleanup(func() {
		s.db.ExecContext(ctx, `DELETE FROM position_events WHERE position_id=$1`, posID)
		s.db.ExecContext(ctx, `DELETE FROM positions WHERE position_id=$1`, posID)
	})
	p := &Position{
		PositionID: posID, Origin: OriginManthan, UserID: "TESTBS", StrategyID: uuid.New().String(),
		Symbol: "BSTEST", Exchange: "NSE", Status: StatusActive,
		EntryPrice: 50, Quantity: 10, InvestedAmount: 500, EntryBrokerOrderID: bid, SignalID: sig.String(),
	}
	if err := s.InsertEntryWithEvent(ctx, p, &PositionEvent{
		EventType: EventTypeEntryFilled, BrokerOrderID: bid, SignalID: sig.String(),
		DeltaQty: 10, FillPrice: 50, RawSourceEvent: []byte("{}"), SourceEventID: "evt-bs-" + posID.String()[:8],
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Fresh row: no verdict yet.
	lots, err := s.FindAllActiveLotsForUser(ctx, "TESTBS")
	if err != nil || len(lots) != 1 {
		t.Fatalf("lots: %v %d", err, len(lots))
	}
	if lots[0].BrokerState != "" || lots[0].BrokerStateReason != "" || !lots[0].BrokerCheckedAt.IsZero() {
		t.Fatalf("new lot must carry no verdict: %+v", lots[0])
	}

	// Verdict written and read back.
	at := time.Now().Add(-time.Minute).Truncate(time.Second)
	ok, err := s.SetBrokerState(ctx, posID, BrokerStatePledged,
		"Pledged: 10 of 10 pledged, 0 free — unpledge to let the strategy manage it",
		[]byte(`{"broker_qty":10,"pledged_qty":10}`), at, "login")
	if err != nil || !ok {
		t.Fatalf("SetBrokerState: ok=%v err=%v", ok, err)
	}
	lots, _ = s.FindAllActiveLotsForUser(ctx, "TESTBS")
	got := lots[0]
	if got.BrokerState != BrokerStatePledged || got.BrokerStateReason == "" || !got.BrokerCheckedAt.Equal(at) {
		t.Fatalf("verdict not round-tripped: %+v", got)
	}
	var source, evidence string
	if err := s.db.QueryRowContext(ctx, `SELECT broker_state_source, broker_state_evidence::text FROM positions WHERE position_id=$1`, posID).Scan(&source, &evidence); err != nil {
		t.Fatalf("read: %v", err)
	}
	if source != "login" || evidence == "" {
		t.Fatalf("source/evidence: %q %q", source, evidence)
	}

	// An unknown state is rejected by the CHECK.
	if _, err := s.SetBrokerState(ctx, posID, "BOGUS", "x", nil, at, "sweep"); err == nil {
		t.Fatalf("CHECK must reject an unknown broker_state")
	}

	// NOT_HELD_AT_BROKER closes the lot (exit price unknown → 0 is stored as
	// a numeric here by UpdateExitWithEvent; the reconciler writes NULL
	// directly) and passes the exit_reason CHECK.
	if err := s.UpdateExitWithEvent(ctx, posID, 0, "", ExitReasonNotHeldAtBroker, 0, &PositionEvent{
		EventType: EventTypeReconcilerDriftFix, DeltaQty: -10, RawSourceEvent: []byte("{}"),
		SourceEventID: "evt-bs-exit-" + posID.String()[:8], Reason: "broker holds 0",
	}); err != nil {
		t.Fatalf("NOT_HELD_AT_BROKER exit rejected: %v", err)
	}
	var status, reason string
	_ = s.db.QueryRowContext(ctx, `SELECT status, exit_reason FROM positions WHERE position_id=$1`, posID).Scan(&status, &reason)
	if status != StatusExited || reason != ExitReasonNotHeldAtBroker {
		t.Fatalf("after exit: %s %s", status, reason)
	}
	// Verdict writes land on a closed lot too (a closure records the verdict
	// that caused it), and clear the stale flag.
	if _, err := s.db.ExecContext(ctx, `UPDATE positions SET broker_state_stale = true WHERE position_id = $1`, posID); err != nil {
		t.Fatalf("set stale: %v", err)
	}
	if ok, err := s.SetBrokerState(ctx, posID, BrokerStateNotHeld, "Broker holds 0", nil, at, "sweep"); err != nil || !ok {
		t.Fatalf("SetBrokerState on EXITED must write: ok=%v err=%v", ok, err)
	}
	var stale bool
	_ = s.db.QueryRowContext(ctx, `SELECT broker_state, broker_state_stale FROM positions WHERE position_id=$1`, posID).Scan(&status, &stale)
	if status != BrokerStateNotHeld || stale {
		t.Fatalf("after write on EXITED: state=%s stale=%v", status, stale)
	}
}
