package manthan

// Manual-exit ledger projector tests (formation fix, 2026-09-03). The
// consumer projects positions-svc verdicts into manthan_orders — it decides
// nothing itself, so these tests pin the filter chain, the idempotency-key
// shape, retry semantics, and (regression) that the retired no-op publisher
// stays a no-op.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go.uber.org/zap"
)

type fakeLedger struct {
	rows      []ManualExitLedgerRow
	insertErr error
	dupNext   bool // next insert reports already-present
	ownExit   bool // HasOwnExitSell answer: the lot's sell was placed by us
}

func (f *fakeLedger) HasOwnExitSell(_ context.Context, _, _ string, _ int) (bool, error) {
	return f.ownExit, nil
}

func (f *fakeLedger) HasOwnExitSellByBrokerID(_ context.Context, _ string) (bool, error) {
	return f.ownExit, nil
}

func (f *fakeLedger) InsertManualExitLedgerSell(_ context.Context, e ManualExitLedgerRow) (bool, error) {
	if f.insertErr != nil {
		return false, f.insertErr
	}
	if f.dupNext {
		return false, nil
	}
	f.rows = append(f.rows, e)
	return true, nil
}

func ledgerConsumer(l *fakeLedger) *ManualExitLedgerConsumer {
	return &ManualExitLedgerConsumer{ledger: l, logger: zap.NewNop()}
}

func manualExitJSON(overrides map[string]any) []byte {
	m := map[string]any{
		"event_id":        "evt-1",
		"event_type":      "POSITION_EXITED",
		"position_id":     "11111111-2222-3333-4444-555555555555",
		"origin":          "MANTHAN",
		"user_id":         "FIV99",
		"strategy_id":     "e36bd07e-4eb9-48c0-8425-62ce25715149",
		"signal_id":       "sig-1",
		"symbol":          "SHANTIGOLD",
		"price":           250.5,
		"quantity":        149,
		"exit_reason":     "MANUAL_EXIT",
		"broker_order_id": "3RDPARTY01",
	}
	for k, v := range overrides {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return b
}

// Formation test 1+2: a confirmed full manual exit of a MANTHAN lot is
// projected into the ledger with the stable idempotency key.
func TestManualExitLedger_ProjectsConfirmedManualExit(t *testing.T) {
	l := &fakeLedger{}
	if err := ledgerConsumer(l).HandleMessage(context.Background(), manualExitJSON(nil)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(l.rows) != 1 {
		t.Fatalf("expected 1 ledger row, got %d", len(l.rows))
	}
	r := l.rows[0]
	if r.SignalID != "manualexit-11111111-2222-3333-4444-555555555555" {
		t.Fatalf("idempotency key wrong: %s", r.SignalID)
	}
	if r.Qty != 149 || r.ExitPrice != 250.5 || r.UserID != "FIV99" || r.Symbol != "SHANTIGOLD" ||
		r.StrategyID != "e36bd07e-4eb9-48c0-8425-62ce25715149" || r.BrokerOrderID != "3RDPARTY01" {
		t.Fatalf("row fields wrong: %+v", r)
	}
}

// USER_MANUAL lots are not in our ledger — never projected.
func TestManualExitLedger_IgnoresUserManualOrigin(t *testing.T) {
	l := &fakeLedger{}
	msg := manualExitJSON(map[string]any{"origin": "USER_MANUAL", "strategy_id": ""})
	if err := ledgerConsumer(l).HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(l.rows) != 0 {
		t.Fatalf("USER_MANUAL projected: %+v", l.rows)
	}
}

// SL-driven exits are OUR orders — the normal fill path owns the ledger row.
func TestManualExitLedger_IgnoresSLTriggerExits(t *testing.T) {
	l := &fakeLedger{}
	msg := manualExitJSON(map[string]any{"exit_reason": "SL_TRIGGER"})
	if err := ledgerConsumer(l).HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(l.rows) != 0 {
		t.Fatalf("SL exit projected as manual: %+v", l.rows)
	}
}

// Partial manual exits keep the position legitimately open — no full-close
// ledger row (Scenario C discipline: no hard full-exit evidence, no write).
func TestManualExitLedger_PartialExitNotProjected(t *testing.T) {
	l := &fakeLedger{}
	msg := manualExitJSON(map[string]any{"event_type": "MANUAL_INTERRUPT", "quantity": 40})
	if err := ledgerConsumer(l).HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(l.rows) != 0 {
		t.Fatalf("partial exit projected as full close: %+v", l.rows)
	}
}

// Missing required fields → dropped (committed), never inserted.
func TestManualExitLedger_MissingFieldsDropped(t *testing.T) {
	for _, drop := range []string{"position_id", "user_id", "symbol", "strategy_id"} {
		l := &fakeLedger{}
		msg := manualExitJSON(map[string]any{drop: ""})
		if err := ledgerConsumer(l).HandleMessage(context.Background(), msg); err != nil {
			t.Fatalf("drop %s: %v", drop, err)
		}
		if len(l.rows) != 0 {
			t.Fatalf("inserted despite missing %s", drop)
		}
	}
	l := &fakeLedger{}
	if err := ledgerConsumer(l).HandleMessage(context.Background(), manualExitJSON(map[string]any{"quantity": 0})); err != nil {
		t.Fatalf("qty 0: %v", err)
	}
	if len(l.rows) != 0 {
		t.Fatal("inserted with qty 0")
	}
}

// DB failure returns an error so the offset is NOT committed and Kafka
// re-delivers — at-least-once + idempotent insert.
func TestManualExitLedger_DBErrorRetries(t *testing.T) {
	l := &fakeLedger{insertErr: errors.New("db down")}
	if err := ledgerConsumer(l).HandleMessage(context.Background(), manualExitJSON(nil)); err == nil {
		t.Fatal("expected retryable error on DB failure")
	}
}

// Replay of an already-recorded exit is a clean no-op (inserted=false).
func TestManualExitLedger_ReplayIsIdempotent(t *testing.T) {
	l := &fakeLedger{dupNext: true}
	if err := ledgerConsumer(l).HandleMessage(context.Background(), manualExitJSON(nil)); err != nil {
		t.Fatalf("replay must commit cleanly: %v", err)
	}
	if len(l.rows) != 0 {
		t.Fatal("replay inserted a second row")
	}
}

// Malformed JSON drops (commit) rather than poison-pilling the partition.
func TestManualExitLedger_BadJSONDropped(t *testing.T) {
	l := &fakeLedger{}
	if err := ledgerConsumer(l).HandleMessage(context.Background(), []byte("{not json")); err != nil {
		t.Fatalf("bad json must not error: %v", err)
	}
}

// REGRESSION PIN (ghost investigation 2026-09-03): the retired
// manthan.execution.events publisher must remain a NO-OP. If someone rewires
// it without resurrecting consumers, manual-exit/detector events would again
// vanish silently — the original ghost-formation root cause. Any deliberate
// revival must go through the order.events / position.events architecture
// and delete this pin consciously.
func TestManthanEventPublisher_RemainsRetiredNoOp(t *testing.T) {
	p := NewManthanEventPublisher([]string{"localhost:9092"}, zap.NewNop())
	if p.writer != nil {
		t.Fatal("ManthanEventPublisher constructed a live writer — the retired manthan.execution.events path must stay a no-op (see event_publisher.go)")
	}
}

// 2026-10-22: positions svc labels OUR exit sells MANUAL_EXIT as well (they
// reach it through the whole-book path). When an exit row of ours exists for
// the lot, the ledger must not add a synthetic SELL on top — that
// double-counted APARINDS (net −1).
func TestManualExitLedger_SkipsOurOwnExit(t *testing.T) {
	l := &fakeLedger{ownExit: true}
	c := ledgerConsumer(l)
	if err := c.HandleMessage(context.Background(), manualExitJSON(nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(l.rows) != 0 {
		t.Fatalf("synthetic SELL recorded for our own exit: %+v", l.rows)
	}
}

// A deterministic DB rejection must be dropped (offset committed), not
// re-delivered forever — it blocked every later position event on 2026-10-22.
func TestManualExitLedger_DropsNonRetryableInsertError(t *testing.T) {
	l := &fakeLedger{insertErr: errors.New(`InsertManualExitLedgerSell: pq: invalid input syntax for type integer: "204.33"`)}
	c := ledgerConsumer(l)
	if err := c.HandleMessage(context.Background(), manualExitJSON(nil)); err != nil {
		t.Fatalf("non-retryable insert error must be dropped, got %v", err)
	}
	l2 := &fakeLedger{insertErr: errors.New("dial tcp: connection refused")}
	if err := ledgerConsumer(l2).HandleMessage(context.Background(), manualExitJSON(nil)); err == nil {
		t.Fatal("transient insert error must still be returned for re-delivery")
	}
}
