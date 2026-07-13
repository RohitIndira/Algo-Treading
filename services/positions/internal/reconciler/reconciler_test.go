package reconciler

// Tests for Chunk P.G drift detection.
//
// Detect() unit tests use the pure API — no DB required.
// DetectAndPublish integration tests hit a real positions_db plus a
// capturing DriftEmitter so we can assert on the wire.

import (
	"context"
	"fmt"
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// mkPos builds a *store.Position for tests — only the fields Detect reads.
func mkPos(origin, symbol string, qty int) *store.Position {
	return &store.Position{
		PositionID: uuid.New(),
		Origin:     origin,
		Symbol:     symbol,
		Status:     store.StatusActive,
		Quantity:   qty,
	}
}

// -------------------------------------------------------------------------
// Detect() — pure logic tests
// -------------------------------------------------------------------------

func TestDetect_NoDriftWhenAligned(t *testing.T) {
	lots := []*store.Position{mkPos(store.OriginManthan, "SBI", 20)}
	broker := map[string]int{"SBI": 20}

	drifts := Detect("S4450", lots, broker)
	if len(drifts) != 0 {
		t.Fatalf("expected 0 drifts, got %d: %+v", len(drifts), drifts)
	}
}

func TestDetect_QtyMismatch(t *testing.T) {
	lots := []*store.Position{mkPos(store.OriginManthan, "SBI", 20)}
	broker := map[string]int{"SBI": 15}

	drifts := Detect("S4450", lots, broker)
	if len(drifts) != 1 {
		t.Fatalf("expected 1 drift, got %d", len(drifts))
	}
	d := drifts[0]
	if d.DriftType != publisher.DriftQtyMismatch {
		t.Errorf("type: got %q, want %q", d.DriftType, publisher.DriftQtyMismatch)
	}
	if d.BrokerQty != 15 || d.OurQty != 20 {
		t.Errorf("qty: broker=%d our=%d, want 15/20", d.BrokerQty, d.OurQty)
	}
	if len(d.PositionIDs) != 1 {
		t.Errorf("position_ids: got %v, want 1", d.PositionIDs)
	}
}

func TestDetect_BrokerOnly(t *testing.T) {
	broker := map[string]int{"UNTRACKED": 100}

	drifts := Detect("S4450", nil, broker)
	if len(drifts) != 1 {
		t.Fatalf("expected 1 drift, got %d", len(drifts))
	}
	d := drifts[0]
	if d.DriftType != publisher.DriftBrokerOnly {
		t.Errorf("type: got %q, want BROKER_ONLY", d.DriftType)
	}
	if d.BrokerQty != 100 || d.OurQty != 0 {
		t.Errorf("qty: broker=%d our=%d, want 100/0", d.BrokerQty, d.OurQty)
	}
	if len(d.PositionIDs) != 0 {
		t.Errorf("position_ids: got %v, want empty", d.PositionIDs)
	}
}

func TestDetect_DBOnly(t *testing.T) {
	lots := []*store.Position{mkPos(store.OriginManthan, "PHANTOM", 50)}

	drifts := Detect("S4450", lots, map[string]int{})
	if len(drifts) != 1 {
		t.Fatalf("expected 1 drift, got %d", len(drifts))
	}
	d := drifts[0]
	if d.DriftType != publisher.DriftDBOnly {
		t.Errorf("type: got %q, want DB_ONLY", d.DriftType)
	}
	if d.BrokerQty != 0 || d.OurQty != 50 {
		t.Errorf("qty: broker=%d our=%d, want 0/50", d.BrokerQty, d.OurQty)
	}
}

func TestDetect_BrokerQtyZeroIsNotDrift(t *testing.T) {
	broker := map[string]int{"NONE": 0}
	drifts := Detect("S4450", nil, broker)
	if len(drifts) != 0 {
		t.Fatalf("qty=0 must not surface as drift, got %d", len(drifts))
	}
}

func TestDetect_MultipleLotsAggregatedIntoOneEvent(t *testing.T) {
	// Two ACTIVE MANTHAN lots on the same symbol.
	lot1 := mkPos(store.OriginManthan, "SBI", 20)
	lot2 := mkPos(store.OriginManthan, "SBI", 15)
	lots := []*store.Position{lot1, lot2}
	broker := map[string]int{"SBI": 30} // 5 short of 35

	drifts := Detect("S4450", lots, broker)
	if len(drifts) != 1 {
		t.Fatalf("expected 1 drift (aggregated), got %d", len(drifts))
	}
	d := drifts[0]
	if d.OurQty != 35 {
		t.Errorf("our_qty: got %d, want 35 (20+15)", d.OurQty)
	}
	if len(d.PositionIDs) != 2 {
		t.Errorf("position_ids: got %v, want 2 lots", d.PositionIDs)
	}
	// Since both lots are MANTHAN, origin split should NOT be populated.
	if d.ManthanQty != 0 || d.UserManualQty != 0 {
		t.Errorf("single-origin drift must not emit split, got manthan=%d user=%d",
			d.ManthanQty, d.UserManualQty)
	}
}

func TestDetect_OriginBreakdownForMixedLots(t *testing.T) {
	// 20 MANTHAN + 10 USER_MANUAL on same symbol; broker reports 25 → mismatch.
	lots := []*store.Position{
		mkPos(store.OriginManthan, "SBI", 20),
		mkPos(store.OriginUserManual, "SBI", 10),
	}
	broker := map[string]int{"SBI": 25}

	drifts := Detect("S4450", lots, broker)
	if len(drifts) != 1 {
		t.Fatalf("expected 1 drift, got %d", len(drifts))
	}
	d := drifts[0]
	if d.OurQty != 30 || d.BrokerQty != 25 {
		t.Errorf("qty: broker=%d our=%d, want 25/30", d.BrokerQty, d.OurQty)
	}
	if d.ManthanQty != 20 {
		t.Errorf("manthan_qty: got %d, want 20", d.ManthanQty)
	}
	if d.UserManualQty != 10 {
		t.Errorf("user_manual_qty: got %d, want 10", d.UserManualQty)
	}
}

func TestDetect_MixOfAlignedAndDrifted(t *testing.T) {
	// SBI aligned. IDEA drifted. UNTRACKED broker-only.
	lots := []*store.Position{
		mkPos(store.OriginManthan, "SBI", 20),
		mkPos(store.OriginManthan, "IDEA", 100),
	}
	broker := map[string]int{
		"SBI":       20,  // aligned
		"IDEA":      90,  // qty mismatch
		"UNTRACKED": 10,  // broker-only
	}

	drifts := Detect("S4450", lots, broker)
	if len(drifts) != 2 {
		t.Fatalf("expected 2 drifts, got %d: %+v", len(drifts), drifts)
	}

	// Assert types are present (order-independent).
	seen := map[string]publisher.DriftEvent{}
	for _, d := range drifts {
		seen[d.Symbol] = d
	}
	if seen["IDEA"].DriftType != publisher.DriftQtyMismatch {
		t.Errorf("IDEA: got %q, want QTY_MISMATCH", seen["IDEA"].DriftType)
	}
	if seen["UNTRACKED"].DriftType != publisher.DriftBrokerOnly {
		t.Errorf("UNTRACKED: got %q, want BROKER_ONLY", seen["UNTRACKED"].DriftType)
	}
	if _, ok := seen["SBI"]; ok {
		t.Errorf("SBI is aligned, must not appear in drifts")
	}
}

// -------------------------------------------------------------------------
// DetectAndPublish — pipes into a capturing emitter and (optional) real DB
// -------------------------------------------------------------------------

// capturingEmitter records every publish for assertions.
type capturingEmitter struct {
	events []publisher.DriftEvent
}

func (c *capturingEmitter) PublishDrift(_ context.Context, ev *publisher.DriftEvent) {
	cp := *ev
	c.events = append(c.events, cp)
}

// stubSource stubs the store — useful when we don't need a real DB.
type stubSource struct {
	lots []*store.Position
}

func (s *stubSource) FindAllActiveLotsForUser(_ context.Context, _ string) ([]*store.Position, error) {
	return s.lots, nil
}

func TestDetectAndPublish_EmitsOneEventPerDrift(t *testing.T) {
	src := &stubSource{lots: []*store.Position{
		mkPos(store.OriginManthan, "IDEA", 100),
		mkPos(store.OriginManthan, "SBI", 20),
	}}
	emitter := &capturingEmitter{}
	logger, _ := zap.NewDevelopment()

	r := New(src, emitter, logger)

	broker := map[string]int{"IDEA": 100, "SBI": 15, "PHANTOM": 5}
	n, err := r.DetectAndPublish(context.Background(), "S4450", broker)
	if err != nil {
		t.Fatalf("DetectAndPublish: %v", err)
	}
	if n != 2 {
		t.Errorf("count: got %d, want 2 (SBI qty mismatch + PHANTOM broker-only)", n)
	}
	if len(emitter.events) != 2 {
		t.Fatalf("emitter received %d events, want 2", len(emitter.events))
	}
	// EventID + DetectedAtMs must be filled in downstream by publisher.
	// Here we only check they made it out untouched (publisher stamps defaults).
	for _, ev := range emitter.events {
		if ev.UserID != "S4450" {
			t.Errorf("user_id: got %q, want S4450", ev.UserID)
		}
		if ev.Symbol == "" || ev.DriftType == "" {
			t.Errorf("event fields missing: %+v", ev)
		}
	}
}

// -------------------------------------------------------------------------
// sweepOnce — periodic tick behavior
// -------------------------------------------------------------------------

// stubUsers records DistinctUsersWithActive calls + returns a fixed set.
type stubUsers struct {
	users []string
	err   error
	calls int
}

func (s *stubUsers) DistinctUsersWithActive(_ context.Context) ([]string, error) {
	s.calls++
	return s.users, s.err
}

// stubHoldings returns a per-user holdings map. err by userID lets tests
// mimic a broker fetch that succeeds for user A but fails for user B.
type stubHoldings struct {
	byUser  map[string]map[string]int
	errByID map[string]error
	calls   int
}

func (s *stubHoldings) GetBrokerHoldings(_ context.Context, userID string) (map[string]int, error) {
	s.calls++
	if e, ok := s.errByID[userID]; ok {
		return nil, e
	}
	return s.byUser[userID], nil
}

// perUserSource routes FindAllActiveLotsForUser by userID.
type perUserSource struct {
	byUser map[string][]*store.Position
}

func (p *perUserSource) FindAllActiveLotsForUser(_ context.Context, userID string) ([]*store.Position, error) {
	return p.byUser[userID], nil
}

func TestSweepOnce_FetchFailureDoesNotPublishFalseDrift(t *testing.T) {
	// User has one MANTHAN lot for SBI at qty=20. If the reconciler
	// interpreted a fetch failure as "broker has nothing", it would
	// publish a false DB_ONLY drift. That must NOT happen.
	src := &perUserSource{byUser: map[string][]*store.Position{
		"S4450": {mkPos(store.OriginManthan, "SBI", 20)},
	}}
	users := &stubUsers{users: []string{"S4450"}}
	holdings := &stubHoldings{errByID: map[string]error{
		"S4450": fmt.Errorf("indira 401"),
	}}
	emitter := &capturingEmitter{}
	logger, _ := zap.NewDevelopment()

	r := NewWithTicker(src, users, holdings, emitter, logger)
	r.sweepOnce(context.Background())

	if users.calls != 1 {
		t.Errorf("users.calls: got %d, want 1", users.calls)
	}
	if holdings.calls != 1 {
		t.Errorf("holdings.calls: got %d, want 1", holdings.calls)
	}
	if len(emitter.events) != 0 {
		t.Errorf("published %d drift events on broker fetch failure — must be 0: %+v",
			len(emitter.events), emitter.events)
	}
}

func TestSweepOnce_MultiUserAllOK(t *testing.T) {
	src := &perUserSource{byUser: map[string][]*store.Position{
		"S4450": {mkPos(store.OriginManthan, "SBI", 20)},
		"U0001": {mkPos(store.OriginUserManual, "IDEA", 100)},
	}}
	users := &stubUsers{users: []string{"S4450", "U0001"}}
	holdings := &stubHoldings{byUser: map[string]map[string]int{
		"S4450": {"SBI": 20},        // aligned → 0 drifts
		"U0001": {"IDEA": 90},       // qty mismatch → 1 drift
	}}
	emitter := &capturingEmitter{}
	r := NewWithTicker(src, users, holdings, emitter, nil)

	r.sweepOnce(context.Background())

	if holdings.calls != 2 {
		t.Errorf("expected holdings fetched for both users, got %d", holdings.calls)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("want 1 drift (U0001 IDEA), got %d: %+v", len(emitter.events), emitter.events)
	}
	if emitter.events[0].UserID != "U0001" || emitter.events[0].Symbol != "IDEA" {
		t.Errorf("wrong drift: got %+v", emitter.events[0])
	}
}

func TestSweepOnce_UsersFetchFailureNoOp(t *testing.T) {
	users := &stubUsers{err: fmt.Errorf("db down")}
	holdings := &stubHoldings{}
	emitter := &capturingEmitter{}
	r := NewWithTicker(&perUserSource{}, users, holdings, emitter, nil)

	r.sweepOnce(context.Background())

	if holdings.calls != 0 {
		t.Errorf("holdings must not be called when users listing fails, got %d", holdings.calls)
	}
	if len(emitter.events) != 0 {
		t.Errorf("published %d events on users-fetch failure — must be 0", len(emitter.events))
	}
}

// -------------------------------------------------------------------------
// DetectAndPublish (rest of old tests)
// -------------------------------------------------------------------------

func TestDetectAndPublish_NoDriftEmitsNothing(t *testing.T) {
	src := &stubSource{lots: []*store.Position{
		mkPos(store.OriginManthan, "SBI", 20),
	}}
	emitter := &capturingEmitter{}
	r := New(src, emitter, nil)

	n, err := r.DetectAndPublish(context.Background(), "S4450", map[string]int{"SBI": 20})
	if err != nil {
		t.Fatalf("DetectAndPublish: %v", err)
	}
	if n != 0 || len(emitter.events) != 0 {
		t.Errorf("aligned: got %d drifts (%d emitted), want 0", n, len(emitter.events))
	}
}
