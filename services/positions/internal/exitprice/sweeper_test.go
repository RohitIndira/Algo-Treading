package exitprice

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// FILATEX S4450 2026-10-23: booked at the stop's limit leg 89.30, real avg
// fill 91.084 on 239 shares bought at 83.37.
func TestCorrection_RebooksAtLedgerFill(t *testing.T) {
	pos := &store.Position{EntryPrice: 83.37, Quantity: 239, ExitPrice: 89.30, RealizedPnL: (89.30 - 83.37) * 239}
	meta := tradeexec.OrderMeta{Found: true, AvgFillPrice: 91.084}
	price, pnl, ok := Correction(pos, meta)
	if !ok {
		t.Fatal("a ledger fill that differs from the booked price must correct")
	}
	if price != 91.084 {
		t.Fatalf("price=%v want 91.084", price)
	}
	want := (91.084 - 83.37) * 239
	if diff := pnl - want; diff > 1e-6 || diff < -1e-6 {
		t.Fatalf("pnl=%v want %v", pnl, want)
	}
}

func TestCorrection_NoOps(t *testing.T) {
	base := &store.Position{EntryPrice: 100, Quantity: 10, ExitPrice: 95}
	cases := []struct {
		name string
		pos  *store.Position
		meta tradeexec.OrderMeta
	}{
		{"ledger has no fill yet", base, tradeexec.OrderMeta{Found: true, AvgFillPrice: 0}},
		{"not a Manthan order", base, tradeexec.OrderMeta{Found: false, AvgFillPrice: 97}},
		{"already matches", base, tradeexec.OrderMeta{Found: true, AvgFillPrice: 95.004}},
		{"zero quantity", &store.Position{EntryPrice: 100, Quantity: 0, ExitPrice: 95}, tradeexec.OrderMeta{Found: true, AvgFillPrice: 97}},
		{"nil position", nil, tradeexec.OrderMeta{Found: true, AvgFillPrice: 97}},
	}
	for _, c := range cases {
		if _, _, ok := Correction(c.pos, c.meta); ok {
			t.Errorf("%s: must not correct", c.name)
		}
	}
}

// A second sweep after a correction sees the booked price equal to the fill
// and does nothing — the sweeper converges.
func TestCorrection_Idempotent(t *testing.T) {
	pos := &store.Position{EntryPrice: 83.37, Quantity: 239, ExitPrice: 89.30}
	meta := tradeexec.OrderMeta{Found: true, AvgFillPrice: 91.084}
	price, pnl, _ := Correction(pos, meta)
	pos.ExitPrice, pos.RealizedPnL = price, pnl
	if _, _, ok := Correction(pos, meta); ok {
		t.Fatal("second pass must be a no-op")
	}
}

// ── SweepOnce with fakes: audit row shape + publish ─────────────────────────

type fakeStore struct {
	exits     []*store.Position
	recent    []*store.Position
	corrected []*store.PositionEvent
	prices    []float64
}

func (f *fakeStore) ListManthanExitsSince(ctx context.Context, since time.Time) ([]*store.Position, error) {
	return f.exits, nil
}

func (f *fakeStore) ListRecentlyCorrectedExits(ctx context.Context, since time.Time) ([]*store.Position, error) {
	return f.recent, nil
}

func (f *fakeStore) CorrectExitPriceWithEvent(ctx context.Context, id uuid.UUID, price, pnl float64, ev *store.PositionEvent) (bool, error) {
	if len(ev.RawSourceEvent) == 0 || !json.Valid(ev.RawSourceEvent) {
		return false, errors.New("pq: invalid input syntax for type json") // what the DB says to an empty jsonb
	}
	if ev.SourceTopic == "" || ev.SourceEventID == "" {
		return false, errors.New("NOT NULL violation: source_topic/source_event_id")
	}
	f.corrected = append(f.corrected, ev)
	f.prices = append(f.prices, price)
	return true, nil
}

type fakeLookup struct {
	meta  map[string]tradeexec.OrderMeta
	calls map[string]int
}

func (f *fakeLookup) LookupOrderMetaFresh(ctx context.Context, id string) (tradeexec.OrderMeta, error) {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[id]++
	return f.meta[id], nil
}

type fakePub struct{ events []*publisher.PositionEvent }

func (f *fakePub) Publish(ctx context.Context, ev *publisher.PositionEvent) {
	f.events = append(f.events, ev)
}

func TestSweepOnce_CorrectsAndPublishes(t *testing.T) {
	pid := uuid.New()
	st := &fakeStore{exits: []*store.Position{{
		PositionID: pid, Origin: "MANTHAN", UserID: "S4450", StrategyID: "strat", SignalID: "sig",
		Symbol: "FILATEX", EntryPrice: 83.37, Quantity: 239, ExitPrice: 89.30, ExitReason: "SL_TRIGGER",
		ExitBrokerOrderID: "NYMZX00014G:",
	}, {
		PositionID: uuid.New(), Origin: "MANTHAN", Symbol: "ALREADYOK", EntryPrice: 100, Quantity: 10, ExitPrice: 97,
		ExitBrokerOrderID: "OK1",
	}}}
	lk := &fakeLookup{meta: map[string]tradeexec.OrderMeta{
		"NYMZX00014G:": {Found: true, OrderType: "SL_SELL", AvgFillPrice: 91.084},
		"OK1":          {Found: true, OrderType: "SL_SELL", AvgFillPrice: 97},
	}}
	pub := &fakePub{}
	n := New(st, lk, pub, 0, zap.NewNop()).SweepOnce(context.Background())
	if n != 1 || len(st.corrected) != 1 || st.prices[0] != 91.084 {
		t.Fatalf("corrected=%d rows=%d prices=%v", n, len(st.corrected), st.prices)
	}
	ev := st.corrected[0]
	if ev.EventType != store.EventTypeExitPriceCorrected || ev.BrokerOrderID != "NYMZX00014G:" {
		t.Fatalf("audit row: %+v", ev)
	}
	if len(pub.events) != 1 || pub.events[0].EventType != publisher.EventPositionExitCorrected ||
		pub.events[0].Price != 91.084 || pub.events[0].PositionID != pid.String() || pub.events[0].ExitReason != "SL_TRIGGER" ||
		pub.events[0].SignalID != "sig" {
		t.Fatalf("published: %+v", pub.events)
	}
}

// A user's own sell that closed a lot is not a Manthan order: trade-execution
// answers Found=false once and the sweeper stops asking for 24 h. Lots
// corrected recently are re-published on every pass with their CURRENT values.
func TestSweepOnce_NotFoundIsRememberedAndRepublishRecent(t *testing.T) {
	manual := &store.Position{PositionID: uuid.New(), Origin: "MANTHAN", Symbol: "USERSOLD", EntryPrice: 10, Quantity: 5, ExitPrice: 9, ExitBrokerOrderID: "USER-1"}
	already := &store.Position{PositionID: uuid.New(), Origin: "MANTHAN", UserID: "S4450", StrategyID: "strat", SignalID: "sig",
		Symbol: "FIXED", EntryPrice: 100, Quantity: 10, ExitPrice: 97.5, RealizedPnL: -25, ExitReason: "SL_TRIGGER", ExitBrokerOrderID: "OK-2"}
	st := &fakeStore{exits: []*store.Position{manual}, recent: []*store.Position{already}}
	lk := &fakeLookup{meta: map[string]tradeexec.OrderMeta{"USER-1": {Found: false}}}
	pub := &fakePub{}
	sw := New(st, lk, pub, 0, zap.NewNop())
	sw.SweepOnce(context.Background())
	sw.SweepOnce(context.Background())
	if lk.calls["USER-1"] != 1 {
		t.Fatalf("not-found id asked %d times, want 1", lk.calls["USER-1"])
	}
	if len(st.corrected) != 0 {
		t.Fatalf("nothing should be corrected: %+v", st.corrected)
	}
	if len(pub.events) != 2 || pub.events[0].Symbol != "FIXED" || pub.events[0].Price != 97.5 || pub.events[0].RealizedPnL != -25 ||
		pub.events[0].EventType != publisher.EventPositionExitCorrected {
		t.Fatalf("republish: %+v", pub.events)
	}
}
