package statemachine

// Asserts publisher wiring — every state transition emits ONE
// position.events envelope with the right shape.
//
// Uses a capturing stub instead of a real Kafka writer so tests stay hermetic.

import (
	"context"
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"

	"go.uber.org/zap"
)

// capturingPublisher records every Publish call so tests can assert
// on the envelope shape.
type capturingPublisher struct {
	events []*publisher.PositionEvent
}

func (c *capturingPublisher) Publish(_ context.Context, ev *publisher.PositionEvent) {
	// Deep-copy so mutations after Publish don't affect what we captured.
	cp := *ev
	c.events = append(c.events, &cp)
}

// newHandlerWithPub is a variant of newHandler that attaches a capturing
// publisher so we can inspect the fan-out on the wire.
func newHandlerWithPub(t *testing.T, cap *capturingPublisher, resp map[string]tradeexec.OrderMeta) *Handler {
	t.Helper()
	db := openTestDB(t)
	logger, _ := zap.NewDevelopment()
	// Note: db handle intentionally kept open — tests here don't call Close
	// because openTestDB registers TRUNCATE per call and the sql.DB is
	// process-scoped in tests.
	return New(store.New(db, logger), &stubLookup{responses: resp}, cap, logger)
}

func TestPublisher_PositionOpenedOnBuy(t *testing.T) {
	cap := &capturingPublisher{}
	stub := map[string]tradeexec.OrderMeta{
		"BROKER-A": {
			Found: true, SignalID: "11111111-1111-1111-1111-111111111111",
			OrderType: "ENTRY", StrategyID: "22222222-2222-2222-2222-222222222222",
			UserID: "S4450", EntrySignalID: "11111111-1111-1111-1111-111111111111",
			EntryBrokerOrderID: "BROKER-A",
		},
	}
	h := newHandlerWithPub(t, cap, stub)

	ev := fillEvent("evt-A", "BROKER-A", "S4450", "IDEA", "1", 100, 14.09)
	if err := h.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(cap.events) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(cap.events))
	}
	got := cap.events[0]
	if got.EventType != publisher.EventPositionOpened {
		t.Errorf("event_type: got %q, want POSITION_OPENED", got.EventType)
	}
	if got.Origin != store.OriginManthan {
		t.Errorf("origin: got %q, want MANTHAN", got.Origin)
	}
	if got.Action != publisher.ActionEntry {
		t.Errorf("action: got %q, want ENTRY", got.Action)
	}
	if got.Price != 14.09 {
		t.Errorf("price: got %v, want 14.09", got.Price)
	}
	if got.Quantity != 100 {
		t.Errorf("quantity: got %d, want 100", got.Quantity)
	}
	if got.SignalID != stub["BROKER-A"].SignalID {
		t.Errorf("signal_id: got %q, want %q", got.SignalID, stub["BROKER-A"].SignalID)
	}
	if got.PositionID == "" {
		t.Error("position_id must be populated")
	}
}

func TestPublisher_PositionExitedOnManthanSell(t *testing.T) {
	cap := &capturingPublisher{}
	entryBID := "TB-ENTRY-Z"
	slBID := "TB-SL-Z"
	entrySig := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	slSig := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	stub := map[string]tradeexec.OrderMeta{
		entryBID: {
			Found: true, SignalID: entrySig, OrderType: "ENTRY",
			StrategyID: "22222222-2222-2222-2222-222222222222", UserID: "S4450",
			EntrySignalID: entrySig, EntryBrokerOrderID: entryBID,
		},
		slBID: {
			Found: true, SignalID: slSig, OrderType: "SL_SELL",
			StrategyID: "22222222-2222-2222-2222-222222222222", UserID: "S4450",
			EntrySignalID: entrySig, EntryBrokerOrderID: entryBID,
		},
	}
	h := newHandlerWithPub(t, cap, stub)

	buy := fillEvent("evt-buy", entryBID, "S4450", "IDEA", "1", 100, 14.09)
	if err := h.Handle(context.Background(), buy); err != nil {
		t.Fatalf("buy: %v", err)
	}
	sell := fillEvent("evt-sell", slBID, "S4450", "IDEA", "2", 100, 13.50)
	if err := h.Handle(context.Background(), sell); err != nil {
		t.Fatalf("sell: %v", err)
	}

	if len(cap.events) != 2 {
		t.Fatalf("expected 2 events (open + exit), got %d", len(cap.events))
	}
	exit := cap.events[1]
	if exit.EventType != publisher.EventPositionExited {
		t.Errorf("event_type: got %q, want POSITION_EXITED", exit.EventType)
	}
	if exit.ExitReason != store.ExitReasonSLTrigger {
		t.Errorf("exit_reason: got %q, want SL_TRIGGER", exit.ExitReason)
	}
	wantPnL := (13.50 - 14.09) * 100.0
	if diff := exit.RealizedPnL - wantPnL; diff > 0.01 || diff < -0.01 {
		t.Errorf("realized_pnl: got %v, want %v", exit.RealizedPnL, wantPnL)
	}
}

func TestPublisher_ManualSellEmitsPerTouchedLot(t *testing.T) {
	cap := &capturingPublisher{}
	manthanBID := "TB-M2"
	manualBID := "TB-U2"

	stub := map[string]tradeexec.OrderMeta{
		manthanBID: {
			Found: true, SignalID: "cccccccc-cccc-cccc-cccc-cccccccccccc",
			OrderType: "ENTRY", StrategyID: "22222222-2222-2222-2222-222222222222", UserID: "S4450",
			EntrySignalID: "cccccccc-cccc-cccc-cccc-cccccccccccc",
			EntryBrokerOrderID: manthanBID,
		},
	}
	h := newHandlerWithPub(t, cap, stub)

	// Setup: 20 MANTHAN + 10 USER_MANUAL
	buyM := fillEvent("m", manthanBID, "S4450", "SBI", "1", 20, 500)
	if err := h.Handle(context.Background(), buyM); err != nil {
		t.Fatalf("m: %v", err)
	}
	buyU := fillEvent("u", manualBID, "S4450", "SBI", "1", 10, 510)
	if err := h.Handle(context.Background(), buyU); err != nil {
		t.Fatalf("u: %v", err)
	}

	// Manual sell 15 → both lots touched
	sell := fillEvent("sell", "BROKER-USELL", "S4450", "SBI", "2", 15, 520)
	if err := h.Handle(context.Background(), sell); err != nil {
		t.Fatalf("sell: %v", err)
	}

	// Expected: 2 OPENED + 2 lot-touch events = 4 total
	if len(cap.events) != 4 {
		t.Fatalf("expected 4 events, got %d: %+v", len(cap.events), cap.events)
	}

	// Third event: USER_MANUAL row fully closed → EventPositionExited
	// Fourth event: MANTHAN row partial → EventManualInterrupt
	touchUM := cap.events[2]
	touchMN := cap.events[3]

	if touchUM.Origin != store.OriginUserManual || touchUM.EventType != publisher.EventPositionExited {
		t.Errorf("USER_MANUAL touch: got type=%q origin=%q, want POSITION_EXITED USER_MANUAL",
			touchUM.EventType, touchUM.Origin)
	}
	if touchMN.Origin != store.OriginManthan || touchMN.EventType != publisher.EventManualInterrupt {
		t.Errorf("MANTHAN touch: got type=%q origin=%q, want MANUAL_INTERRUPT MANTHAN",
			touchMN.EventType, touchMN.Origin)
	}
}
