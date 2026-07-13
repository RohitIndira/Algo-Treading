package statemachine

// State-machine tests run against a REAL local Postgres (positions_db).
// Validates the money-critical behaviour that fixes realized_pnl=0:
//
//   TestBuyFillCreatesManthanRow      — LookupOrderMeta returns Found=true
//                                       → positions row with origin=MANTHAN
//                                         + signal_id populated
//   TestBuyFillCreatesUserManualRow   — Found=false → origin=USER_MANUAL
//   TestBuyReplayIsIdempotent         — same Kafka event twice → 1 row
//   TestManthanSellComputesRealizedPnl — the whole point of this refactor
//   TestManualSellFIFOAcrossOrigins   — USER_MANUAL first, spillover MANTHAN
//
// Prereq: positions_db running on localhost:5432, migration 001 applied.
// Tests TRUNCATE state before each run.

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
)

// stubLookup drives Handler.lookup behaviour without spinning up trade-exec.
type stubLookup struct {
	responses map[string]tradeexec.OrderMeta
}

func (s *stubLookup) LookupOrderMeta(_ context.Context, brokerOrderID string) (tradeexec.OrderMeta, error) {
	if s.responses == nil {
		return tradeexec.OrderMeta{Found: false}, nil
	}
	m, ok := s.responses[brokerOrderID]
	if !ok {
		return tradeexec.OrderMeta{Found: false}, nil
	}
	return m, nil
}

// -- test helpers ------------------------------------------------------

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=positions_db sslmode=disable")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("positions_db not reachable — skipping DB tests (%v)", err)
	}
	// TRUNCATE via position_events → positions FK CASCADE.
	if _, err := db.Exec(`TRUNCATE positions, position_events RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return db
}

func newHandler(t *testing.T, db *sql.DB, resp map[string]tradeexec.OrderMeta) *Handler {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	// Tests skip the publisher — nil is safe (Publish is a no-op).
	return New(store.New(db, logger), &stubLookup{responses: resp}, nil, logger)
}

func fillEvent(eventID, brokerOrderID, userID, symbol, buySell string, qty int, tradedPrice float64) *consumer.OrderEvent {
	ev := &consumer.OrderEvent{
		EventID:       eventID,
		EventType:     "FILLED",
		EventSeq:      time.Now().UnixMicro(),
		Source:        "WSS",
		BrokerOrderID: brokerOrderID,
		UserID:        userID,
		Symbol:        symbol,
		Exchange:      "NSE",
		BuySell:       buySell,
		OrderType:     "REGULAR MARKET",
		Status:        "EXECUTED",
		Quantity:      qty,
		FilledQty:     qty,
		TradedPrice:   tradedPrice,
	}
	body, _ := json.Marshal(ev)
	ev.RawMessage = body
	return ev
}

// -- tests -------------------------------------------------------------

func TestBuyFillCreatesManthanRow(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	entryBrokerID := "PAPER-ENTRY-A"
	stub := map[string]tradeexec.OrderMeta{
		entryBrokerID: {
			Found:              true,
			SignalID:           "11111111-1111-1111-1111-111111111111",
			OrderType:          "ENTRY",
			StrategyID:         "22222222-2222-2222-2222-222222222222",
			UserID:             "S4450",
			EntrySignalID:      "11111111-1111-1111-1111-111111111111",
			EntryBrokerOrderID: entryBrokerID,
		},
	}
	h := newHandler(t, db, stub)

	ev := fillEvent("evt-A", entryBrokerID, "S4450", "IDEA", "1", 100, 14.09)
	if err := h.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	var (
		origin   string
		qty      int
		signalID string
	)
	err := db.QueryRow(`SELECT origin, quantity, signal_id::text FROM positions WHERE entry_broker_order_id=$1`, entryBrokerID).
		Scan(&origin, &qty, &signalID)
	if err != nil {
		t.Fatalf("positions select: %v", err)
	}
	if origin != store.OriginManthan {
		t.Errorf("origin: got %q, want MANTHAN", origin)
	}
	if qty != 100 {
		t.Errorf("qty: got %d, want 100", qty)
	}
	if signalID != stub[entryBrokerID].SignalID {
		t.Errorf("signal_id: got %q, want %q", signalID, stub[entryBrokerID].SignalID)
	}
}

func TestBuyFillCreatesUserManualRow(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	h := newHandler(t, db, nil) // empty stub → Found=false
	ev := fillEvent("evt-B", "BROKER-MANUAL-1", "S4450", "IDEA", "1", 10, 15.00)
	if err := h.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	var (
		origin   string
		signalID sql.NullString
	)
	err := db.QueryRow(`SELECT origin, signal_id FROM positions WHERE entry_broker_order_id=$1`, "BROKER-MANUAL-1").
		Scan(&origin, &signalID)
	if err != nil {
		t.Fatalf("positions select: %v", err)
	}
	if origin != store.OriginUserManual {
		t.Errorf("origin: got %q, want USER_MANUAL", origin)
	}
	if signalID.Valid {
		t.Errorf("signal_id must be NULL for USER_MANUAL, got %q", signalID.String)
	}
}

func TestBuyReplayIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	h := newHandler(t, db, nil)
	ev := fillEvent("evt-C", "BROKER-IDEMP", "S4450", "IDEA", "1", 10, 14.00)

	for i := 0; i < 2; i++ {
		if err := h.Handle(context.Background(), ev); err != nil {
			t.Fatalf("Handle iter %d: %v", i, err)
		}
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM positions WHERE entry_broker_order_id=$1`, "BROKER-IDEMP").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 position after replay, got %d", count)
	}
}

// The MONEY test — closes the realized_pnl=0 gap that kicked off this refactor.
func TestManthanSellComputesRealizedPnl(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	entryBrokerID := "PAPER-ENTRY-B"
	slBrokerID := "PAPER-SL-B"
	entrySignal := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	slSignal := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	strategyID := "22222222-2222-2222-2222-222222222222"

	stub := map[string]tradeexec.OrderMeta{
		entryBrokerID: {
			Found: true, SignalID: entrySignal, OrderType: "ENTRY",
			StrategyID: strategyID, UserID: "S4450",
			EntrySignalID: entrySignal, EntryBrokerOrderID: entryBrokerID,
		},
		slBrokerID: {
			Found: true, SignalID: slSignal, OrderType: "SL_SELL",
			StrategyID: strategyID, UserID: "S4450",
			EntrySignalID: entrySignal, EntryBrokerOrderID: entryBrokerID,
		},
	}
	h := newHandler(t, db, stub)

	// 1. BUY 100 IDEA @ ₹14.09
	buy := fillEvent("evt-buy", entryBrokerID, "S4450", "IDEA", "1", 100, 14.09)
	if err := h.Handle(context.Background(), buy); err != nil {
		t.Fatalf("buy: %v", err)
	}

	// 2. SL fires at ₹13.50 — 100 shares closed
	sell := fillEvent("evt-sell", slBrokerID, "S4450", "IDEA", "2", 100, 13.50)
	if err := h.Handle(context.Background(), sell); err != nil {
		t.Fatalf("sell: %v", err)
	}

	var (
		status      string
		exitPrice   sql.NullFloat64
		realizedPnL sql.NullFloat64
		exitReason  sql.NullString
	)
	err := db.QueryRow(`
		SELECT status, exit_price, realized_pnl, exit_reason
		FROM positions
		WHERE entry_broker_order_id=$1`, entryBrokerID).
		Scan(&status, &exitPrice, &realizedPnL, &exitReason)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if status != store.StatusExited {
		t.Errorf("status: got %q, want EXITED", status)
	}
	if !exitPrice.Valid || exitPrice.Float64 != 13.50 {
		t.Errorf("exit_price: got %v, want 13.50", exitPrice)
	}
	if !exitReason.Valid || exitReason.String != store.ExitReasonSLTrigger {
		t.Errorf("exit_reason: got %v, want SL_TRIGGER", exitReason)
	}
	wantPnL := (13.50 - 14.09) * 100.0
	if !realizedPnL.Valid {
		t.Fatalf("realized_pnl is NULL — the whole point of this refactor is broken")
	}
	if diff := realizedPnL.Float64 - wantPnL; diff > 0.01 || diff < -0.01 {
		t.Errorf("realized_pnl: got %v, want %v (delta %v)", realizedPnL.Float64, wantPnL, diff)
	}
}

// Concrete example from §7.2 of the design doc:
//   MANTHAN 20 SBI @ ₹500 + USER_MANUAL 10 SBI @ ₹510
//   User manually sells 15 @ ₹520
//   Expected FIFO: USER_MANUAL fully exits, MANTHAN partial-exits 5 → 15 remaining
func TestManualSellFIFOAcrossOrigins(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	manthanBID := "PAPER-ENTRY-M"
	manualBID := "PAPER-ENTRY-U"

	stub := map[string]tradeexec.OrderMeta{
		manthanBID: {
			Found: true, SignalID: "cccccccc-cccc-cccc-cccc-cccccccccccc",
			OrderType: "ENTRY", StrategyID: "22222222-2222-2222-2222-222222222222",
			UserID: "S4450", EntrySignalID: "cccccccc-cccc-cccc-cccc-cccccccccccc",
			EntryBrokerOrderID: manthanBID,
		},
		// manualBID not in stub → BUY handled as USER_MANUAL
	}
	h := newHandler(t, db, stub)

	buyM := fillEvent("evt-buy-m", manthanBID, "S4450", "SBI", "1", 20, 500.0)
	if err := h.Handle(context.Background(), buyM); err != nil {
		t.Fatalf("manthan buy: %v", err)
	}
	// Space out entry_time so FIFO order is deterministic
	time.Sleep(20 * time.Millisecond)
	buyU := fillEvent("evt-buy-u", manualBID, "S4450", "SBI", "1", 10, 510.0)
	if err := h.Handle(context.Background(), buyU); err != nil {
		t.Fatalf("user manual buy: %v", err)
	}

	// Manual SELL — no stub entry → Found=false → manual FIFO branch
	sell := fillEvent("evt-sell-manual", "BROKER-USER-SELL", "S4450", "SBI", "2", 15, 520.0)
	if err := h.Handle(context.Background(), sell); err != nil {
		t.Fatalf("manual sell: %v", err)
	}

	// USER_MANUAL fully closed: realized_pnl = (520-510)*10 = 100
	var umStatus string
	var umPnL sql.NullFloat64
	err := db.QueryRow(`SELECT status, realized_pnl FROM positions WHERE entry_broker_order_id=$1`, manualBID).
		Scan(&umStatus, &umPnL)
	if err != nil {
		t.Fatalf("select user_manual: %v", err)
	}
	if umStatus != store.StatusExited {
		t.Errorf("USER_MANUAL status: got %q, want EXITED", umStatus)
	}
	if !umPnL.Valid {
		t.Fatal("USER_MANUAL realized_pnl is NULL")
	}
	if diff := umPnL.Float64 - 100.0; diff > 0.01 || diff < -0.01 {
		t.Errorf("USER_MANUAL realized_pnl: got %v, want 100.00", umPnL.Float64)
	}

	// MANTHAN partial: still ACTIVE, qty=15 (20 - 5 spillover)
	var mnStatus string
	var mnQty int
	err = db.QueryRow(`SELECT status, quantity FROM positions WHERE entry_broker_order_id=$1`, manthanBID).
		Scan(&mnStatus, &mnQty)
	if err != nil {
		t.Fatalf("select manthan: %v", err)
	}
	if mnStatus != store.StatusActive {
		t.Errorf("MANTHAN status: got %q, want ACTIVE (partial exit)", mnStatus)
	}
	if mnQty != 15 {
		t.Errorf("MANTHAN qty: got %d, want 15", mnQty)
	}

	// MANTHAN partial-exit audit event: realized_pnl_delta = (520-500)*5 = 100
	var manthanPnLDelta sql.NullFloat64
	err = db.QueryRow(`
		SELECT realized_pnl_delta
		FROM position_events
		WHERE broker_order_id = $1
		  AND signal_id::text = 'cccccccc-cccc-cccc-cccc-cccccccccccc'`,
		"BROKER-USER-SELL").Scan(&manthanPnLDelta)
	if err != nil {
		t.Fatalf("select manthan audit event: %v", err)
	}
	if !manthanPnLDelta.Valid {
		t.Fatal("manthan audit realized_pnl_delta is NULL")
	}
	if diff := manthanPnLDelta.Float64 - 100.0; diff > 0.01 || diff < -0.01 {
		t.Errorf("MANTHAN partial-exit realized_pnl_delta: got %v, want 100.00", manthanPnLDelta.Float64)
	}
}
