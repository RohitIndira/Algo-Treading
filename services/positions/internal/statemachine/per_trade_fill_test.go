package statemachine

// Regression for 2026-10-28: the broker's WebSocket emits one EXECUTED frame
// per TRADE (filled_qty = that trade's shares), so S4450 SHREEPUSHK (order 38)
// arrived as 1 + 1 + 36 and the row opened from the first frame stayed at
// qty 1 while the "position exists → skip" guard dropped the rest. The final
// FILLED event (filled_qty == order qty) must grow the row to the broker's
// final count at the ledger's average fill price; intermediate per-trade
// frames and replays must change nothing; the row must never shrink.

import (
	"context"
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
)

func TestBuyPerTradeFramesGrowToFinalFill(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	const broker = "NYMZX00011L:"
	const signal = "aa44cd99-ef06-57af-b052-41d94cd97df8"
	const strategy = "a6cb5b08-dddd-4e54-ad2d-d5ae55edb3c9"
	meta := tradeexec.OrderMeta{Found: true, SignalID: signal, OrderType: "ENTRY", StrategyID: strategy, UserID: "S4450",
		EntrySignalID: signal, EntryBrokerOrderID: broker}
	h := newHandler(t, db, map[string]tradeexec.OrderMeta{broker: meta})
	ctx := context.Background()

	readRow := func() (qty int, price, invested float64) {
		if err := db.QueryRow(`SELECT quantity, entry_price, invested_amount FROM positions WHERE entry_broker_order_id=$1 AND status='ACTIVE'`, broker).
			Scan(&qty, &price, &invested); err != nil {
			t.Fatalf("read row: %v", err)
		}
		return
	}

	// Frame 1: first trade, 1 share @ 509.10 (ledger has no avg yet).
	f1 := fillEvent("seq-185882", broker, "S4450", "STK_SHREEPUSHK_EQ_NSE_10588", "1", 38, 509.10)
	f1.FilledQty = 1
	if err := h.Handle(ctx, f1); err != nil {
		t.Fatalf("frame 1: %v", err)
	}
	if q, p, _ := readRow(); q != 1 || p != 509.10 {
		t.Fatalf("after frame 1 want 1 @ 509.10, got %d @ %.2f", q, p)
	}

	// Frame 2: second trade, 36 shares — a per-trade count, NOT a total. Must not move the row.
	f2 := fillEvent("seq-185886", broker, "S4450", "STK_SHREEPUSHK_EQ_NSE_10588", "1", 38, 509.30)
	f2.FilledQty = 36
	if err := h.Handle(ctx, f2); err != nil {
		t.Fatalf("frame 2: %v", err)
	}
	if q, _, _ := readRow(); q != 1 {
		t.Fatalf("a per-trade frame must not resize the row: got %d", q)
	}

	// Final: filled_qty == order qty; the ledger now carries the trade-book average.
	meta.AvgFillPrice = 509.30
	h = newHandler(t, db, map[string]tradeexec.OrderMeta{broker: meta})
	f3 := fillEvent("rest-57550146038", broker, "S4450", "STK_SHREEPUSHK_EQ_NSE_10588", "1", 38, 509.30)
	f3.Source = "REST_ORDERBOOK"
	if err := h.Handle(ctx, f3); err != nil {
		t.Fatalf("final: %v", err)
	}
	q, p, inv := readRow()
	if q != 38 || p != 509.30 || inv < 19353.39 || inv > 19353.41 {
		t.Fatalf("after final want 38 @ 509.30 invested 19353.40, got %d @ %.2f invested %.2f", q, p, inv)
	}
	var corrections int
	if err := db.QueryRow(`SELECT COUNT(*) FROM position_events WHERE broker_order_id=$1 AND event_type='ENTRY_QTY_CORRECTED' AND delta_qty=37`, broker).Scan(&corrections); err != nil || corrections != 1 {
		t.Fatalf("want exactly one ENTRY_QTY_CORRECTED(+37) audit row, got %d err=%v", corrections, err)
	}

	// Replay of the final event, and a stale smaller "final": no change, never shrink.
	if err := h.Handle(ctx, f3); err != nil {
		t.Fatalf("replay: %v", err)
	}
	stale := fillEvent("stale-1", broker, "S4450", "STK_SHREEPUSHK_EQ_NSE_10588", "1", 20, 500.00)
	if err := h.Handle(ctx, stale); err != nil {
		t.Fatalf("stale: %v", err)
	}
	if q, p, _ := readRow(); q != 38 || p != 509.30 {
		t.Fatalf("replay/stale must not change the row: got %d @ %.2f", q, p)
	}
	var count int
	_ = db.QueryRow(`SELECT COUNT(*) FROM positions WHERE entry_broker_order_id=$1`, broker).Scan(&count)
	if count != 1 {
		t.Fatalf("still exactly one position row expected, got %d", count)
	}
}
