package exitprice

import (
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
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
