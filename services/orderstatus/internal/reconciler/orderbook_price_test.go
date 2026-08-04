package reconciler

// Unit coverage for eventFromOrderBookRow's TradedPrice sourcing.
//
// Regression guard for the 2026-08-04 "5/8 positions" incident: a fully-executed
// LIMIT/SL fill must carry TradedPrice from the orderbook `price` (the broker
// overwrites the user's limit with the avg traded price on full execution). If
// it stays 0, positions svc defers the BUY forever waiting for a WSS/meta price
// that can be missed mid-settlement — stranding a real fill with no position.
//
// Precision note: this is a FALLBACK. positions svc precedence is
// meta.AvgFillPrice (exact VWAP) > ev.TradedPrice, so the exact price still wins
// when available; the fallback only stops a fill from getting stuck at price=0.

import (
	"testing"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
	"go.uber.org/zap"
)

func TestEventFromOrderBookRow_TradedPriceSourcing(t *testing.T) {
	r := &Reconciler{logger: zap.NewNop()} // cutoff zero-value ⇒ no boot filter

	mk := func(ordType, status string, price float64, qty, traded, remain int) *indira.OrderBook {
		return &indira.OrderBook{
			OrdId:     "OID1",
			Status:    status,
			OrdType:   ordType,
			OrdAction: "BUY",
			PrdType:   "DELIVERY",
			Price:     price,
			Qty:       qty,
			TradedQty: traded,
			RemainQty: remain,
			Symbol:    indira.OrderBookSymbol{Symbol: "STK_X_EQ_NSE_1", Exc: "NSE"},
		}
	}

	cases := []struct {
		name   string
		o      *indira.OrderBook
		wantTP float64
	}{
		// THE FIX: fully-executed LIMIT/SL fills now carry the fill price.
		{"fully-executed LIMIT → price is the fill", mk("Limit", "Executed", 429.35, 13, 13, 0), 429.35},
		{"fully-executed SL → price is the fill", mk("SL", "Executed", 61.5, 77, 77, 0), 61.5},
		// Existing behaviour preserved.
		{"market fill → price", mk("Market", "Executed", 188.59, 5, 5, 0), 188.59},
		// Must still stay 0 — these would poison the entry price.
		{"resting LIMIT (pending) → 0", mk("Limit", "Pending", 428.75, 13, 0, 13), 0},
		{"partial LIMIT fill → 0 (defer to tradebook)", mk("Limit", "Executed", 429.0, 13, 5, 8), 0},
		{"executed but price 0 → 0", mk("Limit", "Executed", 0, 13, 13, 0), 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := r.eventFromOrderBookRow("S4450", tc.o, store.SourceRESTOrderbook)
			if ev == nil {
				t.Fatal("event is nil")
			}
			if ev.TradedPrice != tc.wantTP {
				t.Errorf("TradedPrice = %v, want %v", ev.TradedPrice, tc.wantTP)
			}
		})
	}
}
