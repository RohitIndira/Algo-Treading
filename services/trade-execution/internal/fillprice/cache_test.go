package fillprice

import (
	"context"
	"errors"
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

func TestAggregateByOrderVWAP(t *testing.T) {
	trades := []indiraClient.TradeBook{
		{OrdId: "A", TradedQty: 10, TradedPrice: 100, TradeTime: "2026-06-02 10:00:00"},
		{OrdId: "A", TradedQty: 30, TradedPrice: 104, TradeTime: "2026-06-02 10:00:01"},
		{OrdId: "B", TradedQty: 5, TradedPrice: 50},
		{OrdId: "", TradedQty: 99, TradedPrice: 1}, // skipped: no order id
		{OrdId: "C", TradedQty: 0, TradedPrice: 7}, // skipped: zero qty
	}
	got := aggregateByOrder(trades)

	// A: VWAP = (10*100 + 30*104) / 40 = 103
	if a, ok := got["A"]; !ok || a.Qty != 40 || a.Price != 103 {
		t.Errorf("A: got %+v, want qty=40 price=103", a)
	}
	if b, ok := got["B"]; !ok || b.Qty != 5 || b.Price != 50 {
		t.Errorf("B: got %+v, want qty=5 price=50", b)
	}
	if _, ok := got["C"]; ok {
		t.Errorf("C should be skipped (zero qty)")
	}
	if _, ok := got[""]; ok {
		t.Errorf("empty order id should be skipped")
	}
}

type stubFetcher struct {
	calls  int
	trades []indiraClient.TradeBook
	err    error
}

func (s *stubFetcher) GetTradeBook(ctx context.Context, auth *indiraClient.AuthContext, orderIds ...string) ([]indiraClient.TradeBook, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.trades, nil
}

func TestCacheBatchesAndCaches(t *testing.T) {
	f := &stubFetcher{trades: []indiraClient.TradeBook{
		{OrdId: "X1", TradedQty: 1, TradedPrice: 90.34},
		{OrdId: "X2", TradedQty: 2, TradedPrice: 200},
	}}
	c := NewCache(f, time.Second)
	auth := &indiraClient.AuthContext{}

	// Two lookups for the same user within TTL → one broker call (batched).
	if fill, ok, err := c.FillForOrder(context.Background(), auth, "u1", "X1"); err != nil || !ok || fill.Price != 90.34 {
		t.Fatalf("X1: ok=%v err=%v fill=%+v", ok, err, fill)
	}
	if fill, ok, err := c.FillForOrder(context.Background(), auth, "u1", "X2"); err != nil || !ok || fill.Price != 200 {
		t.Fatalf("X2: ok=%v err=%v fill=%+v", ok, err, fill)
	}
	if f.calls != 1 {
		t.Errorf("expected 1 batched broker call, got %d", f.calls)
	}

	// Unknown order → ok=false (not yet booked).
	if _, ok, _ := c.FillForOrder(context.Background(), auth, "u1", "ZZZ"); ok {
		t.Errorf("unknown order should return ok=false")
	}
}

func TestCacheErrorPropagates(t *testing.T) {
	f := &stubFetcher{err: errors.New("broker down")}
	c := NewCache(f, time.Second)
	if _, ok, err := c.FillForOrder(context.Background(), &indiraClient.AuthContext{}, "u1", "X1"); ok || err == nil {
		t.Errorf("expected error propagation, got ok=%v err=%v", ok, err)
	}
}
