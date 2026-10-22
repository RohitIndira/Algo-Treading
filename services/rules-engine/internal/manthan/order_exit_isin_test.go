package manthan

import (
	"encoding/json"
	"testing"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// 2026-10-22: the SL_EXIT payload carried no ISIN, trade-execution could not
// resolve the exchange token, and every software-stop exit since late
// September failed. The ISIN must ride on the wire exactly as the position
// holds it.
func TestGenerateSLExit_CarriesISIN(t *testing.T) {
	g := NewOrderGenerator(zap.NewNop())
	strategy := types.UserStrategy{StrategyID: "strat-1", UserID: "S4450", TradingMode: "LIVE"}
	pos := &types.Position{
		Symbol:     "KILITCH",
		ISIN:       "INE729D01010",
		Quantity:   79,
		EntryPrice: 249.90,
		CurrentSL:  212.40,
	}

	order := g.GenerateSLExit(strategy, pos, 205.10, (205.10-249.90)*79)

	if order.ISIN != "INE729D01010" {
		t.Fatalf("ISIN not carried: got %q", order.ISIN)
	}
	if order.Symbol != "KILITCH" || order.Quantity != 79 || order.OrderSide != "SELL" {
		t.Fatalf("unexpected order identity: %+v", order)
	}
	if order.SLPrice != 212.40 {
		t.Fatalf("SLPrice must be the position's current stop: got %v", order.SLPrice)
	}

	body, err := json.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if got, _ := wire["isin"].(string); got != "INE729D01010" {
		t.Fatalf("wire payload lacks isin: %s", body)
	}
}

// An empty ISIN on the position must still produce an exit (trade-execution
// falls back to the filled entry row) — the generator never blocks an exit.
func TestGenerateSLExit_EmptyISINStillExits(t *testing.T) {
	g := NewOrderGenerator(zap.NewNop())
	strategy := types.UserStrategy{StrategyID: "strat-1", UserID: "U", TradingMode: "LIVE"}
	pos := &types.Position{Symbol: "X", Quantity: 1, CurrentSL: 10}
	order := g.GenerateSLExit(strategy, pos, 9, -1)
	if order.Symbol != "X" || order.Quantity != 1 {
		t.Fatalf("exit not generated: %+v", order)
	}
}
