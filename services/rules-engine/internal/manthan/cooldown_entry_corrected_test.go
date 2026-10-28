package manthan

// 2026-10-28: positions svc grows a lot opened from a per-trade fill frame to
// the broker's final count and publishes POSITION_ENTRY_CORRECTED. The
// rules-engine must re-confirm the fill with the corrected qty/price exactly
// as it does for POSITION_OPENED, and must not touch the cooldown table.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestEntryCorrected_ReconfirmsFill(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	var gotStrategy, gotSymbol string
	var gotPrice float64
	var gotQty int32
	c := &CooldownConsumer{logger: logger, confirmFill: func(strategyID, symbol string, price float64, qty int32) {
		gotStrategy, gotSymbol, gotPrice, gotQty = strategyID, symbol, price, qty
	}}
	env := positionEventEnvelope{
		EventID: "corr-1", EventType: "POSITION_ENTRY_CORRECTED", ProducedAtMs: time.Now().UnixMilli(),
		PositionID: "d78de55f-da57-4bba-977e-a38b364c3869", Origin: "MANTHAN", UserID: "S4450",
		StrategyID: "a6cb5b08-dddd-4e54-ad2d-d5ae55edb3c9", Symbol: "STK_SHREEPUSHK_EQ_NSE_10588",
		Action: "ENTRY", Price: 509.30, Quantity: 38,
	}
	body, _ := json.Marshal(env)
	if err := c.runOne(context.Background(), body); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if gotStrategy != env.StrategyID || gotSymbol != "SHREEPUSHK" || gotPrice != 509.30 || gotQty != 38 {
		t.Fatalf("confirmFill not re-driven with the corrected fill: %s %s %.2f %d", gotStrategy, gotSymbol, gotPrice, gotQty)
	}
}
