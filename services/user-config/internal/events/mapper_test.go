package events

// Unit coverage for the pure event-mapping logic. No DB, no Kafka — these
// exercise the model→ConfigEvent transforms the publisher relies on:
//   - thin vs full vs lifecycle vs manthan event shapes
//   - nil-pointer safety in the *T -> value helpers and in toPayload when
//     TradeConfig / Conditions are absent
//   - manthan payload carries ONLY the slim field set the rules-engine needs

import (
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/google/uuid"
)

func f64(v float64) *float64 { return &v }
func i32(v int32) *int32     { return &v }

func sampleStrategy() *models.Strategy {
	return &models.Strategy{
		StrategyID:   uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		UserID:       "S4450",
		StrategyName: "MyManthan",
		StrategyType: models.StrategyTypeManthan,
		TradingMode:  models.TradingModeLive,
		Active:       true,
		Version:      7,
		CreatedAt:    time.Unix(1000, 0),
		UpdatedAt:    time.Unix(2000, 0),
		TradeConfig: &models.TradeConfig{
			OrderType:      "LIMIT",
			ProductType:    "CNC",
			Validity:       "DAY",
			Quantity:       10,
			Exchange:       "NSE",
			OrderSide:      "BUY",
			StopLossType:   "SL-M",
			StopLossPct:    f64(20),
			TrailingSLPct:  f64(20),
			TotalCapital:   f64(100000),
			MaxPositions:   i32(25),
			PerStockAmount: f64(4000),
		},
	}
}

func TestValueOrZeroHelpers(t *testing.T) {
	if got := valueOrZeroFloat64(nil); got != 0 {
		t.Errorf("valueOrZeroFloat64(nil) = %v, want 0", got)
	}
	if got := valueOrZeroFloat64(f64(12.5)); got != 12.5 {
		t.Errorf("valueOrZeroFloat64(&12.5) = %v, want 12.5", got)
	}
	if got := valueOrZeroInt32(nil); got != 0 {
		t.Errorf("valueOrZeroInt32(nil) = %v, want 0", got)
	}
	if got := valueOrZeroInt32(i32(42)); got != 42 {
		t.Errorf("valueOrZeroInt32(&42) = %v, want 42", got)
	}
}

func TestToThinConfigEvent_NoPayload(t *testing.T) {
	ev := ToThinConfigEvent(ConfigResumed, "S4450", "sid-1", 3)
	if ev.Config != nil {
		t.Error("thin event must have nil Config")
	}
	if ev.Type != ConfigResumed || ev.UserID != "S4450" || ev.StrategyID != "sid-1" || ev.Version != 3 {
		t.Errorf("thin event identity wrong: %+v", ev)
	}
	if ev.Timestamp <= 0 {
		t.Error("Timestamp must be stamped")
	}
	if ev.PositionHandling != "" {
		t.Error("thin event carries no position handling")
	}
}

func TestToLifecycleConfigEvent_CarriesPositionHandling(t *testing.T) {
	ev := ToLifecycleConfigEvent(ConfigDeleted, "S4450", "sid-1", 4, "SQUARE_OFF_AT_MARKET")
	if ev.Config != nil {
		t.Error("lifecycle event stays thin (nil Config)")
	}
	if ev.PositionHandling != "SQUARE_OFF_AT_MARKET" {
		t.Errorf("PositionHandling = %q, want SQUARE_OFF_AT_MARKET", ev.PositionHandling)
	}
	// Empty string preserves legacy behaviour.
	ev2 := ToLifecycleConfigEvent(ConfigPaused, "S4450", "sid-1", 5, "")
	if ev2.PositionHandling != "" {
		t.Errorf("empty PositionHandling must pass through, got %q", ev2.PositionHandling)
	}
}

func TestToFullConfigEvent_PopulatesPayload(t *testing.T) {
	s := sampleStrategy()
	ev := ToFullConfigEvent(ConfigCreated, s)
	if ev.Config == nil {
		t.Fatal("full event must carry a payload")
	}
	if ev.StrategyID != s.StrategyID.String() || ev.Version != 7 {
		t.Errorf("event identity wrong: %+v", ev)
	}
	p := ev.Config
	if p.UserID != "S4450" || p.StrategyName != "MyManthan" {
		t.Errorf("payload identity wrong: %+v", p)
	}
	if p.TradeConfig.OrderType != "LIMIT" || p.TradeConfig.ProductType != "CNC" {
		t.Errorf("trade config not mapped: %+v", p.TradeConfig)
	}
	if p.TradeConfig.StopLossPct != 20 || p.TradeConfig.TrailingSLPct != 20 {
		t.Errorf("SL pcts not mapped: %+v", p.TradeConfig)
	}
	if p.CreatedAt != s.CreatedAt.UnixNano() || p.UpdatedAt != s.UpdatedAt.UnixNano() {
		t.Error("timestamps not carried on full payload")
	}
}

func TestToFullConfigEvent_NilTradeConfigNoPanic(t *testing.T) {
	s := sampleStrategy()
	s.TradeConfig = nil
	s.Conditions = nil
	ev := ToFullConfigEvent(ConfigUpdated, s) // must not panic
	if ev.Config == nil {
		t.Fatal("payload should still be built")
	}
	// Absent TradeConfig → zero-value payload, not a crash.
	if ev.Config.TradeConfig.OrderType != "" || ev.Config.TradeConfig.StopLossPct != 0 {
		t.Errorf("nil TradeConfig should yield zero payload, got %+v", ev.Config.TradeConfig)
	}
}

func TestToManthanConfigEvent_SlimPayload(t *testing.T) {
	s := sampleStrategy()
	ev := ToManthanConfigEvent(ConfigUpdated, s)
	if ev.Config == nil {
		t.Fatal("manthan event must carry a payload")
	}
	p := ev.Config
	if p.StrategyType != string(models.StrategyTypeManthan) {
		t.Errorf("StrategyType = %q, want MANTHAN", p.StrategyType)
	}
	// Slim manthan payload maps capital + SL config...
	if p.TradeConfig.TotalCapital != 100000 || p.TradeConfig.MaxPositions != 25 || p.TradeConfig.PerStockAmount != 4000 {
		t.Errorf("capital fields not mapped: %+v", p.TradeConfig)
	}
	if p.TradeConfig.StopLossPct != 20 || p.TradeConfig.TrailingSLPct != 20 || p.TradeConfig.StopLossType != "SL-M" || p.TradeConfig.ProductType != "CNC" {
		t.Errorf("SL/product fields not mapped: %+v", p.TradeConfig)
	}
	// ...and deliberately omits order-level fields the backend controls.
	if p.TradeConfig.OrderType != "" || p.TradeConfig.Validity != "" || p.TradeConfig.Quantity != 0 {
		t.Errorf("manthan payload must NOT carry order-level fields, got %+v", p.TradeConfig)
	}
}

func TestToManthanConfigEvent_NilPointersZeroed(t *testing.T) {
	s := sampleStrategy()
	s.TradeConfig.TotalCapital = nil
	s.TradeConfig.MaxPositions = nil
	s.TradeConfig.StopLossPct = nil
	ev := ToManthanConfigEvent(ConfigCreated, s)
	p := ev.Config.TradeConfig
	if p.TotalCapital != 0 || p.MaxPositions != 0 || p.StopLossPct != 0 {
		t.Errorf("nil pointers must map to 0, got %+v", p)
	}
}
