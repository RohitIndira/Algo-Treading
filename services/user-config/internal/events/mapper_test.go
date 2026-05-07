package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestToFullConfigEvent_AllFieldsMapped(t *testing.T) {
	now := time.Now()
	strategyID := uuid.New()

	minMcap := 1000.0
	maxMcap := 50000.0
	minPct := 1.5
	maxPct := 5.0
	minVol := int64(100000)

	limit := 123.45
	sl := 2.0
	tp := 5.0
	trail := 1.1

	maxDailyTrades := int32(5)
	maxPerTradeRisk := 500.0
	maxExposure := 10.0
	maxLoss := 2000.0

	s := &models.Strategy{
		StrategyID:   strategyID,
		UserID:       "user-123",
		StrategyName: "My First Strategy",
		Description:  "desc",
		Active:       true,
		TradingMode:  models.TradingModeLive,
		Version:      7,
		CreatedAt:    now.Add(-1 * time.Hour),
		UpdatedAt:    now,
		Conditions: &models.StrategyCondition{
			MatchAllNews:      false,
			ImpactScoreMin:    5,
			ImpactScoreMax:    9,
			Sentiments:        pq.StringArray{"POSITIVE"},
			Categories:        pq.StringArray{"EARNINGS", "MERGERS"},
			StockCodes:        pq.Int64Array{500325, 532540},
			MarketCapTypes:    pq.StringArray{"LARGE", "MID"},
			MinMarketCap:      &minMcap,
			MaxMarketCap:      &maxMcap,
			MinPriceChangePct: &minPct,
			MaxPriceChangePct: &maxPct,
			MinVolume:         &minVol,
			Exchanges:         pq.StringArray{"NSE", "BSE"},
			CreatedAt:         now.Add(-2 * time.Hour),
		},
		TradeConfig: &models.TradeConfig{
			OrderType:     "MARKET",
			ProductType:   "INTRADAY",
			Validity:      "DAY",
			Quantity:      10,
			Exchange:      "NSE",
			OrderSide:     "BUY",
			LimitPrice:    &limit,
			StopLossPct:   &sl,
			TakeProfitPct: &tp,
			TrailingSLPct: &trail,
			StopLossType:  "FIXED",
			CreatedAt:     now.Add(-3 * time.Hour),
		},
		RiskLimits: &models.RiskLimits{
			MaxDailyTrades:          &maxDailyTrades,
			MaxPerTradeRisk:         &maxPerTradeRisk,
			MaxPortfolioExposurePct: &maxExposure,
			MaxLossPerDay:           &maxLoss,
			EnableRiskChecks:        true,
			EnableAutoSquareOff:     true,
			AutoSquareOffTime:       "15:15",
			CreatedAt:               now.Add(-4 * time.Hour),
		},
	}

	e := ToFullConfigEvent(ConfigCreated, s)
	if e.Type != ConfigCreated {
		t.Fatalf("expected type %s got %s", ConfigCreated, e.Type)
	}
	if e.UserID != s.UserID {
		t.Fatalf("expected user_id %s got %s", s.UserID, e.UserID)
	}
	if e.StrategyID != s.StrategyID.String() {
		t.Fatalf("expected strategy_id %s got %s", s.StrategyID.String(), e.StrategyID)
	}
	if e.Version != uint64(s.Version) {
		t.Fatalf("expected version %d got %d", s.Version, e.Version)
	}
	if e.Timestamp <= 0 {
		t.Fatalf("expected timestamp > 0")
	}
	if e.Config == nil {
		t.Fatalf("expected config not nil")
	}

	// Strategy payload
	if e.Config.StrategyName != s.StrategyName {
		t.Fatalf("strategy_name mismatch")
	}
	if e.Config.TradingMode != string(s.TradingMode) {
		t.Fatalf("trading_mode mismatch")
	}

	// Conditions (strings as-is)
	if e.Config.Conditions.ImpactScoreMin != 5 || e.Config.Conditions.ImpactScoreMax != 9 {
		t.Fatalf("impact score mismatch")
	}
	if len(e.Config.Conditions.Sentiments) != 1 || e.Config.Conditions.Sentiments[0] != "POSITIVE" {
		t.Fatalf("sentiments mismatch: %+v", e.Config.Conditions.Sentiments)
	}
	if len(e.Config.Conditions.Exchanges) != 2 {
		t.Fatalf("exchanges mismatch: %+v", e.Config.Conditions.Exchanges)
	}
	if e.Config.Conditions.MinMarketCap != minMcap || e.Config.Conditions.MaxMarketCap != maxMcap {
		t.Fatalf("mcap mismatch")
	}
	if e.Config.Conditions.MinPriceChangePct != minPct || e.Config.Conditions.MaxPriceChangePct != maxPct {
		t.Fatalf("pct mismatch")
	}
	if e.Config.Conditions.MinVolume != minVol {
		t.Fatalf("volume mismatch")
	}

	// TradeConfig (strings as-is)
	if e.Config.TradeConfig.OrderType != "MARKET" {
		t.Fatalf("order_type mismatch")
	}
	if e.Config.TradeConfig.Exchange != "NSE" {
		t.Fatalf("exchange mismatch")
	}
	if e.Config.TradeConfig.LimitPrice != limit {
		t.Fatalf("limit_price mismatch")
	}
	if e.Config.TradeConfig.StopLossPct != sl || e.Config.TradeConfig.TakeProfitPct != tp {
		t.Fatalf("sl/tp mismatch")
	}

	// RiskLimits
	if e.Config.RiskLimits.MaxDailyTrades != maxDailyTrades {
		t.Fatalf("max_daily_trades mismatch")
	}
	if e.Config.RiskLimits.EnableAutoSquareOff != true {
		t.Fatalf("enable_auto_square_off mismatch")
	}
}

func TestToFullConfigEvent_NilPointers_DoNotPanicAndHaveEmptySlices(t *testing.T) {
	s := &models.Strategy{StrategyID: uuid.New(), UserID: "u", StrategyName: "n", TradingMode: models.TradingModePaper, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	s.Conditions = nil
	s.TradeConfig = nil
	s.RiskLimits = nil

	e := ToFullConfigEvent(ConfigCreated, s)
	if e.Config == nil {
		t.Fatalf("expected config")
	}
	// Conditions should exist and slices should be non-nil for JSON stability
	if e.Config.Conditions.Sentiments == nil || e.Config.Conditions.Categories == nil || e.Config.Conditions.StockCodes == nil || e.Config.Conditions.Exchanges == nil {
		t.Fatalf("expected non-nil slice fields")
	}
}

func TestToFullConfigEvent_NilSlicesBecomeEmptyArraysInJSON(t *testing.T) {
	s := &models.Strategy{StrategyID: uuid.New(), UserID: "u", StrategyName: "n", TradingMode: models.TradingModePaper, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	s.Conditions = &models.StrategyCondition{
		Sentiments:     nil,
		Categories:     nil,
		StockCodes:     nil,
		MarketCapTypes: nil,
		Exchanges:      nil,
	}

	e := ToFullConfigEvent(ConfigCreated, s)
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	js := string(b)
	if strings.Contains(js, `"sentiments":null`) || strings.Contains(js, `"categories":null`) || strings.Contains(js, `"stock_codes":null`) || strings.Contains(js, `"market_cap_types":null`) || strings.Contains(js, `"exchanges":null`) {
		t.Fatalf("expected no null arrays, got: %s", js)
	}
}

func TestToThinConfigEvent_DeletePausedResumed(t *testing.T) {
	for _, tt := range []ConfigEventType{ConfigDeleted, ConfigPaused, ConfigResumed} {
		e := ToThinConfigEvent(tt, "user-1", "strat-1", 9)
		if e.Type != tt {
			t.Fatalf("type mismatch")
		}
		if e.Config != nil {
			t.Fatalf("expected config nil for %s", tt)
		}
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"config":null`) {
			t.Fatalf("expected config:null in json for %s", tt)
		}
	}
}

func TestEventTypeConstants(t *testing.T) {
	if ConfigCreated != "CONFIG_CREATED" {
		t.Fatal("ConfigCreated constant mismatch")
	}
	if ConfigUpdated != "CONFIG_UPDATED" {
		t.Fatal("ConfigUpdated constant mismatch")
	}
	if ConfigDeleted != "CONFIG_DELETED" {
		t.Fatal("ConfigDeleted constant mismatch")
	}
	if ConfigPaused != "CONFIG_PAUSED" {
		t.Fatal("ConfigPaused constant mismatch")
	}
	if ConfigResumed != "CONFIG_RESUMED" {
		t.Fatal("ConfigResumed constant mismatch")
	}
}

func TestTimestampIsUnixNano(t *testing.T) {
	s := &models.Strategy{StrategyID: uuid.New(), UserID: "u", StrategyName: "n", TradingMode: models.TradingModePaper, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	e := ToFullConfigEvent(ConfigCreated, s)
	now := time.Now().UnixNano()
	if e.Timestamp <= 0 {
		t.Fatalf("expected timestamp > 0")
	}
	if e.Timestamp > now {
		t.Fatalf("expected timestamp <= now")
	}
	if e.Timestamp < time.Now().Add(-1*time.Second).UnixNano() {
		t.Fatalf("timestamp not in unix nano range")
	}
}
