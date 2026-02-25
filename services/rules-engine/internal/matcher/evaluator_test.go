package matcher

import (
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

func baseEvent() *models.MarketEvent {
	e := &models.MarketEvent{EventID: "e1", EventType: "news", Timestamp: time.Now()}
	e.StockData.StockCode = 101
	e.StockData.Exchange = "NSE"
	e.NewsData.Category = "policy"
	e.Analysis.Sentiment = "positive" // maps to positive in GetSentimentValue()
	e.Analysis.ImpactScore = 5
	e.MarketData.PctChange = 2.0
	e.MarketData.LastTradedPrice = 100
	e.MarketData.PriceMap.Volume = 10_000
	return e
}

func baseStrategy() *models.Strategy {
	return &models.Strategy{
		UserID:      "u1",
		StrategyID:  "s1",
		Active:      true,
		Version:     1,
		Conditions:  models.Conditions{ImpactScoreMin: 1, ImpactScoreMax: 10},
		TradeConfig: models.TradeConfig{Exchange: "NSE"},
		RiskLimits:  models.RiskLimits{MaxDailyTrades: 1},
	}
}

func TestMatcher_AllFiltersPass_EmitsSignal(t *testing.T) {
	e := baseEvent()
	s := baseStrategy()
	s.Conditions.Categories = []string{"policy"}
	s.Conditions.Sentiments = []string{"positive"}
	s.Conditions.Stocks = []int64{101}
	s.Conditions.MinPctChange = 1
	s.Conditions.MaxPctChange = 5
	s.Conditions.VolumeThreshold = 100

	ev := NewEvaluator(zap.NewNop())
	res := ev.Evaluate(e, s)
	if !res.IsFullMatch() {
		t.Fatalf("expected full match, failed: %v", res.FailedConditions)
	}
}

func TestMatcher_ImpactScoreBelow_Drops(t *testing.T) {
	e := baseEvent()
	e.Analysis.ImpactScore = 0
	s := baseStrategy()
	s.Conditions.ImpactScoreMin = 1
	s.Conditions.ImpactScoreMax = 10

	ev := NewEvaluator(zap.NewNop())
	res := ev.Evaluate(e, s)
	if res.IsFullMatch() {
		t.Fatalf("expected not full match")
	}
}

func TestMatcher_WrongCategory_Drops(t *testing.T) {
	e := baseEvent()
	e.NewsData.Category = "earnings"
	s := baseStrategy()
	s.Conditions.Categories = []string{"policy"}

	ev := NewEvaluator(zap.NewNop())
	res := ev.Evaluate(e, s)
	if res.IsFullMatch() {
		t.Fatalf("expected not full match")
	}
}

func TestMatcher_WrongExchange_Drops(t *testing.T) {
	e := baseEvent()
	e.StockData.Exchange = "BSE"
	s := baseStrategy()
	s.TradeConfig.Exchange = "NSE"

	ev := NewEvaluator(zap.NewNop())
	res := ev.Evaluate(e, s)
	if res.IsFullMatch() {
		t.Fatalf("expected not full match")
	}
}

func TestMatcher_WrongSentiment_Drops(t *testing.T) {
	e := baseEvent()
	e.Analysis.Sentiment = "negative"
	s := baseStrategy()
	s.Conditions.Sentiments = []string{"positive"}

	ev := NewEvaluator(zap.NewNop())
	res := ev.Evaluate(e, s)
	if res.IsFullMatch() {
		t.Fatalf("expected not full match")
	}
}

func TestMatcher_StockCodeNotInSet_Drops(t *testing.T) {
	e := baseEvent()
	e.StockData.StockCode = 999
	s := baseStrategy()
	s.Conditions.Stocks = []int64{101}

	ev := NewEvaluator(zap.NewNop())
	res := ev.Evaluate(e, s)
	if res.IsFullMatch() {
		t.Fatalf("expected not full match")
	}
}

func TestMatcher_EmptyStockCodes_MatchesAll(t *testing.T) {
	e := baseEvent()
	s := baseStrategy()
	s.Conditions.Stocks = nil

	ev := NewEvaluator(zap.NewNop())
	res := ev.Evaluate(e, s)
	// should not fail on stock
	for _, f := range res.FailedConditions {
		if f == "stock" {
			t.Fatalf("expected stock to match when empty")
		}
	}
}
