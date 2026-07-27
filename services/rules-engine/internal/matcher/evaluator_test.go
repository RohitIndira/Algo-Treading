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
	s.Conditions.MinPctChange = 1
	s.Conditions.MaxPctChange = 5

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
	s.Conditions.Exchanges = []string{"NSE"}

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

// Regression: the news feed labels caps as "Small Cap"/"Mid Cap"/"Large Cap"
// while strategy conditions store "SMALL"/"MID"/"LARGE". A plain EqualFold never
// matched, so any strategy with a market_cap_types filter silently placed zero
// orders. The evaluator must canonicalize both forms before comparing.
func TestMatcher_MarketCap_SuffixedFeedForm_Matches(t *testing.T) {
	cases := []struct {
		eventMCap string
		filter    []string
	}{
		{"Small Cap", []string{"SMALL"}},
		{"Mid Cap", []string{"MID", "SMALL"}},
		{"Large Cap", []string{"LARGE"}},
		{"small cap", []string{"Small"}}, // case-insensitive on both sides
	}
	for _, tc := range cases {
		e := baseEvent()
		e.StockData.MCapType = tc.eventMCap
		s := baseStrategy()
		s.Conditions.MarketCapTypes = tc.filter

		res := NewEvaluator(zap.NewNop()).Evaluate(e, s)
		if !res.IsFullMatch() {
			t.Fatalf("event mcap %q vs filter %v: expected full match, failed: %v",
				tc.eventMCap, tc.filter, res.FailedConditions)
		}
	}
}

func TestMatcher_MarketCap_WrongBucket_Drops(t *testing.T) {
	e := baseEvent()
	e.StockData.MCapType = "Large Cap"
	s := baseStrategy()
	s.Conditions.MarketCapTypes = []string{"MID", "SMALL"}

	res := NewEvaluator(zap.NewNop()).Evaluate(e, s)
	if res.IsFullMatch() {
		t.Fatalf("expected drop: Large Cap event should not match {MID,SMALL} filter")
	}
}

// NOTE: Tests for per-stock-code and volume-threshold filtering were removed:
// models.Conditions has no Stocks/VolumeThreshold field and Evaluator.Evaluate
// performs no such check (it evaluates impact, sentiment, category, market cap,
// price range, pct-change, and exchange). The strategy_conditions table does have
// stock_codes/min_volume columns, but that filtering is not implemented in the
// matcher — re-add these tests alongside the feature if it is built.
