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

// hasCondition reports whether a condition name appears in a result slice.
func hasCondition(list []string, name string) bool {
	for _, c := range list {
		if c == name {
			return true
		}
	}
	return false
}

// market_cap_range is a STRICT bounded range in ₹ crore sourced from
// CompanyMaster.mcap. Both bounds are inclusive and 0/0 means "filter unset".
// There is no open-ended form: the gateway rejects a min with no max, so the
// evaluator fails an out-of-order range closed rather than reinterpreting it.
func TestMatcher_MarketCapRange(t *testing.T) {
	cases := []struct {
		name      string
		mcap      float64
		min, max  float64
		wantMatch bool
	}{
		{"filter unset", 5000, 0, 0, true},
		{"in range", 5000, 1000, 10000, true},
		{"below min", 500, 1000, 10000, false},
		{"above max (Ashok Leyland)", 88783.31, 1000, 10000, false},
		{"inclusive lower boundary", 1000, 1000, 10000, true},
		{"inclusive upper boundary", 10000, 1000, 10000, true},
		{"zero min is a real lower bound", 5000, 0, 10000, true},
		{"exact-cap range", 5000, 5000, 5000, true},
		{"missing mcap with active filter", 0, 1000, 10000, false},
		{"missing mcap, filter unset", 0, 0, 0, true},
		{"corrupt config max<min fails closed", 5000, 10000, 1000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := baseEvent()
			e.StockData.MCap = tc.mcap
			s := baseStrategy()
			s.Conditions.MarketCapRange = models.MarketCapRange{MinMcap: tc.min, MaxMcap: tc.max}

			res := NewEvaluator(zap.NewNop()).Evaluate(e, s)

			if got := hasCondition(res.MatchedConditions, "market_cap_range"); got != tc.wantMatch {
				t.Fatalf("mcap=%v range=[%v,%v]: market_cap_range matched=%v, want %v (failed: %v)",
					tc.mcap, tc.min, tc.max, got, tc.wantMatch, res.FailedConditions)
			}
			// No other condition is narrowed in baseStrategy, so the range alone
			// decides the overall match.
			if res.IsFullMatch() != tc.wantMatch {
				t.Fatalf("mcap=%v range=[%v,%v]: IsFullMatch=%v, want %v",
					tc.mcap, tc.min, tc.max, res.IsFullMatch(), tc.wantMatch)
			}
		})
	}
}

// market_cap (SMALL/MID/LARGE bucket) and market_cap_range (numeric ₹ Cr) are
// independent conditions that AND — a stock must satisfy both. Ashok Leyland is
// a genuine "Mid Cap" but its ₹88,783 Cr sits far above a 1,000–10,000 band, so
// the bucket filter passes while the range filter rejects it.
func TestMatcher_MarketCapTypeAndRange_AreANDed(t *testing.T) {
	e := baseEvent()
	e.StockData.MCapType = "Mid Cap"
	e.StockData.MCap = 88783.31

	s := baseStrategy()
	s.Conditions.MarketCapTypes = []string{"MID"}
	s.Conditions.MarketCapRange = models.MarketCapRange{MinMcap: 1000, MaxMcap: 10000}

	res := NewEvaluator(zap.NewNop()).Evaluate(e, s)

	if res.IsFullMatch() {
		t.Fatal("expected drop: bucket passes but the numeric range must reject")
	}
	if !hasCondition(res.MatchedConditions, "market_cap") {
		t.Errorf("market_cap (bucket) should have matched, failed: %v", res.FailedConditions)
	}
	if !hasCondition(res.FailedConditions, "market_cap_range") {
		t.Errorf("market_cap_range should have failed, matched: %v", res.MatchedConditions)
	}
}

// match_all_news auto-passes every non-impact condition. market_cap_range must
// appear in both the matched list and the score map, or the reported condition
// set is inconsistent with the other conditions.
func TestMatcher_MatchAllNews_IncludesMarketCapRange(t *testing.T) {
	e := baseEvent()
	e.StockData.MCap = 88783.31 // would fail the range below if it were evaluated

	s := baseStrategy()
	s.Conditions.MatchAllNews = true
	s.Conditions.MarketCapRange = models.MarketCapRange{MinMcap: 1000, MaxMcap: 10000}

	res := NewEvaluator(zap.NewNop()).Evaluate(e, s)

	if !res.IsFullMatch() {
		t.Fatalf("match-all should pass, failed: %v", res.FailedConditions)
	}
	if !hasCondition(res.MatchedConditions, "market_cap_range") {
		t.Error("match_all_news must auto-match market_cap_range")
	}
	if got := res.ConditionScores["market_cap_range"]; got != 100.0 {
		t.Errorf("match_all_news must score market_cap_range 100, got %v", got)
	}
}

// NOTE: Tests for per-stock-code and volume-threshold filtering were removed:
// models.Conditions has no Stocks/VolumeThreshold field and Evaluator.Evaluate
// performs no such check (it evaluates impact, sentiment, category, market cap,
// price range, pct-change, and exchange). The strategy_conditions table does have
// stock_codes/min_volume columns, but that filtering is not implemented in the
// matcher — re-add these tests alongside the feature if it is built.
