package matcher

import (
	"math"
	"strings"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// PctChangeStatus describes how the event's price change % compares to the strategy range.
// - "within_range": absPctChange is in [min, max]  → place order immediately
// - "below_min":    absPctChange < min             → place LIMIT order at the min-% target price
// - "above_max":    absPctChange > max             → skip trade (condition failed)
// - "":             no pct-change filter was set on this strategy
type PctChangeStatus string

const (
	PctChangeWithinRange PctChangeStatus = "within_range"
	PctChangeBelowMin    PctChangeStatus = "below_min"
	PctChangeAboveMax    PctChangeStatus = "above_max"
)

// EvaluationResult holds the result of strategy evaluation
type EvaluationResult struct {
	MatchedConditions []string
	FailedConditions  []string
	ConditionScores   map[string]float64
	// PctChangeStatus carries the three-way result of the pct-change check so
	// the handler can decide between an immediate MARKET order and a pending LIMIT order.
	PctChangeStatus PctChangeStatus
}

// Evaluator evaluates strategies against events
type Evaluator struct {
	logger *zap.Logger
}

// NewEvaluator creates a new evaluator
func NewEvaluator(logger *zap.Logger) *Evaluator {
	return &Evaluator{
		logger: logger,
	}
}

// Evaluate evaluates a strategy against an event
func (e *Evaluator) Evaluate(event *models.MarketEvent, strategy *models.Strategy) *EvaluationResult {
	result := &EvaluationResult{
		MatchedConditions: make([]string, 0),
		FailedConditions:  make([]string, 0),
		ConditionScores:   make(map[string]float64),
	}

	// Check if this is a match-all strategy (e.g., user sent "/all")
	if strategy.Conditions.MatchAllNews {
		e.evaluateMatchAllStrategy(event, strategy, result)
		return result
	}

	// Evaluate each condition normally
	e.evaluateImpactScore(event, strategy, result)
	e.evaluateSentiment(event, strategy, result)
	e.evaluateCategory(event, strategy, result)
	e.evaluateMarketCap(event, strategy, result)
	e.evaluateMarketCapRange(event, strategy, result)
	e.evaluatePriceRange(event, strategy, result)
	e.evaluatePctChange(event, strategy, result)
	e.evaluateExchange(event, strategy, result)

	return result
}

// evaluateMatchAllStrategy evaluates a match-all strategy
// When match_all_news=true, only impact score is checked, all other conditions are auto-matched
func (e *Evaluator) evaluateMatchAllStrategy(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	e.logger.Debug("Evaluating match-all strategy",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("event_id", event.EventID))

	// Still check impact score threshold
	e.evaluateImpactScore(event, strategy, result)

	// Auto-match all other conditions
	result.MatchedConditions = append(result.MatchedConditions,
		"match_all_news",
		"sentiment",
		"category",
		"market_cap",
		"market_cap_range",
		"price_range",
		"pct_change",
		"exchange",
	)

	// Set perfect scores for all auto-matched conditions
	result.ConditionScores["match_all_news"] = 100.0
	result.ConditionScores["sentiment"] = 100.0
	result.ConditionScores["category"] = 100.0
	result.ConditionScores["market_cap"] = 100.0
	result.ConditionScores["market_cap_range"] = 100.0
	result.ConditionScores["price_range"] = 100.0
	result.ConditionScores["pct_change"] = 100.0
	result.ConditionScores["exchange"] = 100.0
}

// evaluateImpactScore evaluates impact score condition
func (e *Evaluator) evaluateImpactScore(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "impact_score"

	min := strategy.Conditions.ImpactScoreMin
	max := strategy.Conditions.ImpactScoreMax
	// if max is unset (0), treat as 10
	if max == 0 {
		max = 10
	}

	// Event impact score must be within [min,max]
	if event.Analysis.ImpactScore >= min && event.Analysis.ImpactScore <= max {
		result.MatchedConditions = append(result.MatchedConditions, condition)

		// Score based on how much the impact exceeds threshold
		// Max score if impact is at maximum (10)
		score := float64(event.Analysis.ImpactScore) / 10.0 * 100.0
		result.ConditionScores[condition] = score
	} else {
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
	}
}

// evaluateSentiment evaluates sentiment condition
func (e *Evaluator) evaluateSentiment(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "sentiment"

	// Empty sentiments list means accept all sentiments
	if len(strategy.Conditions.Sentiments) == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	eventSentiment := event.Analysis.GetSentimentValue()

	for _, sentiment := range strategy.Conditions.Sentiments {
		if strings.EqualFold(sentiment, eventSentiment) {
			result.MatchedConditions = append(result.MatchedConditions, condition)
			result.ConditionScores[condition] = 100.0
			return
		}
	}

	result.FailedConditions = append(result.FailedConditions, condition)
	result.ConditionScores[condition] = 0
}

// evaluateCategory evaluates category condition
func (e *Evaluator) evaluateCategory(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "category"

	// Empty categories list means accept all categories
	if len(strategy.Conditions.Categories) == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	for _, category := range strategy.Conditions.Categories {
		if strings.EqualFold(category, event.NewsData.Category) {
			result.MatchedConditions = append(result.MatchedConditions, condition)
			result.ConditionScores[condition] = 100.0
			return
		}
	}

	result.FailedConditions = append(result.FailedConditions, condition)
	result.ConditionScores[condition] = 0
}

// evaluateMarketCap evaluates market cap type condition.
// Strategy conditions store the bare bucket ("SMALL", "MID", "LARGE"), while
// Kafka news events carry the feed's suffixed form ("Small Cap", "Mid Cap",
// "Large Cap"). A plain case-insensitive compare therefore never matched, so
// any strategy with a market_cap_types filter silently produced zero signals.
// normMCap canonicalizes both sides (uppercase, spaces removed, trailing "CAP"
// dropped) so "SMALL" and "Small Cap" both reduce to "SMALL" before comparing.
func (e *Evaluator) evaluateMarketCap(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "market_cap"

	// Empty market_cap_types list means accept all
	if len(strategy.Conditions.MarketCapTypes) == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	normMCap := func(s string) string {
		up := strings.ReplaceAll(strings.ToUpper(strings.TrimSpace(s)), " ", "")
		return strings.TrimSuffix(up, "CAP")
	}
	eventMCapType := normMCap(event.StockData.MCapType)

	for _, mcapType := range strategy.Conditions.MarketCapTypes {
		if normMCap(mcapType) == eventMCapType {
			result.MatchedConditions = append(result.MatchedConditions, condition)
			result.ConditionScores[condition] = 100.0
			return
		}
	}

	result.FailedConditions = append(result.FailedConditions, condition)
	result.ConditionScores[condition] = 0
}

// evaluateMarketCapRange evaluates the numeric market-cap range filter (₹ crore),
// sourced from OdinMasterData.CompanyMaster.mcap and carried on StockData.MCap.
//
// This is a STRICT bounded range — there is no open-ended form:
//   - min == 0 && max == 0 → filter not set, auto-pass
//   - otherwise            → min <= mcap <= max (both bounds inclusive)
//
// The gateway rejects max < min (and a min with no max) with HTTP 400, so no
// swap or +Inf fallback is done here; an out-of-order range can only mean
// corrupt config and is failed closed.
//
// This condition is independent of evaluateMarketCap (the SMALL/MID/LARGE bucket
// filter). A strategy may set both, in which case a stock must satisfy both —
// IsFullMatch() requires every condition to pass.
//
// A stock whose CompanyMaster doc has no mcap decodes to 0.0 and fails any active
// filter: an unknown cap cannot be proven to be inside the requested band.
func (e *Evaluator) evaluateMarketCapRange(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "market_cap_range"

	mcap := event.StockData.MCap
	min := strategy.Conditions.MarketCapRange.MinMcap
	max := strategy.Conditions.MarketCapRange.MaxMcap

	// Both zero → filter not configured.
	if min == 0 && max == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	// Defensive: validation rejects this at the API boundary, so it should be
	// unreachable. Fail closed and log loudly rather than trade on bad config.
	if max < min {
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
		e.logger.Warn("market_cap_range has max < min — rejecting; strategy config is corrupt",
			zap.Float64("min_mcap", min),
			zap.Float64("max_mcap", max),
			zap.String("strategy_id", strategy.StrategyID))
		return
	}

	if mcap >= min && mcap <= max {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	result.FailedConditions = append(result.FailedConditions, condition)
	result.ConditionScores[condition] = 0
	e.logger.Debug("market_cap_range filter excluded stock",
		zap.String("isin", event.StockData.ISIN),
		zap.Float64("mcap", mcap),
		zap.Float64("min_mcap", min),
		zap.Float64("max_mcap", max),
		zap.String("strategy_id", strategy.StrategyID))
}

// evaluatePriceRange evaluates price range condition
func (e *Evaluator) evaluatePriceRange(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "price_range"

	price := event.MarketData.LastTradedPrice
	minPrice := strategy.Conditions.PriceRange.MinPrice
	maxPrice := strategy.Conditions.PriceRange.MaxPrice

	// If both min and max are 0, no price filter
	if minPrice == 0 && maxPrice == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	// Check if price is within range
	if price >= minPrice && price <= maxPrice {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
	} else {
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
	}
}

// evaluatePctChange evaluates percent change condition.
//
// Three outcomes:
//   - above_max  → condition FAILS  (trade skipped)
//   - within_range → condition PASSES, MARKET order (existing behaviour)
//   - below_min  → condition PASSES, LIMIT order at the min-% target price
//     The handler reads result.PctChangeStatus to decide which order type to use.
func (e *Evaluator) evaluatePctChange(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "pct_change"

	absPctChange := math.Abs(event.MarketData.PctChange)
	min := strategy.Conditions.MinPctChange
	max := strategy.Conditions.MaxPctChange

	// If both are 0, no percent change filter — auto-pass.
	if min == 0 && max == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}
	// Treat unset max as infinity.
	effectiveMax := max
	if effectiveMax == 0 {
		effectiveMax = math.MaxFloat64
	}

	// Case 3 – at or above max: skip trade.
	if absPctChange >= effectiveMax {
		result.PctChangeStatus = PctChangeAboveMax
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
		e.logger.Info("pct_change above max — trade skipped",
			zap.Float64("abs_pct_change", absPctChange),
			zap.Float64("max_pct_change", effectiveMax),
			zap.String("strategy_id", strategy.StrategyID))
		return
	}

	// Case 1 – within [min, max]: place order immediately.
	if absPctChange >= min {
		result.PctChangeStatus = PctChangeWithinRange
		result.MatchedConditions = append(result.MatchedConditions, condition)

		if effectiveMax == math.MaxFloat64 {
			ratio := 1.0
			if min > 0 {
				ratio = absPctChange / min
			}
			result.ConditionScores[condition] = math.Min(ratio, 2.0) / 2.0 * 100.0
			return
		}
		span := effectiveMax - min
		if span <= 0 {
			result.ConditionScores[condition] = 100.0
			return
		}
		score := (absPctChange - min) / span * 100.0
		if score < 0 {
			score = 0
		}
		if score > 100 {
			score = 100
		}
		result.ConditionScores[condition] = score
		return
	}

	// Case 2 – below min: still a match, but handler must place a LIMIT order.
	// The order price will be set to the level where the stock reaches min%.
	result.PctChangeStatus = PctChangeBelowMin
	result.MatchedConditions = append(result.MatchedConditions, condition)
	result.ConditionScores[condition] = 50.0 // partial score; order is pending entry
	e.logger.Info("pct_change below min — LIMIT order will be placed at target price",
		zap.Float64("abs_pct_change", absPctChange),
		zap.Float64("min_pct_change", min),
		zap.String("strategy_id", strategy.StrategyID))
}

// evaluateExchange evaluates exchange condition against the Conditions.Exchanges filter list.
// TradeConfig.Exchange is the execution exchange (where to place orders) and is NOT used for filtering.
func (e *Evaluator) evaluateExchange(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "exchange"

	// Empty filter list means accept all exchanges
	if len(strategy.Conditions.Exchanges) == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	// Strategy conditions store proto-prefixed names (e.g. "EXCHANGE_NSE") while
	// Kafka news events carry plain names ("NSE"). Normalize both before comparing.
	normExch := func(s string) string {
		return strings.ToUpper(strings.TrimPrefix(strings.ToUpper(s), "EXCHANGE_"))
	}
	eventExch := normExch(event.StockData.Exchange)
	for _, ex := range strategy.Conditions.Exchanges {
		if normExch(ex) == eventExch {
			result.MatchedConditions = append(result.MatchedConditions, condition)
			result.ConditionScores[condition] = 100.0
			return
		}
	}

	result.FailedConditions = append(result.FailedConditions, condition)
	result.ConditionScores[condition] = 0
}

// GetMatchedConditionCount returns the number of matched conditions
func (r *EvaluationResult) GetMatchedConditionCount() int {
	return len(r.MatchedConditions)
}

// GetFailedConditionCount returns the number of failed conditions
func (r *EvaluationResult) GetFailedConditionCount() int {
	return len(r.FailedConditions)
}

// GetTotalConditionCount returns the total number of conditions evaluated
func (r *EvaluationResult) GetTotalConditionCount() int {
	return len(r.MatchedConditions) + len(r.FailedConditions)
}

// IsFullMatch returns true if all conditions matched
func (r *EvaluationResult) IsFullMatch() bool {
	return len(r.FailedConditions) == 0
}
