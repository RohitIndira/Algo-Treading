package matcher

import (
	"math"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// EvaluationResult holds the result of strategy evaluation
type EvaluationResult struct {
	MatchedConditions []string
	FailedConditions  []string
	ConditionScores   map[string]float64
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

	// If strategy requests depth-only matching, evaluate only stock/exchange/price/volume and depth
	if strategy.Conditions.DepthOnly {
		e.evaluateStock(event, strategy, result)
		e.evaluateExchange(event, strategy, result)
		e.evaluatePriceRange(event, strategy, result)
		e.evaluateVolume(event, strategy, result)
		e.evaluateDepthLTP(event, strategy, result)
		return result
	}

	// Evaluate each condition normally
	e.evaluateImpactScore(event, strategy, result)
	e.evaluateSentiment(event, strategy, result)
	e.evaluateCategory(event, strategy, result)
	e.evaluateStock(event, strategy, result)
	e.evaluatePriceRange(event, strategy, result)
	e.evaluateVolume(event, strategy, result)
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
		"stock",
		"price_range",
		"volume",
		"pct_change",
		"exchange",
	)

	// Set perfect scores for all auto-matched conditions
	result.ConditionScores["match_all_news"] = 100.0
	result.ConditionScores["sentiment"] = 100.0
	result.ConditionScores["category"] = 100.0
	result.ConditionScores["stock"] = 100.0
	result.ConditionScores["price_range"] = 100.0
	result.ConditionScores["volume"] = 100.0
	result.ConditionScores["pct_change"] = 100.0
	result.ConditionScores["exchange"] = 100.0
}

// evaluateImpactScore evaluates impact score condition
func (e *Evaluator) evaluateImpactScore(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "impact_score"

	// Event impact score must be >= strategy threshold
	if event.Analysis.ImpactScore >= strategy.Conditions.ImpactScoreThreshold {
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
		if sentiment == eventSentiment {
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
		if category == event.NewsData.Category {
			result.MatchedConditions = append(result.MatchedConditions, condition)
			result.ConditionScores[condition] = 100.0
			return
		}
	}

	result.FailedConditions = append(result.FailedConditions, condition)
	result.ConditionScores[condition] = 0
}

// evaluateStock evaluates stock condition
func (e *Evaluator) evaluateStock(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "stock"

	// Empty stocks list means apply to all stocks
	if len(strategy.Conditions.Stocks) == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	for _, stock := range strategy.Conditions.Stocks {
		if stock == event.StockData.StockCode {
			result.MatchedConditions = append(result.MatchedConditions, condition)
			result.ConditionScores[condition] = 100.0
			return
		}
	}

	result.FailedConditions = append(result.FailedConditions, condition)
	result.ConditionScores[condition] = 0
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

// evaluateVolume evaluates volume condition
func (e *Evaluator) evaluateVolume(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "volume"

	volume := event.MarketData.PriceMap.Volume
	minVolume := strategy.Conditions.VolumeThreshold

	// If threshold is 0, no volume filter
	if minVolume == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	// Volume must be >= threshold
	if volume >= minVolume {
		result.MatchedConditions = append(result.MatchedConditions, condition)

		// Score based on how much volume exceeds threshold
		// Diminishing returns after 2x threshold
		ratio := float64(volume) / float64(minVolume)
		score := math.Min(ratio, 2.0) / 2.0 * 100.0
		result.ConditionScores[condition] = score
	} else {
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
	}
}

// evaluatePctChange evaluates percent change condition
func (e *Evaluator) evaluatePctChange(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "pct_change"

	absPctChange := math.Abs(event.MarketData.PctChange)
	threshold := strategy.Conditions.PctChangeThreshold

	// If threshold is 0, no percent change filter
	if threshold == 0 {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	// Absolute percent change must be >= threshold
	if absPctChange >= threshold {
		result.MatchedConditions = append(result.MatchedConditions, condition)

		// Score based on how much change exceeds threshold
		// Diminishing returns after 2x threshold
		ratio := absPctChange / threshold
		score := math.Min(ratio, 2.0) / 2.0 * 100.0
		result.ConditionScores[condition] = score
	} else {
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
	}
}

// evaluateExchange evaluates exchange condition
func (e *Evaluator) evaluateExchange(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "exchange"

	// If strategy has no exchange preference, accept all
	if strategy.TradeConfig.Exchange == "" {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
		return
	}

	// Check if exchange matches
	if event.StockData.Exchange == strategy.TradeConfig.Exchange {
		result.MatchedConditions = append(result.MatchedConditions, condition)
		result.ConditionScores[condition] = 100.0
	} else {
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
	}
}

// evaluateDepthLTP evaluates depth and LTP based conditions
func (e *Evaluator) evaluateDepthLTP(event *models.MarketEvent, strategy *models.Strategy, result *EvaluationResult) {
	condition := "depth_and_ltp"

	md := event.MarketData
	ltp := md.LastTradedPrice

	// If no depth data present, fail only if strategy requires thresholds
	if len(md.BidPrices) == 0 || len(md.AskPrices) == 0 || len(md.BidQuantities) == 0 || len(md.AskQuantities) == 0 {
		// If strategy has no depth thresholds configured, consider it matched
		if strategy.Conditions.MinBidQuantity == 0 && strategy.Conditions.MinAskQuantity == 0 && strategy.Conditions.MaxSpreadPct == 0 {
			result.MatchedConditions = append(result.MatchedConditions, condition)
			result.ConditionScores[condition] = 100.0
			return
		}
		// Depth data required but missing
		result.FailedConditions = append(result.FailedConditions, condition)
		result.ConditionScores[condition] = 0
		return
	}

	bestBid := md.BidPrices[0]
	bestAsk := md.AskPrices[0]
	bestBidQty := int64(0)
	bestAskQty := int64(0)
	if len(md.BidQuantities) > 0 {
		bestBidQty = int64(md.BidQuantities[0])
	}
	if len(md.AskQuantities) > 0 {
		bestAskQty = int64(md.AskQuantities[0])
	}

	// Check min bid qty
	if strategy.Conditions.MinBidQuantity > 0 {
		if bestBidQty < strategy.Conditions.MinBidQuantity {
			result.FailedConditions = append(result.FailedConditions, condition)
			result.ConditionScores[condition] = 0
			return
		}
	}

	// Check min ask qty
	if strategy.Conditions.MinAskQuantity > 0 {
		if bestAskQty < strategy.Conditions.MinAskQuantity {
			result.FailedConditions = append(result.FailedConditions, condition)
			result.ConditionScores[condition] = 0
			return
		}
	}

	// Check spread pct
	if strategy.Conditions.MaxSpreadPct > 0 && ltp > 0 {
		spread := bestAsk - bestBid
		spreadPct := (spread / ltp) * 100.0
		if spreadPct > strategy.Conditions.MaxSpreadPct {
			result.FailedConditions = append(result.FailedConditions, condition)
			result.ConditionScores[condition] = 0
			return
		}
	}

	// Optionally check that LTP lies between best bid and best ask
	if ltp > 0 {
		if !(ltp >= bestBid && ltp <= bestAsk) {
			// If LTP outside spread, still allow if no strict spread configured
			if strategy.Conditions.MaxSpreadPct > 0 {
				result.FailedConditions = append(result.FailedConditions, condition)
				result.ConditionScores[condition] = 0
				return
			}
		}
	}

	// Passed all configured depth checks
	result.MatchedConditions = append(result.MatchedConditions, condition)
	result.ConditionScores[condition] = 100.0
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
