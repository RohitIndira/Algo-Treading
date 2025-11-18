package matcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/index"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// Matcher is responsible for matching events against strategies
type Matcher struct {
	queryEngine   *index.QueryEngine
	strategyCache *cache.StrategyCache
	evaluator     *Evaluator
	scorer        *Scorer
	logger        *zap.Logger
	minMatchScore float64
	maxConcurrent int
}

// NewMatcher creates a new matcher instance
func NewMatcher(
	queryEngine *index.QueryEngine,
	strategyCache *cache.StrategyCache,
	minMatchScore float64,
	maxConcurrent int,
	logger *zap.Logger,
) *Matcher {
	return &Matcher{
		queryEngine:   queryEngine,
		strategyCache: strategyCache,
		evaluator:     NewEvaluator(logger),
		scorer:        NewScorer(),
		logger:        logger,
		minMatchScore: minMatchScore,
		maxConcurrent: maxConcurrent,
	}
}

// MatchEvent matches an event against all relevant strategies
func (m *Matcher) MatchEvent(ctx context.Context, event *models.MarketEvent) ([]*models.RuleMatch, error) {
	startTime := time.Now()

	// Validate event
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("invalid event: %w", err)
	}

	// Find candidate strategies from Elasticsearch
	candidates, err := m.queryEngine.FindMatchingStrategies(ctx, event)
	if err != nil {
		return nil, fmt.Errorf("failed to find candidates: %w", err)
	}

	if len(candidates) == 0 {
		m.logger.Debug("No candidate strategies found",
			zap.String("event_id", event.EventID),
			zap.Int64("stock_code", event.StockData.StockCode))
		return []*models.RuleMatch{}, nil
	}

	m.logger.Debug("Found candidate strategies",
		zap.String("event_id", event.EventID),
		zap.Int("count", len(candidates)))

	// Extract strategy IDs
	strategyIDs := make([]string, len(candidates))
	for i, candidate := range candidates {
		strategyIDs[i] = candidate.StrategyID
	}

	// Try to get full strategies from cache
	cachedStrategies, err := m.strategyCache.GetStrategies(ctx, strategyIDs)
	if err != nil {
		m.logger.Warn("Failed to get strategies from cache", zap.Error(err))
		cachedStrategies = make(map[string]*models.Strategy)
	}

	// Evaluate strategies concurrently
	matches := m.evaluateStrategiesConcurrent(ctx, event, candidates, cachedStrategies)

	duration := time.Since(startTime)
	m.logger.Info("Event matching completed",
		zap.String("event_id", event.EventID),
		zap.Int("candidates", len(candidates)),
		zap.Int("matches", len(matches)),
		zap.Duration("duration", duration))

	return matches, nil
}

// evaluateStrategiesConcurrent evaluates strategies concurrently
func (m *Matcher) evaluateStrategiesConcurrent(
	ctx context.Context,
	event *models.MarketEvent,
	candidates []*models.ElasticsearchStrategy,
	cachedStrategies map[string]*models.Strategy,
) []*models.RuleMatch {

	// Create buffered channel for results
	resultsChan := make(chan *models.RuleMatch, len(candidates))

	// Create semaphore for concurrency control
	sem := make(chan struct{}, m.maxConcurrent)

	// Wait group for all evaluations
	var wg sync.WaitGroup

	// Evaluate each candidate
	for _, candidate := range candidates {
		wg.Add(1)

		go func(esStrategy *models.ElasticsearchStrategy) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Check context cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Get full strategy (from cache or reconstruct)
			var strategy *models.Strategy
			if cached, exists := cachedStrategies[esStrategy.StrategyID]; exists {
				strategy = cached
			} else {
				// Reconstruct strategy from ES data
				strategy = m.reconstructStrategy(esStrategy)
			}

			// Evaluate strategy against event
			match := m.evaluateStrategy(ctx, event, strategy)
			if match != nil {
				resultsChan <- match
			}
		}(candidate)
	}

	// Close results channel when all evaluations complete
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	matches := make([]*models.RuleMatch, 0)
	for match := range resultsChan {
		matches = append(matches, match)
	}

	return matches
}

// evaluateStrategy evaluates a single strategy against an event
func (m *Matcher) evaluateStrategy(ctx context.Context, event *models.MarketEvent, strategy *models.Strategy) *models.RuleMatch {
	// Skip inactive strategies
	if !strategy.Active {
		return nil
	}

	// Evaluate conditions
	result := m.evaluator.Evaluate(event, strategy)

	// Calculate match score
	score := m.scorer.CalculateScore(result)

	// Check if score meets minimum threshold
	if score < m.minMatchScore {
		m.logger.Warn("Strategy score below threshold",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("strategy_name", strategy.StrategyName),
			zap.Float64("score", score),
			zap.Float64("threshold", m.minMatchScore),
			zap.Strings("matched", result.MatchedConditions),
			zap.Strings("failed", result.FailedConditions))
		return nil
	}

	// Create match result
	match := &models.RuleMatch{
		UserID:            strategy.UserID,
		StrategyID:        strategy.StrategyID,
		StrategyName:      strategy.StrategyName,
		MatchScore:        score,
		MatchedConditions: result.MatchedConditions,
		FailedConditions:  result.FailedConditions,
		ApprovedByRisk:    false, // Will be set by risk management
		Timestamp:         time.Now(),
		EventID:           event.EventID,
	}

	m.logger.Debug("Strategy matched",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("user_id", strategy.UserID),
		zap.Float64("score", score),
		zap.Strings("matched_conditions", result.MatchedConditions))

	return match
}

// reconstructStrategy reconstructs a full strategy from Elasticsearch data
func (m *Matcher) reconstructStrategy(esStrategy *models.ElasticsearchStrategy) *models.Strategy {
	return &models.Strategy{
		StrategyID:   esStrategy.StrategyID,
		UserID:       esStrategy.UserID,
		StrategyName: esStrategy.StrategyName,
		Active:       esStrategy.Active,
		Conditions: models.Conditions{
			MatchAllNews:         esStrategy.MatchAllNews,
			ImpactScoreThreshold: esStrategy.ImpactScoreMin,
			Sentiments:           esStrategy.Sentiments,
			Categories:           esStrategy.Categories,
			Stocks:               esStrategy.Stocks,
			PriceRange: models.PriceRange{
				MinPrice: esStrategy.PriceMin,
				MaxPrice: esStrategy.PriceMax,
			},
			VolumeThreshold:    esStrategy.VolumeMin,
			PctChangeThreshold: esStrategy.PctChangeMin,
		},
		TradeConfig: models.TradeConfig{
			Exchange: esStrategy.Exchange, // Already normalized in Elasticsearch
			// Note: Some fields may not be available in ES index
			// They should be fetched from cache or User Config Service
		},
		RiskLimits: models.RiskLimits{
			MaxDailyTrades: esStrategy.MaxDailyTrades,
			MaxLossPerDay:  esStrategy.MaxLossPerDay,
		},
		UpdatedAt: time.Unix(esStrategy.UpdatedAt, 0),
	}
}

// EvaluateSingleStrategy evaluates a single strategy (for testing/admin)
func (m *Matcher) EvaluateSingleStrategy(ctx context.Context, event *models.MarketEvent, strategy *models.Strategy) (*models.RuleMatch, error) {
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("invalid event: %w", err)
	}

	if err := strategy.Validate(); err != nil {
		return nil, fmt.Errorf("invalid strategy: %w", err)
	}

	match := m.evaluateStrategy(ctx, event, strategy)
	if match == nil {
		return nil, models.ErrNoMatchFound
	}

	return match, nil
}
