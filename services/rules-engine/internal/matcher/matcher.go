package matcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/index"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/userconfig"
	"go.uber.org/zap"
)

// Matcher is responsible for matching events against strategies
type Matcher struct {
	queryEngine      *index.QueryEngine
	strategyCache    *cache.StrategyCache
	userConfigClient *userconfig.Client
	evaluator        *Evaluator
	scorer           *Scorer
	logger           *zap.Logger
	minMatchScore    float64
	maxConcurrent    int
}

// NewMatcher creates a new matcher instance
func NewMatcher(
	queryEngine *index.QueryEngine,
	strategyCache *cache.StrategyCache,
	userConfigClient *userconfig.Client,
	minMatchScore float64,
	maxConcurrent int,
	logger *zap.Logger,
) *Matcher {
	return &Matcher{
		queryEngine:      queryEngine,
		strategyCache:    strategyCache,
		userConfigClient: userConfigClient,
		evaluator:        NewEvaluator(logger),
		scorer:           NewScorer(),
		logger:           logger,
		minMatchScore:    minMatchScore,
		maxConcurrent:    maxConcurrent,
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

			// Get full strategy from cache (required for complete trade_config)
			// Get full strategy (from cache or reconstruct)
			var strategy *models.Strategy
			if cached, exists := cachedStrategies[esStrategy.StrategyID]; exists {
				strategy = cached
				m.logger.Debug("Strategy found in cache",
					zap.String("strategy_id", esStrategy.StrategyID),
					zap.String("user_id", esStrategy.UserID))
			} else {
				// Fallback: Try fetching from user-config service via gRPC
				m.logger.Info("Strategy not in cache, attempting to fetch from user-config service",
					zap.String("strategy_id", esStrategy.StrategyID),
					zap.String("user_id", esStrategy.UserID))

				if m.userConfigClient != nil {
					fetchedStrategy, err := m.userConfigClient.GetStrategy(ctx, esStrategy.StrategyID, esStrategy.UserID)
					if err != nil {
						m.logger.Warn("Failed to fetch strategy from user-config service, using reconstructed strategy with defaults",
							zap.String("strategy_id", esStrategy.StrategyID),
							zap.String("user_id", esStrategy.UserID),
							zap.Error(err))
						// Use reconstructed strategy as fallback
						strategy = m.reconstructStrategy(esStrategy)
					} else {
						strategy = fetchedStrategy

						// Cache the fetched strategy for future use
						if err := m.strategyCache.SetStrategy(ctx, strategy); err != nil {
							m.logger.Warn("Failed to cache fetched strategy",
								zap.String("strategy_id", strategy.StrategyID),
								zap.Error(err))
						} else {
							m.logger.Info("Strategy fetched from user-config and cached successfully",
								zap.String("strategy_id", strategy.StrategyID))
						}
					}
				} else {
					m.logger.Warn("Strategy not found in cache and no user-config client available, using reconstructed strategy",
						zap.String("strategy_id", esStrategy.StrategyID),
						zap.String("user_id", esStrategy.UserID),
						zap.String("strategy_name", esStrategy.StrategyName))
					// Use reconstructed strategy as fallback
					strategy = m.reconstructStrategy(esStrategy)
				}
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
		Strategy:          strategy, // Include full strategy with trade_config from Kafka
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
	// Provide sensible defaults for trade_config when not available from cache/user-config
	return &models.Strategy{
		StrategyID:   esStrategy.StrategyID,
		UserID:       esStrategy.UserID,
		StrategyName: esStrategy.StrategyName,
		Active:       esStrategy.Active,
		Conditions: models.Conditions{
			Stocks:    esStrategy.Stocks,
			Exchanges: esStrategy.Exchanges,
			PriceRange: models.PriceRange{
				MinPrice: esStrategy.PriceMin,
				MaxPrice: esStrategy.PriceMax,
			},
			VolumeThreshold:    esStrategy.VolumeMin,
			PctChangeThreshold: esStrategy.PctChangeMin,
			MinBidQuantity:     esStrategy.MinBidQty,
			MinAskQuantity:     esStrategy.MinAskQty,
			MaxSpreadPct:       esStrategy.MaxSpreadPct,
			MinBidAskRatio:     esStrategy.MinBidAskRatio,
			MaxBidAskRatio:     esStrategy.MaxBidAskRatio,
			MinTotalDepthQty:   esStrategy.MinTotalDepthQty,
		},
		TradeConfig: models.TradeConfig{
			// Defaults - these should ideally come from cache/user-config
			OrderType:    "MARKET",
			OrderSide:    "BUY",
			Quantity:     100, // Default quantity
			Exchange:     "",  // Will be set from Elasticsearch data
			ProductType:  "INTRADAY",
			StopLossType: "FIXED",
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
