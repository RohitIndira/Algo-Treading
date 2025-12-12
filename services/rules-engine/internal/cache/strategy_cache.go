package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// StrategyCache manages caching of strategies
type StrategyCache struct {
	cache  Cache
	logger *zap.Logger
	ttl    time.Duration
}

// NewStrategyCache creates a new strategy cache
func NewStrategyCache(cache Cache, ttl time.Duration, logger *zap.Logger) *StrategyCache {
	return &StrategyCache{
		cache:  cache,
		logger: logger,
		ttl:    ttl,
	}
}

// GetStrategy retrieves a strategy from cache
func (sc *StrategyCache) GetStrategy(ctx context.Context, strategyID string) (*models.Strategy, error) {
	key := sc.strategyKey(strategyID)

	data, err := sc.cache.Get(ctx, key)
	if err != nil {
		if err == models.ErrCacheMiss {
			sc.logger.Debug("Strategy cache miss", zap.String("strategy_id", strategyID))
		}
		return nil, err
	}

	var strategy models.Strategy
	if err := json.Unmarshal([]byte(data), &strategy); err != nil {
		sc.logger.Error("Failed to unmarshal cached strategy",
			zap.String("strategy_id", strategyID),
			zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal strategy: %w", err)
	}

	sc.logger.Debug("Strategy cache hit", zap.String("strategy_id", strategyID))
	return &strategy, nil
}

// calculateTTLUntilMidnight calculates the duration until next midnight (12 AM)
func (sc *StrategyCache) calculateTTLUntilMidnight() time.Duration {
	now := time.Now()
	// Get next midnight: tomorrow at 00:00:00
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	ttl := nextMidnight.Sub(now)

	sc.logger.Debug("Calculated TTL until midnight",
		zap.Duration("ttl", ttl),
		zap.Time("next_midnight", nextMidnight))

	return ttl
}

// SetStrategy stores a strategy in cache with TTL until midnight
func (sc *StrategyCache) SetStrategy(ctx context.Context, strategy *models.Strategy) error {
	key := sc.strategyKey(strategy.StrategyID)
	ttl := sc.calculateTTLUntilMidnight()

	if err := sc.cache.Set(ctx, key, strategy, ttl); err != nil {
		sc.logger.Error("Failed to cache strategy",
			zap.String("strategy_id", strategy.StrategyID),
			zap.Error(err))
		return err
	}

	sc.logger.Debug("Strategy cached until midnight",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("user_id", strategy.UserID),
		zap.Duration("ttl", ttl))

	return nil
}

// GetStrategies retrieves multiple strategies from cache
func (sc *StrategyCache) GetStrategies(ctx context.Context, strategyIDs []string) (map[string]*models.Strategy, error) {
	if len(strategyIDs) == 0 {
		return make(map[string]*models.Strategy), nil
	}

	keys := make([]string, len(strategyIDs))
	for i, id := range strategyIDs {
		keys[i] = sc.strategyKey(id)
	}

	data, err := sc.cache.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	strategies := make(map[string]*models.Strategy)
	for i, id := range strategyIDs {
		if val, exists := data[keys[i]]; exists {
			var strategy models.Strategy
			if err := json.Unmarshal([]byte(val), &strategy); err != nil {
				sc.logger.Error("Failed to unmarshal cached strategy",
					zap.String("strategy_id", id),
					zap.Error(err))
				continue
			}
			strategies[id] = &strategy
		}
	}

	sc.logger.Debug("Batch strategy cache lookup",
		zap.Int("requested", len(strategyIDs)),
		zap.Int("found", len(strategies)))

	return strategies, nil
}

// SetStrategies stores multiple strategies in cache with TTL until midnight
// SetStrategies stores multiple strategies in cache
func (sc *StrategyCache) SetStrategies(ctx context.Context, strategies []*models.Strategy) error {
	if len(strategies) == 0 {
		return nil
	}

	items := make(map[string]interface{})
	for _, strategy := range strategies {
		key := sc.strategyKey(strategy.StrategyID)
		items[key] = strategy
	}

	ttl := sc.calculateTTLUntilMidnight()
	if err := sc.cache.SetMulti(ctx, items, ttl); err != nil {
		sc.logger.Error("Failed to cache strategies",
			zap.Int("count", len(strategies)),
			zap.Error(err))
		return err
	}

	sc.logger.Debug("Strategies cached until midnight",
		zap.Int("count", len(strategies)),
		zap.Duration("ttl", ttl))
	return nil
}

// DeleteStrategy removes a strategy from cache
func (sc *StrategyCache) DeleteStrategy(ctx context.Context, strategyID string) error {
	key := sc.strategyKey(strategyID)

	if err := sc.cache.Delete(ctx, key); err != nil {
		sc.logger.Error("Failed to delete cached strategy",
			zap.String("strategy_id", strategyID),
			zap.Error(err))
		return err
	}

	sc.logger.Debug("Strategy removed from cache", zap.String("strategy_id", strategyID))
	return nil
}

// InvalidateUserStrategies invalidates all strategies for a user
func (sc *StrategyCache) InvalidateUserStrategies(ctx context.Context, userID string) error {
	pattern := fmt.Sprintf("strategy:*:user:%s", userID)

	if err := sc.cache.DeletePattern(ctx, pattern); err != nil {
		sc.logger.Error("Failed to invalidate user strategies",
			zap.String("user_id", userID),
			zap.Error(err))
		return err
	}

	sc.logger.Info("User strategies invalidated", zap.String("user_id", userID))
	return nil
}

// InvalidateAll clears all cached strategies
func (sc *StrategyCache) InvalidateAll(ctx context.Context) error {
	pattern := "strategy:*"

	if err := sc.cache.DeletePattern(ctx, pattern); err != nil {
		sc.logger.Error("Failed to invalidate all strategies", zap.Error(err))
		return err
	}

	sc.logger.Info("All strategies invalidated")
	return nil
}

// WarmCache pre-loads strategies into cache
func (sc *StrategyCache) WarmCache(ctx context.Context, strategies []*models.Strategy) error {
	if len(strategies) == 0 {
		sc.logger.Info("No strategies to warm cache")
		return nil
	}

	if err := sc.SetStrategies(ctx, strategies); err != nil {
		return fmt.Errorf("failed to warm cache: %w", err)
	}

	sc.logger.Info("Cache warmed",
		zap.Int("strategy_count", len(strategies)))

	return nil
}

// strategyKey generates a cache key for a strategy
func (sc *StrategyCache) strategyKey(strategyID string) string {
	return fmt.Sprintf("strategy:%s", strategyID)
}

// GetUserStrategies retrieves strategies for a user (requires pattern scan)
// Note: This is expensive and should be used sparingly
func (sc *StrategyCache) GetUserStrategies(ctx context.Context, userID string) ([]*models.Strategy, error) {
	// This implementation would require additional indexing
	// For production, consider maintaining a separate index of user -> strategy IDs
	sc.logger.Warn("GetUserStrategies called - this is an expensive operation",
		zap.String("user_id", userID))

	// Return empty for now - should be implemented with proper indexing if needed
	return []*models.Strategy{}, nil
}

// Exists checks if a strategy exists in cache
func (sc *StrategyCache) Exists(ctx context.Context, strategyID string) (bool, error) {
	key := sc.strategyKey(strategyID)
	return sc.cache.Exists(ctx, key)
}
