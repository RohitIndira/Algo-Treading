package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/index"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// StrategyEvent represents a strategy configuration event from Kafka
type StrategyEvent struct {
	EventType string           `json:"event_type"` // CREATE, UPDATE, DELETE, ACTIVATE, DEACTIVATE
	Strategy  *StrategyPayload `json:"strategy"`
	Timestamp int64            `json:"timestamp"`
}

// StrategyPayload represents the strategy data in the event
type StrategyPayload struct {
	StrategyID   string          `json:"strategy_id"`
	UserID       string          `json:"user_id"`
	StrategyName string          `json:"strategy_name"`
	Active       bool            `json:"active"`
	Conditions   json.RawMessage `json:"conditions"`
	TradeConfig  json.RawMessage `json:"trade_config"`
	RiskLimits   json.RawMessage `json:"risk_limits"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// StrategySync syncs strategies from Kafka to Elasticsearch and Redis cache
type StrategySyncer struct {
	reader        *kafka.Reader
	indexer       *index.Indexer
	strategyCache *cache.StrategyCache
	logger        *zap.Logger
	stats         *SyncStats
}

// SyncStats tracks sync statistics
type SyncStats struct {
	TotalProcessed int64
	Created        int64
	Updated        int64
	Deleted        int64
	Activated      int64
	Deactivated    int64
	Errors         int64
}

// NewStrategySyncer creates a new strategy syncer
func NewStrategySyncer(brokers []string, topic string, groupID string, indexer *index.Indexer, strategyCache *cache.StrategyCache, logger *zap.Logger) *StrategySyncer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.LastOffset, // Use last offset to avoid replaying old DELETE events
		MinBytes:       10e3,             // 10KB
		MaxBytes:       10e6,             // 10MB
		CommitInterval: time.Second,
		MaxWait:        500 * time.Millisecond,
	})

	return &StrategySyncer{
		reader:        reader,
		indexer:       indexer,
		strategyCache: strategyCache,
		logger:        logger,
		stats:         &SyncStats{},
	}
}

// Start starts the strategy sync process
func (ss *StrategySyncer) Start(ctx context.Context) error {
	ss.logger.Info("Starting strategy syncer...")

	for {
		select {
		case <-ctx.Done():
			ss.logger.Info("Strategy syncer stopped")
			return nil
		default:
			msg, err := ss.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				ss.logger.Error("Failed to fetch message", zap.Error(err))
				ss.stats.Errors++
				continue
			}

			if err := ss.processMessage(ctx, &msg); err != nil {
				ss.logger.Error("Failed to process message",
					zap.Error(err),
					zap.String("key", string(msg.Key)))
				ss.stats.Errors++
			}

			// Commit the message
			if err := ss.reader.CommitMessages(ctx, msg); err != nil {
				ss.logger.Error("Failed to commit message", zap.Error(err))
			}
		}
	}
}

// processMessage processes a single Kafka message
func (ss *StrategySyncer) processMessage(ctx context.Context, msg *kafka.Message) error {
	var event StrategyEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	ss.stats.TotalProcessed++

	ss.logger.Debug("Processing strategy event",
		zap.String("event_type", event.EventType),
		zap.String("strategy_id", event.Strategy.StrategyID),
		zap.String("user_id", event.Strategy.UserID))

	switch event.EventType {
	case "CREATE", "UPDATE", "ACTIVATE", "DEACTIVATE":
		strategy, err := ss.convertToStrategy(event.Strategy)
		if err != nil {
			return fmt.Errorf("failed to convert strategy: %w", err)
		}

		// Validate strategy before indexing and caching
		// This prevents incomplete strategies (especially missing trade_config) from being cached
		if err := strategy.Validate(); err != nil {
			ss.logger.Error("Received invalid strategy from Kafka, skipping cache update",
				zap.String("strategy_id", strategy.StrategyID),
				zap.String("user_id", strategy.UserID),
				zap.String("event_type", event.EventType),
				zap.Int32("quantity", strategy.TradeConfig.Quantity),
				zap.String("order_type", strategy.TradeConfig.OrderType),
				zap.Error(err))
			// Don't return error - just skip this invalid strategy
			// This prevents overwriting good cached data with incomplete data
			return nil
		}

		// Index in Elasticsearch for matching
		if err := ss.indexer.IndexStrategy(ctx, strategy); err != nil {
			return fmt.Errorf("failed to index strategy: %w", err)
		}

		// Cache in Redis for full strategy retrieval with trade_config
		if ss.strategyCache != nil {
			if err := ss.strategyCache.SetStrategy(ctx, strategy); err != nil {
				ss.logger.Warn("Failed to cache strategy in Redis",
					zap.String("strategy_id", strategy.StrategyID),
					zap.Error(err))
			} else {
				ss.logger.Debug("Strategy cached in Redis",
					zap.String("strategy_id", strategy.StrategyID),
					zap.Int32("quantity", strategy.TradeConfig.Quantity))
			}
		}

		switch event.EventType {
		case "CREATE":
			ss.stats.Created++
		case "UPDATE":
			ss.stats.Updated++
		case "ACTIVATE":
			ss.stats.Activated++
		case "DEACTIVATE":
			ss.stats.Deactivated++
		}

		ss.logger.Info("Strategy indexed and cached",
			zap.String("event_type", event.EventType),
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("user_id", strategy.UserID),
			zap.Bool("active", strategy.Active))

	case "DELETE":
		if err := ss.indexer.DeleteStrategy(ctx, event.Strategy.StrategyID); err != nil {
			return fmt.Errorf("failed to delete strategy: %w", err)
		}

		// Delete from cache too
		if ss.strategyCache != nil {
			if err := ss.strategyCache.DeleteStrategy(ctx, event.Strategy.StrategyID); err != nil {
				ss.logger.Warn("Failed to delete strategy from cache",
					zap.String("strategy_id", event.Strategy.StrategyID),
					zap.Error(err))
			}
		}

		ss.stats.Deleted++
		ss.logger.Info("Strategy deleted from index and cache",
			zap.String("strategy_id", event.Strategy.StrategyID))

	default:
		ss.logger.Warn("Unknown event type", zap.String("event_type", event.EventType))
	}

	return nil
}

// convertToStrategy converts StrategyPayload to models.Strategy
func (ss *StrategySyncer) convertToStrategy(payload *StrategyPayload) (*models.Strategy, error) {
	var conditions models.Conditions
	if err := json.Unmarshal(payload.Conditions, &conditions); err != nil {
		return nil, fmt.Errorf("failed to unmarshal conditions: %w", err)
	}

	var tradeConfig models.TradeConfig
	if err := json.Unmarshal(payload.TradeConfig, &tradeConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal trade_config: %w", err)
	}

	// Log the raw trade_config for debugging incomplete data issues
	ss.logger.Debug("Processing strategy trade_config",
		zap.String("strategy_id", payload.StrategyID),
		zap.String("user_id", payload.UserID),
		zap.Int32("quantity", tradeConfig.Quantity),
		zap.String("order_type", tradeConfig.OrderType),
		zap.String("exchange", tradeConfig.Exchange),
		zap.Float64("stop_loss_pct", tradeConfig.StopLossPct),
		zap.Float64("take_profit_pct", tradeConfig.TakeProfitPct))

	var riskLimits models.RiskLimits
	if err := json.Unmarshal(payload.RiskLimits, &riskLimits); err != nil {
		return nil, fmt.Errorf("failed to unmarshal risk_limits: %w", err)
	}

	// Normalize Kafka field values to match internal format
	tradeConfig.OrderType = normalizeOrderType(tradeConfig.OrderType)
	tradeConfig.Exchange = normalizeExchange(tradeConfig.Exchange)

	return &models.Strategy{
		StrategyID:   payload.StrategyID,
		UserID:       payload.UserID,
		StrategyName: payload.StrategyName,
		Active:       payload.Active,
		Conditions:   conditions,
		TradeConfig:  tradeConfig,
		RiskLimits:   riskLimits,
		CreatedAt:    payload.CreatedAt,
		UpdatedAt:    payload.UpdatedAt,
	}, nil
}

// normalizeOrderType converts Kafka order type format to internal format
// "ORDER_TYPE_MARKET" -> "MARKET", "ORDER_TYPE_LIMIT" -> "LIMIT"
func normalizeOrderType(orderType string) string {
	switch orderType {
	case "ORDER_TYPE_MARKET":
		return "MARKET"
	case "ORDER_TYPE_LIMIT":
		return "LIMIT"
	default:
		return orderType // Return as-is if already normalized
	}
}

// normalizeExchange converts Kafka exchange format to internal format
// "EXCHANGE_NSE" -> "NSE", "EXCHANGE_BSE" -> "BSE"
func normalizeExchange(exchange string) string {
	switch exchange {
	case "EXCHANGE_NSE":
		return "NSE"
	case "EXCHANGE_BSE":
		return "BSE"
	default:
		return exchange // Return as-is if already normalized
	}
}

// GetStats returns sync statistics
func (ss *StrategySyncer) GetStats() *SyncStats {
	return ss.stats
}

// Close closes the strategy syncer
func (ss *StrategySyncer) Close() error {
	ss.logger.Info("Closing strategy syncer",
		zap.Int64("total_processed", ss.stats.TotalProcessed),
		zap.Int64("created", ss.stats.Created),
		zap.Int64("updated", ss.stats.Updated),
		zap.Int64("deleted", ss.stats.Deleted),
		zap.Int64("errors", ss.stats.Errors))

	return ss.reader.Close()
}

