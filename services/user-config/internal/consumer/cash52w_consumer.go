package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// cash52WeekConfigEvent mirrors the complete 52W config event published by
// the StrategyService to the user-configs.cash52w topic.
type cash52WeekConfigEvent struct {
	EventType       string                  `json:"event_type"`
	UserID          string                  `json:"user_id"`
	Enabled         bool                    `json:"enabled"`
	TotalCapital    float64                 `json:"total_capital"`
	CapitalPerStock float64                 `json:"capital_per_stock"`
	MaxStocks       int                     `json:"max_stocks"`
	AutoRebalance   bool                    `json:"auto_rebalance"`
	TradingMode     string                  `json:"trading_mode"`
	StopLossLevels  models.StopLossLevels   `json:"stop_loss_levels"`
	ProfitLevels    models.ProfitLevels     `json:"profit_levels"`
	ForceExitAll    bool                    `json:"force_exit_all"`
	ForceExitStocks []string                `json:"force_exit_stocks"`
	PauseNewEntries bool                    `json:"pause_new_entries"`
	Timestamp       int64                   `json:"timestamp"`
}

// Cash52WConfigConsumer consumes 52W-only config events from Kafka and keeps
// the cash52w_configs table in sync.
type Cash52WConfigConsumer struct {
	reader *kafka.Reader
	repo   *repository.StrategyRepository
	logger *zap.Logger
}

// NewCash52WConfigConsumer creates a new consumer for the 52W config topic.
func NewCash52WConfigConsumer(brokers []string, topic, groupID string, repo *repository.StrategyRepository, logger *zap.Logger) *Cash52WConfigConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &Cash52WConfigConsumer{
		reader: reader,
		repo:   repo,
		logger: logger,
	}
}

// Start begins consuming 52W config events until the context is cancelled.
func (c *Cash52WConfigConsumer) Start(ctx context.Context) error {
	if c.reader == nil {
		return fmt.Errorf("cash52w consumer has no reader configured")
	}

	c.logger.Info("Starting Cash52W config consumer",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group_id", c.reader.Config().GroupID))

	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				c.logger.Info("Cash52W config consumer context cancelled, stopping")
				return nil
			}
			c.logger.Warn("Cash52W config consumer read error", zap.Error(err))
			continue
		}

		var ev cash52WeekConfigEvent
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			c.logger.Warn("Failed to unmarshal Cash52W config event", zap.Error(err))
			continue
		}

		if ev.UserID == "" {
			c.logger.Warn("Skipping Cash52W event with empty user_id")
			continue
		}

		cfg := &models.Cash52WConfig{
			UserID:          ev.UserID,
			Enabled:         ev.Enabled,
			TotalCapital:    ev.TotalCapital,
			CapitalPerStock: ev.CapitalPerStock,
			MaxStocks:       ev.MaxStocks,
			AutoRebalance:   ev.AutoRebalance,
			TradingMode:     ev.TradingMode,
			StopLossLevels:  ev.StopLossLevels,
			ProfitLevels:    ev.ProfitLevels,
			ForceExitAll:    ev.ForceExitAll,
			ForceExitStocks: ev.ForceExitStocks,
			PauseNewEntries: ev.PauseNewEntries,
		}

		switch ev.EventType {
		case "CREATE", "UPDATE":
			if err := c.repo.UpsertCash52WConfig(ctx, cfg); err != nil {
				c.logger.Warn("Failed to upsert Cash52W config from Kafka event",
					zap.Error(err),
					zap.String("user_id", ev.UserID),
					zap.String("event_type", ev.EventType))
			} else {
				c.logger.Debug("Upserted Cash52W config from Kafka event",
					zap.String("user_id", ev.UserID),
					zap.String("event_type", ev.EventType))
			}
		case "DELETE":
			if err := c.repo.DeleteCash52WConfig(ctx, ev.UserID); err != nil {
				c.logger.Warn("Failed to delete Cash52W config from Kafka event",
					zap.Error(err),
					zap.String("user_id", ev.UserID))
			} else {
				c.logger.Debug("Deleted Cash52W config from Kafka event",
					zap.String("user_id", ev.UserID))
			}
		default:
			c.logger.Warn("Unknown Cash52W event_type, skipping",
				zap.String("event_type", ev.EventType),
				zap.String("user_id", ev.UserID))
		}
	}
}

// Close stops the Kafka reader.
func (c *Cash52WConfigConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
