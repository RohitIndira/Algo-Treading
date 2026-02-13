package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"go.uber.org/zap"
)

// ConfigPublisher publishes user configuration updates to Kafka
// m2 flush 15ngine you need to go and find majer bug there 
//
// ARCHITECTURE:
// 1. User updates config via API Gateway
// 2. user-config saves to PostgreSQL
// 3. user-config publishes to Kafka topic: user-configs.cash52w
// 4. rules-engine consumes and updates in-memory ConfigStore
// 5. rules-engine uses cached config (ZERO DB reads during trading!)
type ConfigPublisher struct {
	producer sarama.SyncProducer
	topic    string
	logger   *logger.Logger
}

// NewConfigPublisher creates a new Kafka config publisher
func NewConfigPublisher(brokers []string, topic string, lgr *logger.Logger) (*ConfigPublisher, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Idempotent = true // Ensure exactly-once delivery
	config.Net.MaxOpenRequests = 1    // Required for idempotent producer

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	lgr.Info("Kafka config publisher initialized",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic))

	return &ConfigPublisher{
		producer: producer,
		topic:    topic,
		logger:   lgr,
	}, nil
}

// PublishConfigUpdate publishes a configuration update event
func (p *ConfigPublisher) PublishConfigUpdate(ctx context.Context, config *models.Cash52WConfig) error {
	// Create event wrapper with event_type and ALL config fields
	now := time.Now()
	event := map[string]interface{}{
		"event_type":        "UPDATE",
		"user_id":           config.UserID,
		"enabled":           config.Enabled,
		"total_capital":     config.TotalCapital,
		"capital_per_stock": config.CapitalPerStock,
		"max_stocks":        config.MaxStocks,
		"auto_rebalance":    config.AutoRebalance,
		"trading_mode":      config.TradingMode,
		"stop_loss_levels":  config.StopLossLevels,
		"profit_levels":     config.ProfitLevels,
		"force_exit_all":    config.ForceExitAll,
		"force_exit_stocks": config.ForceExitStocks,
		"pause_new_entries": config.PauseNewEntries,
		"updated_at":        now,
		"version":           config.Version,
	}

	// Serialize event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		p.logger.Error("Failed to marshal config event",
			zap.String("user_id", config.UserID),
			zap.Error(err))
		return fmt.Errorf("failed to marshal config event: %w", err)
	}

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(config.UserID), // Partition by user_id
		Value: sarama.ByteEncoder(data),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("event_type"),
				Value: []byte("config_update"),
			},
			{
				Key:   []byte("user_id"),
				Value: []byte(config.UserID),
			},
			{
				Key:   []byte("enabled"),
				Value: []byte(fmt.Sprintf("%t", config.Enabled)),
			},
			{
				Key:   []byte("trading_mode"),
				Value: []byte(config.TradingMode),
			},
		},
	}

	// Publish to Kafka
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.logger.Error("Failed to publish config update to Kafka",
			zap.String("user_id", config.UserID),
			zap.String("topic", p.topic),
			zap.Error(err))
		return fmt.Errorf("failed to publish to Kafka: %w", err)
	}

	p.logger.Info("Published config update to Kafka",
		zap.String("user_id", config.UserID),
		zap.String("topic", p.topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.Bool("enabled", config.Enabled),
		zap.String("trading_mode", config.TradingMode),
		zap.Float64("capital_per_stock", config.CapitalPerStock),
		zap.Int("max_stocks", config.MaxStocks))

	return nil
}

// PublishForceExit publishes a force exit command
// This is a high-priority command for emergency situations
func (p *ConfigPublisher) PublishForceExit(ctx context.Context, userID string, forceExitAll bool, stocks []string) error {
	event := map[string]interface{}{
		"user_id":        userID,
		"force_exit_all": forceExitAll,
		"stocks":         stocks,
		"timestamp":      ctx.Value("timestamp"),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal force exit event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(userID),
		Value: sarama.ByteEncoder(data),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("event_type"),
				Value: []byte("force_exit"),
			},
			{
				Key:   []byte("user_id"),
				Value: []byte(userID),
			},
			{
				Key:   []byte("priority"),
				Value: []byte("high"), // High priority for emergency
			},
		},
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.logger.Error("Failed to publish force exit to Kafka",
			zap.String("user_id", userID),
			zap.Error(err))
		return fmt.Errorf("failed to publish force exit: %w", err)
	}

	p.logger.Warn("Published force exit command to Kafka",
		zap.String("user_id", userID),
		zap.Bool("force_exit_all", forceExitAll),
		zap.Strings("stocks", stocks),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))

	return nil
}

// Close closes the Kafka producer
func (p *ConfigPublisher) Close() error {
	if err := p.producer.Close(); err != nil {
		p.logger.Error("Failed to close Kafka producer", zap.Error(err))
		return err
	}
	p.logger.Info("Kafka config publisher closed")
	return nil
}
