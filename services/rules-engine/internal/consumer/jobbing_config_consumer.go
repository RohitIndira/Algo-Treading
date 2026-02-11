package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/jobbing"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// JobbingConfigEvent represents the configuration event from user-config service
type JobbingConfigEvent struct {
	EventType string `json:"event_type"` // CREATED, UPDATED, DELETED, ENABLED, DISABLED
	UserID    string `json:"user_id"`
	Token     string `json:"token"`
	Config    struct {
		ID               string  `json:"id"`
		UserID           string  `json:"user_id"`
		Token            string  `json:"token"`
		Symbol           string  `json:"symbol"`
		Exchange         string  `json:"exchange"`
		LowerRange       float64 `json:"lower_range"`
		HigherRange      float64 `json:"higher_range"`
		InitialBuyOffset float64 `json:"initial_buy_offset"`
		DistanceContinue float64 `json:"distance_continue"`
		QuantityPerOrder int32   `json:"quantity_per_order"`
		MaxQuantity      int32   `json:"max_quantity"`
		TradingMode      string  `json:"trading_mode"`
		Enabled          bool    `json:"enabled"`
	} `json:"config"`
}

// JobbingConfigHandler defines the interface for handling jobbing configuration events
type JobbingConfigHandler interface {
	SetJobbingConfig(userID string, tokens []string, cfg jobbing.UserTokenConfig)
	RemoveJobbingConfig(userID string, token string)
}

// JobbingConfigConsumer consumes jobbing configuration events from Kafka
type JobbingConfigConsumer struct {
	reader  *kafka.Reader
	handler JobbingConfigHandler
	logger  *zap.Logger
}

// NewJobbingConfigConsumer creates a new consumer for jobbing configuration events
func NewJobbingConfigConsumer(brokers []string, topic, groupID string, handler JobbingConfigHandler, logger *zap.Logger) (*JobbingConfigConsumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("jobbing config topic cannot be empty")
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers cannot be empty")
	}
	if groupID == "" {
		groupID = "rules-engine-jobbing-config-v1"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset, // Process all config events from beginning
		MaxWait:        500 * time.Millisecond,
	})

	logger.Info("Jobbing config Kafka consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group", groupID))

	return &JobbingConfigConsumer{
		reader:  reader,
		handler: handler,
		logger:  logger,
	}, nil
}

// Start begins consuming jobbing configuration messages until context is cancelled
func (c *JobbingConfigConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Jobbing config Kafka consumer")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Jobbing config Kafka consumer stopped")
			return ctx.Err()
		default:
			if err := c.processMessage(ctx); err != nil {
				c.logger.Error("Failed to process jobbing config message", zap.Error(err))
				// Continue processing on errors
			}
		}
	}
}

func (c *JobbingConfigConsumer) processMessage(ctx context.Context) error {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch jobbing config message: %w", err)
	}

	c.logger.Debug("Received jobbing config Kafka message",
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.Time("time", msg.Time),
		zap.String("key", string(msg.Key)))

	var event JobbingConfigEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logger.Error("Failed to unmarshal JobbingConfigEvent",
			zap.Error(err),
			zap.ByteString("message", msg.Value))
		// Commit to skip malformed messages
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("Failed to commit message after unmarshal error", zap.Error(commitErr))
		}
		return err
	}

	// Process the configuration event
	if err := c.handleConfigEvent(&event); err != nil {
		c.logger.Error("Failed to handle jobbing config event",
			zap.Error(err),
			zap.String("event_type", event.EventType),
			zap.String("user_id", event.UserID),
			zap.String("token", event.Token))
	}

	// Commit the message
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit jobbing config message", zap.Error(err))
		return err
	}

	return nil
}

func (c *JobbingConfigConsumer) handleConfigEvent(event *JobbingConfigEvent) error {
	switch strings.ToUpper(event.EventType) {
	case "CREATED", "UPDATED", "ENABLED":
		// Add or update configuration
		cfg := jobbing.UserTokenConfig{
			LowerRange:       event.Config.LowerRange,
			HigherRange:      event.Config.HigherRange,
			InitialBuyOffset: event.Config.InitialBuyOffset,
			DistanceContinue: event.Config.DistanceContinue,
			QuantityPerOrder: event.Config.QuantityPerOrder,
			MaxQuantity:      event.Config.MaxQuantity,
		}

		// Only process if enabled
		if event.Config.Enabled || strings.ToUpper(event.EventType) == "ENABLED" {
			c.handler.SetJobbingConfig(event.UserID, []string{event.Token}, cfg)
			c.logger.Info("Applied jobbing configuration",
				zap.String("event_type", event.EventType),
				zap.String("user_id", event.UserID),
				zap.String("token", event.Token),
				zap.String("symbol", event.Config.Symbol),
				zap.Float64("lower_range", cfg.LowerRange),
				zap.Float64("higher_range", cfg.HigherRange))
		}

	case "DISABLED", "DELETED":
		// Remove configuration
		c.handler.RemoveJobbingConfig(event.UserID, event.Token)
		c.logger.Info("Removed jobbing configuration",
			zap.String("event_type", event.EventType),
			zap.String("user_id", event.UserID),
			zap.String("token", event.Token))

	default:
		c.logger.Warn("Unknown jobbing config event type",
			zap.String("event_type", event.EventType),
			zap.String("user_id", event.UserID),
			zap.String("token", event.Token))
	}

	return nil
}

// Close closes the Kafka consumer
func (c *JobbingConfigConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
