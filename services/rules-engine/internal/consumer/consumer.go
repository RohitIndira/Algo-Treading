package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/config"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Consumer consumes market events from Kafka
type Consumer struct {
	reader  *kafka.Reader
	handler EventHandler
	logger  *zap.Logger
	stats   *models.MatchingStats
}

// EventHandler defines the interface for handling events
type EventHandler interface {
	HandleEvent(ctx context.Context, event *models.MarketEvent) error
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg *config.KafkaConfig, handler EventHandler, stats *models.MatchingStats, logger *zap.Logger) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       cfg.MaxBytes,
		CommitInterval: cfg.CommitInterval,
		StartOffset:    kafka.LastOffset,
		MaxWait:        1 * time.Second,
	})

	logger.Info("Kafka consumer created",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.Topic),
		zap.String("group", cfg.ConsumerGroup))

	return &Consumer{
		reader:  reader,
		handler: handler,
		logger:  logger,
		stats:   stats,
	}, nil
}

// Start starts consuming messages
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Kafka consumer stopped")
			return ctx.Err()
		default:
			if err := c.processMessage(ctx); err != nil {
				c.logger.Error("Failed to process message", zap.Error(err))
				c.stats.IncrementKafkaErrors()
			}
		}
	}
}

// processMessage processes a single Kafka message
func (c *Consumer) processMessage(ctx context.Context) error {
	// Read message with timeout
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch message: %w", err)
	}

	c.logger.Debug("Received Kafka message",
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.Time("time", msg.Time))

	// First, try to unmarshal as MongoDB event
	var mongoEvent models.MongoDBEvent
	if err := json.Unmarshal(msg.Value, &mongoEvent); err != nil {
		c.logger.Error("Failed to unmarshal MongoDB event",
			zap.Error(err),
			zap.ByteString("message", msg.Value))

		// Commit the message to skip it
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("Failed to commit malformed message", zap.Error(commitErr))
		}

		return fmt.Errorf("failed to unmarshal MongoDB event: %w", err)
	}

	// Convert MongoDB event to MarketEvent
	event, err := mongoEvent.ToMarketEvent()
	if err != nil {
		c.logger.Error("Failed to convert MongoDB event",
			zap.Error(err),
			zap.String("news_id", mongoEvent.NewsID))

		// Commit to skip unconvertible events
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("Failed to commit unconvertible message", zap.Error(commitErr))
		}

		return fmt.Errorf("failed to convert event: %w", err)
	}

	// Validate converted event
	if err := event.Validate(); err != nil {
		c.logger.Warn("Invalid event received",
			zap.Error(err),
			zap.String("event_id", event.EventID))

		// Commit to skip invalid events
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("Failed to commit invalid message", zap.Error(commitErr))
		}

		return fmt.Errorf("invalid event: %w", err)
	}

	// Handle event
	startTime := time.Now()
	if err := c.handler.HandleEvent(ctx, event); err != nil {
		c.logger.Error("Failed to handle event",
			zap.Error(err),
			zap.String("event_id", event.EventID))
		return fmt.Errorf("failed to handle event: %w", err)
	}

	// Commit message
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit message", zap.Error(err))
		return fmt.Errorf("failed to commit message: %w", err)
	}

	duration := time.Since(startTime)
	c.logger.Debug("Event processed successfully",
		zap.String("event_id", event.EventID),
		zap.Duration("duration", duration))

	c.stats.IncrementEventsProcessed()

	return nil
}

// Close closes the Kafka consumer
func (c *Consumer) Close() error {
	c.logger.Info("Closing Kafka consumer")
	return c.reader.Close()
}

// GetStats returns reader statistics
func (c *Consumer) GetStats() kafka.ReaderStats {
	return c.reader.Stats()
}
