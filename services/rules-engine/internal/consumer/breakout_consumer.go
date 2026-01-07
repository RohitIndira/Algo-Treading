package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// BreakoutHandler defines the interface for handling 52-week breakout events.
type BreakoutHandler interface {
	HandleBreakout(ctx context.Context, ev *models.Breakout52WEvent) error
}

// BreakoutConsumer consumes 52-week breakout events from Kafka.
type BreakoutConsumer struct {
	reader  *kafka.Reader
	handler BreakoutHandler
	logger  *zap.Logger
}

// NewBreakoutConsumer creates a new Kafka consumer for 52-week breakouts.
func NewBreakoutConsumer(brokers []string, topic, groupID string, handler BreakoutHandler, logger *zap.Logger) (*BreakoutConsumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("breakout topic cannot be empty")
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers cannot be empty")
	}
	if groupID == "" {
		groupID = "rules-engine-cash52w-group"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
		MaxWait:        1 * time.Second,
	})

	logger.Info("52w-breakout Kafka consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group", groupID))

	return &BreakoutConsumer{
		reader:  reader,
		handler: handler,
		logger:  logger,
	}, nil
}

// Start begins consuming breakout messages until context is cancelled.
func (c *BreakoutConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting 52w-breakout Kafka consumer")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("52w-breakout Kafka consumer stopped")
			return ctx.Err()
		default:
			if err := c.processMessage(ctx); err != nil {
				c.logger.Error("Failed to process 52w-breakout message", zap.Error(err))
			}
		}
	}
}

func (c *BreakoutConsumer) processMessage(ctx context.Context) error {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch 52w-breakout message: %w", err)
	}

	c.logger.Debug("Received 52w-breakout Kafka message",
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.Time("time", msg.Time))

	var ev models.Breakout52WEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		c.logger.Error("Failed to unmarshal Breakout52WEvent",
			zap.Error(err),
			zap.ByteString("message", msg.Value))
		// commit to skip malformed messages
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("Failed to commit malformed 52w-breakout message", zap.Error(commitErr))
		}
		return fmt.Errorf("failed to unmarshal Breakout52WEvent: %w", err)
	}

	if err := c.handler.HandleBreakout(ctx, &ev); err != nil {
		c.logger.Error("Failed to handle Breakout52WEvent", zap.Error(err))
		return fmt.Errorf("failed to handle Breakout52WEvent: %w", err)
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit 52w-breakout message", zap.Error(err))
		return fmt.Errorf("failed to commit 52w-breakout message: %w", err)
	}

	return nil
}

// Close closes the Kafka consumer.
func (c *BreakoutConsumer) Close() error {
	c.logger.Info("Closing 52w-breakout Kafka consumer")
	return c.reader.Close()
}
