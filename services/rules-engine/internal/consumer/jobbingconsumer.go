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

// JobbingHandler defines the interface for handling jobbing market depth events.
type JobbingHandler interface {
	HandleJobbing(ctx context.Context, ev *models.JobbingMarketDepthEvent) error
}

// JobbingConsumer consumes real-time market depth events from Kafka for jobbing strategy.
type JobbingConsumer struct {
	reader  *kafka.Reader
	handler JobbingHandler
	logger  *zap.Logger
}

// NewJobbingConsumer creates a new Kafka consumer for jobbing strategy.
func NewJobbingConsumer(brokers []string, topic, groupID string, handler JobbingHandler, logger *zap.Logger) (*JobbingConsumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("jobbing topic cannot be empty")
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers cannot be empty")
	}
	if groupID == "" {
		// Use a versioned consumer group for jobbing strategy
		groupID = "rules-engine-jobbing-group-v1"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		CommitInterval: 1 * time.Second,
		// For jobbing strategy, we typically want latest data (real-time),
		// not historical backlog. Start from latest offset.
		StartOffset: kafka.LastOffset,
		MaxWait:     500 * time.Millisecond, // Lower wait time for real-time trading
	})

	logger.Info("Jobbing Kafka consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group", groupID))

	return &JobbingConsumer{
		reader:  reader,
		handler: handler,
		logger:  logger,
	}, nil
}

// Start begins consuming jobbing market depth messages until context is cancelled.
func (c *JobbingConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Jobbing Kafka consumer")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Jobbing Kafka consumer stopped")
			return ctx.Err()
		default:
			if err := c.processMessage(ctx); err != nil {
				c.logger.Error("Failed to process jobbing message", zap.Error(err))
				// Continue processing on errors to avoid stopping the consumer
			}
		}
	}
}

func (c *JobbingConsumer) processMessage(ctx context.Context) error {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch jobbing message: %w", err)
	}

	c.logger.Debug("Received jobbing Kafka message",
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.Time("time", msg.Time),
		zap.String("key", string(msg.Key)))

	var ev models.JobbingMarketDepthEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		c.logger.Error("Failed to unmarshal JobbingMarketDepthEvent",
			zap.Error(err),
			zap.ByteString("message", msg.Value))
		// Commit to skip malformed messages
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("Failed to commit malformed jobbing message", zap.Error(commitErr))
		}
		return fmt.Errorf("failed to unmarshal JobbingMarketDepthEvent: %w", err)
	}

	// Log key market metrics for monitoring
	c.logger.Debug("Processing jobbing event",
		zap.String("symbol", ev.StockData.Symbol),
		zap.String("exchange", ev.StockData.Exchange),
		zap.Float64("ltp", ev.MarketData.LastTradedPrice),
		zap.Float64("spread_pct", ev.MarketData.DepthMetrics.SpreadPct),
		zap.Float64("bid_ask_ratio", ev.MarketData.DepthMetrics.BidAskRatio),
		zap.String("ltp_position", ev.MarketData.DepthMetrics.LTPPositionType))

	if err := c.handler.HandleJobbing(ctx, &ev); err != nil {
		c.logger.Error("Failed to handle JobbingMarketDepthEvent",
			zap.Error(err),
			zap.String("symbol", ev.StockData.Symbol))
		return fmt.Errorf("failed to handle JobbingMarketDepthEvent: %w", err)
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit jobbing message", zap.Error(err))
		return fmt.Errorf("failed to commit jobbing message: %w", err)
	}

	return nil
}

// Close closes the Kafka consumer.
func (c *JobbingConsumer) Close() error {
	c.logger.Info("Closing Jobbing Kafka consumer")
	return c.reader.Close()
}
