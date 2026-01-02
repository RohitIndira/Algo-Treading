package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// TradeSignalProcessor processes incoming trade signals
type TradeSignalProcessor interface {
	ProcessTradeSignal(ctx context.Context, signal *models.TradeSignal) error
}

// KafkaConsumer consumes trade signals from Kafka
type KafkaConsumer struct {
	reader           *kafka.Reader
	processor        TradeSignalProcessor
	logger           *zap.Logger
	retryAttempts    int
	initialRetryWait time.Duration
	maxRetryWait     time.Duration
}

// NewKafkaConsumer creates a new Kafka consumer for trade signals
func NewKafkaConsumer(brokers []string, groupID string, processor TradeSignalProcessor, logger *zap.Logger) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          "trade-signals",
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
		ReadBackoffMin: 100 * time.Millisecond,
		ReadBackoffMax: 1 * time.Second,
		MaxWait:        500 * time.Millisecond,
	})

	logger.Info("Kafka consumer initialized",
		zap.Strings("brokers", brokers),
		zap.String("topic", "trade-signals"),
		zap.String("group_id", groupID))

	return &KafkaConsumer{
		reader:           reader,
		processor:        processor,
		logger:           logger,
		retryAttempts:    0,
		initialRetryWait: 100 * time.Millisecond,
		maxRetryWait:     30 * time.Second,
	}
}

// Start starts consuming trade signals
func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer for trade-signals")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Kafka consumer shutting down")
			return nil
		default:
			// Read message
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					// Context cancelled, exit gracefully
					return nil
				}

				// Handle EOF gracefully - this is normal when no messages are available
				if errors.Is(err, io.EOF) {
					c.logger.Debug("No messages available, waiting for new messages")
					c.resetRetry()
					time.Sleep(500 * time.Millisecond)
					continue
				}

				// Handle other Kafka-specific errors with exponential backoff
				c.handleFetchError(ctx, err)
				continue
			}

			// Successfully fetched a message, reset retry counter
			c.resetRetry()

			// Process message
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process message",
					zap.Error(err),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset))
				// Don't commit if processing failed
				continue
			}

			// Commit message
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit message", zap.Error(err))
			}
		}
	}
}

// handleFetchError handles fetch errors with exponential backoff
func (c *KafkaConsumer) handleFetchError(ctx context.Context, err error) {
	c.retryAttempts++

	// Calculate backoff duration with exponential increase
	backoffDuration := c.initialRetryWait
	for i := 0; i < c.retryAttempts-1; i++ {
		backoffDuration *= 2
		if backoffDuration > c.maxRetryWait {
			backoffDuration = c.maxRetryWait
			break
		}
	}

	c.logger.Warn("Kafka fetch error, applying exponential backoff",
		zap.Error(err),
		zap.Int("attempt", c.retryAttempts),
		zap.Duration("backoff", backoffDuration))

	select {
	case <-time.After(backoffDuration):
		// Backoff completed, continue
	case <-ctx.Done():
		// Context cancelled during backoff
	}
}

// resetRetry resets the retry counter after successful message fetch
func (c *KafkaConsumer) resetRetry() {
	if c.retryAttempts > 0 {
		c.logger.Debug("Kafka connection recovered, resetting retry counter",
			zap.Int("previous_attempts", c.retryAttempts))
		c.retryAttempts = 0
	}
}

// processMessage processes a single Kafka message
func (c *KafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	c.logger.Debug("Processing trade signal message",
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset))

	// Parse trade signal
	var signal models.TradeSignal
	if err := json.Unmarshal(msg.Value, &signal); err != nil {
		return fmt.Errorf("failed to unmarshal trade signal: %w", err)
	}

	c.logger.Info("Trade signal received",
		zap.String("order_id", signal.OrderID),
		zap.String("user_id", signal.UserID),
		zap.String("symbol", signal.Symbol),
		zap.Int64("stock_code", signal.StockCode),
		zap.String("order_type", signal.OrderType),
		zap.Float64("price", signal.Price))

	// Process the signal
	if err := c.processor.ProcessTradeSignal(ctx, &signal); err != nil {
		return fmt.Errorf("failed to process trade signal: %w", err)
	}

	c.logger.Info("Trade signal processed successfully",
		zap.String("order_id", signal.OrderID))

	return nil
}

// Close closes the Kafka consumer
func (c *KafkaConsumer) Close() error {
	c.logger.Info("Closing Kafka consumer")
	return c.reader.Close()
}
