package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
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
	reader    *kafka.Reader
	processor TradeSignalProcessor
	logger    *zap.Logger
	workers   int
}

// NewKafkaConsumer creates a new Kafka consumer for trade signals.
// workers controls the maximum number of messages processed concurrently.
func NewKafkaConsumer(brokers []string, groupID string, processor TradeSignalProcessor, logger *zap.Logger, workers int) *KafkaConsumer {
	if workers <= 0 {
		workers = 50
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          "trade-signals",
		GroupID:        groupID,
		MinBytes:       10e3,  // 10KB — allows micro-batching at the broker level
		MaxBytes:       10e6,  // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	logger.Info("Kafka consumer initialized",
		zap.Strings("brokers", brokers),
		zap.String("topic", "trade-signals"),
		zap.String("group_id", groupID),
		zap.Int("workers", workers))

	return &KafkaConsumer{
		reader:    reader,
		processor: processor,
		logger:    logger,
		workers:   workers,
	}
}

// Start consumes trade signals with bounded concurrency.
// Up to c.workers messages are processed in parallel; fetch errors use
// exponential backoff (100ms → 30s) instead of a fixed 1s sleep.
func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer for trade-signals", zap.Int("workers", c.workers))

	sem := make(chan struct{}, c.workers)
	var wg sync.WaitGroup

	const (
		initBackoff = 100 * time.Millisecond
		maxBackoff  = 30 * time.Second
	)
	backoff := initBackoff

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			c.logger.Info("Kafka consumer shutting down")
			return nil
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			c.logger.Error("Failed to fetch message", zap.Error(err), zap.Duration("retry_in", backoff))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				wg.Wait()
				return nil
			}
			backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
			continue
		}
		backoff = initBackoff // reset on successful fetch

		// Acquire a worker slot (blocks when all workers are busy).
		sem <- struct{}{}
		wg.Add(1)
		go func(m kafka.Message) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := c.processMessage(ctx, m); err != nil {
				c.logger.Error("Failed to process message",
					zap.Error(err),
					zap.String("topic", m.Topic),
					zap.Int("partition", m.Partition),
					zap.Int64("offset", m.Offset))
				return // skip commit on failure
			}

			if err := c.reader.CommitMessages(ctx, m); err != nil {
				c.logger.Error("Failed to commit message", zap.Error(err))
			}
		}(msg)
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
		zap.Float64("price", signal.Price),
		zap.String("trading_mode", signal.TradingMode))

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
