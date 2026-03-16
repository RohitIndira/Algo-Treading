package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/workerpool"
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
	pool      *workerpool.Pool
	workers   int
}

// NewKafkaConsumer creates a new Kafka consumer for trade signals.
// workers controls the maximum number of messages processed concurrently.
func NewKafkaConsumer(brokers []string, groupID string, processor TradeSignalProcessor, logger *zap.Logger, workers int) *KafkaConsumer {
	if workers <= 0 {
		workers = 100
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          "trade-signals",
		GroupID:        groupID,
		MinBytes:       10e3,  // 10KB — allows micro-batching at the broker level
		MaxBytes:       10e6,  // 10MB
		CommitInterval: 100 * time.Millisecond,
		StartOffset:    kafka.LastOffset,
	})

	pool := workerpool.New(workers, workers*4)

	logger.Info("Kafka consumer initialized",
		zap.Strings("brokers", brokers),
		zap.String("topic", "trade-signals"),
		zap.String("group_id", groupID),
		zap.Int("workers", workers))

	return &KafkaConsumer{
		reader:    reader,
		processor: processor,
		logger:    logger,
		pool:      pool,
		workers:   workers,
	}
}

// Start consumes trade signals with bounded concurrency via the worker pool.
// Fetch errors use exponential backoff (100ms → 30s).
func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer for trade-signals", zap.Int("workers", c.workers))

	const (
		initBackoff = 100 * time.Millisecond
		maxBackoff  = 30 * time.Second
	)
	backoff := initBackoff

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Kafka consumer shutting down — draining worker pool")
			c.pool.Stop()
			c.logger.Info("Kafka consumer worker pool drained")
			return nil
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.pool.Stop()
				return nil
			}
			c.logger.Error("Failed to fetch message", zap.Error(err), zap.Duration("retry_in", backoff))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				c.pool.Stop()
				return nil
			}
			backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
			continue
		}
		backoff = initBackoff // reset on successful fetch

		metrics.KafkaMessagesReceived.WithLabelValues("trade-signals").Inc()
		metrics.KafkaConsumerLag.WithLabelValues("trade-signals", strconv.Itoa(msg.Partition)).Set(float64(msg.HighWaterMark - msg.Offset))

		// Update worker pool metrics
		metrics.WorkerPoolActive.Set(float64(c.pool.Active()))
		metrics.WorkerPoolQueued.Set(float64(c.pool.Queued()))

		// Submit to bounded worker pool (blocks if queue is full).
		c.pool.Submit(func() {
			if err := c.processMessage(ctx, msg); err != nil {
				metrics.KafkaProcessingErrors.WithLabelValues("trade-signals").Inc()
				c.logger.Error("Failed to process message",
					zap.Error(err),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset))
				return // skip commit on failure
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit message", zap.Error(err))
			}
		})
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

// Stop drains the worker pool (blocks until in-flight tasks complete).
func (c *KafkaConsumer) Stop() {
	c.pool.Stop()
}

// Close closes the Kafka consumer
func (c *KafkaConsumer) Close() error {
	c.logger.Info("Closing Kafka consumer")
	return c.reader.Close()
}
