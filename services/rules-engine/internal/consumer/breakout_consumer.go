package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
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
	reader      *kafka.Reader
	handler     BreakoutHandler
	logger      *zap.Logger
	workerCount int
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
		// Use a versioned consumer group so that when this behaviour was
		// introduced (starting from earliest offsets with same-day filtering),
		// we can safely re-read the topic and pick up same-day backlog.
		groupID = "rules-engine-cash52w-group-v2"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		CommitInterval: 1 * time.Second,
		// Start from the earliest offsets for this consumer group so that
		// on a fresh deployment we can process same-day backlog of breakouts
		// for users who enable the strategy later in the day. Older
		// breakouts are filtered out by date in the engine.
		StartOffset: kafka.FirstOffset,
		MaxWait:     1 * time.Second,
	})

	logger.Info("52w-breakout Kafka consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group", groupID))

	return &BreakoutConsumer{
		reader:      reader,
		handler:     handler,
		logger:      logger,
		workerCount: 10, // Default: 10 concurrent workers
	}, nil
}

// Start begins consuming breakout messages until context is cancelled.
// Uses a worker pool for concurrent processing of multiple events.
func (c *BreakoutConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting 52w-breakout Kafka consumer with worker pool",
		zap.Int("workers", c.workerCount))

	// Create a buffered channel for message distribution
	messageChan := make(chan kafka.Message, c.workerCount*2)

	// WaitGroup to track worker goroutines
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < c.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, workerID, messageChan)
		}(i)
	}

	c.logger.Info("52w-breakout worker pool started", zap.Int("workers", c.workerCount))

	// Main loop: fetch messages and distribute to workers
	go func() {
		for {
			select {
			case <-ctx.Done():
				c.logger.Info("52w-breakout message fetcher stopping")
				close(messageChan)
				return
			default:
				msg, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						// Context cancelled, exit gracefully
						close(messageChan)
						return
					}
					c.logger.Error("Failed to fetch 52w-breakout message", zap.Error(err))
					time.Sleep(time.Second)
					continue
				}

				// Send message to worker pool
				select {
				case messageChan <- msg:
					// Message sent to worker
				case <-ctx.Done():
					close(messageChan)
					return
				}
			}
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	c.logger.Info("52w-breakout Kafka consumer stopping, waiting for workers to finish")

	// Wait for all workers to complete
	wg.Wait()

	c.logger.Info("52w-breakout Kafka consumer stopped")
	return ctx.Err()
}

// worker processes messages from the message channel concurrently
func (c *BreakoutConsumer) worker(ctx context.Context, workerID int, messageChan <-chan kafka.Message) {
	c.logger.Debug("52w-breakout worker started", zap.Int("worker_id", workerID))

	for msg := range messageChan {
		if err := c.processMessage(ctx, msg); err != nil {
			c.logger.Error("Failed to process 52w-breakout message",
				zap.Int("worker_id", workerID),
				zap.Error(err))
		}
	}

	c.logger.Debug("52w-breakout worker stopped", zap.Int("worker_id", workerID))
}

func (c *BreakoutConsumer) processMessage(ctx context.Context, msg kafka.Message) (err error) {
	// Panic recovery to prevent worker death
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("PANIC in 52w-breakout message processing",
				zap.Any("panic", r),
				zap.Stack("stack"))
			err = fmt.Errorf("panic in 52w-breakout processing: %v", r)
		}
	}()

	c.logger.Debug("Processing 52w-breakout Kafka message",
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

	c.logger.Info("Processing 52w-breakout event",
		zap.String("symbol", ev.Symbol),
		zap.String("token", ev.Token),
		zap.Float64("ltp", ev.LTP))

	if err := c.handler.HandleBreakout(ctx, &ev); err != nil {
		c.logger.Error("Failed to handle Breakout52WEvent",
			zap.Error(err),
			zap.String("symbol", ev.Symbol))
		// Commit even on error to move forward
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("Failed to commit failed 52w-breakout message", zap.Error(commitErr))
		}
		return fmt.Errorf("failed to handle Breakout52WEvent: %w", err)
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit 52w-breakout message", zap.Error(err))
		return fmt.Errorf("failed to commit 52w-breakout message: %w", err)
	}

	c.logger.Debug("52w-breakout event processed successfully",
		zap.String("symbol", ev.Symbol))
	return nil
}

// Close closes the Kafka consumer.
func (c *BreakoutConsumer) Close() error {
	c.logger.Info("Closing 52w-breakout Kafka consumer")
	return c.reader.Close()
}
