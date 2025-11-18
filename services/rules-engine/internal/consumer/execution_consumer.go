package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ExecutionResult represents execution result from Kafka
type ExecutionResult struct {
	ExecutionID   string    `json:"execution_id"`
	OrderID       string    `json:"order_id"`
	Status        string    `json:"status"`
	ExecutedPrice float64   `json:"executed_price"`
	ExecutedQty   int32     `json:"executed_quantity"`
	BrokerOrderID string    `json:"broker_order_id"`
	ExecutionTime time.Time `json:"execution_time"`
	ErrorMessage  string    `json:"error_message,omitempty"`
}

// ExecutionConsumer consumes execution results from Kafka
type ExecutionConsumer struct {
	reader     *kafka.Reader
	signalRepo *repository.TradeSignalRepository
	logger     *zap.Logger
}

// NewExecutionConsumer creates a new execution consumer
func NewExecutionConsumer(brokers []string, groupID string, signalRepo *repository.TradeSignalRepository, logger *zap.Logger) *ExecutionConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          "trade-executions",
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	logger.Info("Execution consumer initialized",
		zap.Strings("brokers", brokers),
		zap.String("topic", "trade-executions"),
		zap.String("group_id", groupID))

	return &ExecutionConsumer{
		reader:     reader,
		signalRepo: signalRepo,
		logger:     logger,
	}
}

// Start starts consuming execution results
func (c *ExecutionConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting execution consumer for trade-executions")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Execution consumer shutting down")
			return nil
		default:
			// Read message
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.logger.Error("Failed to fetch execution message", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			// Process message
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process execution message",
					zap.Error(err),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset))
				// Don't commit if processing failed
				continue
			}

			// Commit message
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit execution message", zap.Error(err))
			}
		}
	}
}

// processMessage processes a single execution result message
func (c *ExecutionConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	c.logger.Debug("Processing execution result message",
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset))

	// Parse execution result
	var result ExecutionResult
	if err := json.Unmarshal(msg.Value, &result); err != nil {
		return fmt.Errorf("failed to unmarshal execution result: %w", err)
	}

	c.logger.Info("Execution result received",
		zap.String("order_id", result.OrderID),
		zap.String("execution_id", result.ExecutionID),
		zap.String("status", result.Status),
		zap.String("broker_order_id", result.BrokerOrderID))

	// Update trade_signals table in PostgreSQL
	if c.signalRepo != nil {
		err := c.signalRepo.UpdateSignalStatus(
			ctx,
			result.OrderID,
			result.Status,
			result.ExecutedPrice,
			result.BrokerOrderID,
			result.ErrorMessage,
		)
		if err != nil {
			return fmt.Errorf("failed to update signal status: %w", err)
		}

		c.logger.Info("Trade signal status updated in PostgreSQL",
			zap.String("order_id", result.OrderID),
			zap.String("status", result.Status),
			zap.Float64("executed_price", result.ExecutedPrice))
	}

	return nil
}

// Close closes the Kafka consumer
func (c *ExecutionConsumer) Close() error {
	c.logger.Info("Closing execution consumer")
	return c.reader.Close()
}
