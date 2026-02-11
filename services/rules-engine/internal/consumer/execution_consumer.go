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

// OrderFillHandler defines interface for handling order fill notifications
type OrderFillHandler interface {
	HandleOrderFill(orderID string, userID string, token string, side string, executedQty int32, executedPrice float64) error
}

// ExecutionResult represents execution result from Kafka
type ExecutionResult struct {
	ExecutionID   string    `json:"execution_id"`
	OrderID       string    `json:"order_id"`
	UserID        string    `json:"user_id"` // Added for jobbing engine
	Token         string    `json:"token"`   // Added for jobbing engine
	Side          string    `json:"side"`    // Added for jobbing engine (BUY/SELL)
	Status        string    `json:"status"`
	ExecutedPrice float64   `json:"executed_price"`
	ExecutedQty   int32     `json:"executed_quantity"`
	BrokerOrderID string    `json:"broker_order_id"`
	ExecutionTime time.Time `json:"execution_time"`
	ErrorMessage  string    `json:"error_message,omitempty"`
}

// ExecutionConsumer consumes execution results from Kafka
type ExecutionConsumer struct {
	reader           *kafka.Reader
	signalRepo       *repository.TradeSignalRepository
	orderFillHandler OrderFillHandler
	logger           *zap.Logger
}

// NewExecutionConsumer creates a new execution consumer
func NewExecutionConsumer(brokers []string, groupID string, signalRepo *repository.TradeSignalRepository, orderFillHandler OrderFillHandler, logger *zap.Logger) *ExecutionConsumer {
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
		reader:           reader,
		signalRepo:       signalRepo,
		orderFillHandler: orderFillHandler,
		logger:           logger,
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

	// Notify order fill handler (e.g., jobbing engine) if execution was successful
	if c.orderFillHandler != nil && result.Status == "COMPLETE" && result.ExecutedQty > 0 {
		err := c.orderFillHandler.HandleOrderFill(
			result.OrderID,
			result.UserID,
			result.Token,
			result.Side,
			result.ExecutedQty,
			result.ExecutedPrice,
		)
		if err != nil {
			c.logger.Error("Failed to notify order fill handler",
				zap.Error(err),
				zap.String("order_id", result.OrderID),
				zap.String("user_id", result.UserID),
				zap.String("token", result.Token))
			// Don't return error - this is not critical for execution processing
		} else {
			c.logger.Info("Order fill handler notified successfully",
				zap.String("order_id", result.OrderID),
				zap.String("user_id", result.UserID),
				zap.String("token", result.Token),
				zap.String("side", result.Side),
				zap.Int32("executed_qty", result.ExecutedQty),
				zap.Float64("executed_price", result.ExecutedPrice))
		}
	}

	return nil
}

// Close closes the Kafka consumer
func (c *ExecutionConsumer) Close() error {
	c.logger.Info("Closing execution consumer")
	return c.reader.Close()
}
