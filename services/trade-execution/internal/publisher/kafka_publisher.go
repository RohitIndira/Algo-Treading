package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// KafkaPublisher publishes execution results and order updates to Kafka
type KafkaPublisher struct {
	executionWriter *kafka.Writer
	updatesWriter   *kafka.Writer
	logger          *zap.Logger
}

// NewKafkaPublisher creates a new Kafka publisher
func NewKafkaPublisher(brokers []string, logger *zap.Logger) *KafkaPublisher {
	// Writer for trade-executions topic — async to avoid blocking the order
	// execution hot path. Errors are logged via Completion callback.
	executionWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "trade-executions",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    10,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				logger.Error("Async Kafka write failed (trade-executions)",
					zap.Int("messages", len(messages)),
					zap.Error(err))
			}
		},
	}

	// Writer for order-updates topic — async for the same reason.
	updatesWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "order-updates",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    10,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				logger.Error("Async Kafka write failed (order-updates)",
					zap.Int("messages", len(messages)),
					zap.Error(err))
			}
		},
	}

	logger.Info("Kafka publishers initialized",
		zap.Strings("brokers", brokers),
		zap.Strings("topics", []string{"trade-executions", "order-updates"}))

	return &KafkaPublisher{
		executionWriter: executionWriter,
		updatesWriter:   updatesWriter,
		logger:          logger,
	}
}

// PublishExecutionResult publishes execution result to trade-executions topic
func (p *KafkaPublisher) PublishExecutionResult(ctx context.Context, result *models.ExecutionResult) error {
	// Convert to JSON
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal execution result: %w", err)
	}

	// Create Kafka message
	msg := kafka.Message{
		Key:   []byte(result.OrderID),
		Value: resultJSON,
		Headers: []kafka.Header{
			{Key: "order_id", Value: []byte(result.OrderID)},
			{Key: "user_id", Value: []byte(result.UserID)},
			{Key: "status", Value: []byte(result.Status)},
			{Key: "exchange", Value: []byte(result.Exchange)},
			{Key: "timestamp", Value: []byte(result.ExecutionTime.Format(time.RFC3339))},
		},
	}

	// Publish to Kafka
	if err := p.executionWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write execution result to Kafka: %w", err)
	}

	p.logger.Info("Execution result published to Kafka",
		zap.String("topic", "trade-executions"),
		zap.String("order_id", result.OrderID),
		zap.String("status", result.Status),
		zap.Float64("executed_price", result.ExecutedPrice))

	return nil
}

// PublishOrderUpdate publishes order update to order-updates topic
func (p *KafkaPublisher) PublishOrderUpdate(ctx context.Context, update *models.OrderUpdate) error {
	// Convert to JSON
	updateJSON, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal order update: %w", err)
	}

	// Create Kafka message
	msg := kafka.Message{
		Key:   []byte(update.OrderID),
		Value: updateJSON,
		Headers: []kafka.Header{
			{Key: "update_type", Value: []byte(update.UpdateType)},
			{Key: "user_id", Value: []byte(update.UserID)},
			{Key: "priority", Value: []byte(update.Priority)},
			{Key: "timestamp", Value: []byte(update.CreatedAt.Format(time.RFC3339))},
		},
	}

	// Add notification channel headers
	if update.NotificationChannels.Push {
		msg.Headers = append(msg.Headers, kafka.Header{Key: "notification_channel", Value: []byte("push")})
	}
	if update.NotificationChannels.Email {
		msg.Headers = append(msg.Headers, kafka.Header{Key: "notification_channel", Value: []byte("email")})
	}
	if update.NotificationChannels.InApp {
		msg.Headers = append(msg.Headers, kafka.Header{Key: "notification_channel", Value: []byte("in_app")})
	}

	// Publish to Kafka
	if err := p.updatesWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write order update to Kafka: %w", err)
	}

	p.logger.Info("Order update published to Kafka",
		zap.String("topic", "order-updates"),
		zap.String("order_id", update.OrderID),
		zap.String("update_type", update.UpdateType),
		zap.String("message", update.Message))

	return nil
}

// Close closes both Kafka writers
func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing Kafka publishers")

	if err := p.executionWriter.Close(); err != nil {
		p.logger.Error("Failed to close execution writer", zap.Error(err))
	}

	if err := p.updatesWriter.Close(); err != nil {
		p.logger.Error("Failed to close updates writer", zap.Error(err))
	}

	return nil
}
