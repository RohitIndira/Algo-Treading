package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// KafkaPublisher publishes trade signals to Kafka
type KafkaPublisher struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewKafkaPublisher creates a new Kafka publisher
func NewKafkaPublisher(brokers []string, topic string, logger *zap.Logger) *KafkaPublisher {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,                       // flush each signal immediately — no batching delay
		BatchTimeout: 1 * time.Millisecond,    // near-zero wait before flush
		Async:        true,                    // non-blocking — avoids 50-500ms sync wait per write
		RequiredAcks: kafka.RequireOne,         // ack from leader only (faster than all replicas)
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				logger.Error("Kafka async write failed",
					zap.Error(err),
					zap.Int("message_count", len(messages)))
			}
		},
	}

	logger.Info("Kafka publisher initialized",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic))

	return &KafkaPublisher{
		writer: writer,
		logger: logger,
	}
}

// PublishTradeSignal publishes a trade signal to Kafka
func (p *KafkaPublisher) PublishTradeSignal(ctx context.Context, orderReq *models.OrderRequest) error {
	// Convert to JSON
	orderJSON, err := json.Marshal(orderReq)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	// Create Kafka message
	msg := kafka.Message{
		Key:   []byte(orderReq.OrderID),
		Value: orderJSON,
		Headers: []kafka.Header{
			{Key: "order_id", Value: []byte(orderReq.OrderID)},
			{Key: "user_id", Value: []byte(orderReq.UserID)},
			{Key: "strategy_id", Value: []byte(orderReq.StrategyID)},
			{Key: "event_id", Value: []byte(orderReq.EventID)},
			{Key: "order_type", Value: []byte(orderReq.OrderType)},
			{Key: "timestamp", Value: []byte(orderReq.Timestamp.Format(time.RFC3339))},
		},
	}

	// Publish to Kafka
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	p.logger.Debug("Trade signal published to Kafka",
		zap.String("order_id", orderReq.OrderID),
		zap.String("user_id", orderReq.UserID),
		zap.Int64("stock_code", orderReq.StockCode),
		zap.String("symbol", orderReq.Symbol))

	return nil
}

// Close closes the Kafka writer
func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing Kafka publisher")
	return p.writer.Close()
}
