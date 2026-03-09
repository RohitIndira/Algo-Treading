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
		BatchSize:    10,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false, // Synchronous for reliability
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
func (p *KafkaPublisher) PublishTradeSignal(ctx context.Context, signal *models.TradeSignal) error {
	// Convert to JSON
	signalJSON, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("failed to marshal signal: %w", err)
	}

	// Create Kafka message
	msg := kafka.Message{
		Key:   []byte(signal.UserID),
		Value: signalJSON,
		Headers: []kafka.Header{
			{Key: "user_id", Value: []byte(signal.UserID)},
			{Key: "strategy_id", Value: []byte(signal.StrategyID)},
			{Key: "news_id", Value: []byte(signal.NewsID)},
			{Key: "trading_mode", Value: []byte(signal.TradingMode)},
			{Key: "timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		},
	}

	// Publish to Kafka
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	p.logger.Debug("Trade signal published to Kafka",
		zap.String("user_id", signal.UserID),
		zap.String("strategy_id", signal.StrategyID),
		zap.Int64("stock_code", signal.StockCode))

	return nil
}

// Close closes the Kafka writer
func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing Kafka publisher")
	return p.writer.Close()
}
