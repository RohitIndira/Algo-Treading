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

// PublishAllocation publishes a portfolio allocation event to Kafka. This is
// used for tracking per-user allocation state for strategies like
// Cash 52-Week High.
func (p *KafkaPublisher) PublishAllocation(ctx context.Context, ev *models.PortfolioAllocationEvent) error {
	if ev == nil {
		return fmt.Errorf("allocation event is nil")
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("failed to marshal allocation event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(ev.UserID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "user_id", Value: []byte(ev.UserID)},
			{Key: "strategy_id", Value: []byte(ev.StrategyID)},
			{Key: "strategy_name", Value: []byte(ev.StrategyName)},
			{Key: "date", Value: []byte(ev.Date)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write allocation event to Kafka: %w", err)
	}

	p.logger.Debug("Portfolio allocation event published to Kafka",
		zap.String("user_id", ev.UserID),
		zap.String("strategy_id", ev.StrategyID),
		zap.Int("total_positions", ev.TotalPositions))

	return nil
}

// PublishRealtimePortfolio publishes a realtime marked-to-market portfolio
// snapshot to Kafka. The topic is determined by the writer's configuration
// (typically cfg.PortfolioRealtimeTopic).
func (p *KafkaPublisher) PublishRealtimePortfolio(ctx context.Context, ev *models.RealtimePortfolioEvent) error {
	if ev == nil {
		return fmt.Errorf("realtime portfolio event is nil")
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("failed to marshal realtime portfolio event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(ev.UserID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "user_id", Value: []byte(ev.UserID)},
			{Key: "strategy_id", Value: []byte(ev.StrategyID)},
			{Key: "strategy_name", Value: []byte(ev.StrategyName)},
			{Key: "mode", Value: []byte(ev.Mode)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write realtime portfolio event to Kafka: %w", err)
	}

	p.logger.Debug("Realtime portfolio event published to Kafka",
		zap.String("user_id", ev.UserID),
		zap.String("strategy_id", ev.StrategyID),
		zap.Int("positions", len(ev.Positions)))

	return nil
}

// Close closes the Kafka writer
func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing Kafka publisher")
	return p.writer.Close()
}
