package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// RabbitMQPublisher publishes orders to RabbitMQ for odin-api-wrapper
type RabbitMQPublisher struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	exchange    string
	routingKey  string
	logger      *zap.Logger
	confirmChan chan amqp.Confirmation
}

// OrderMessage represents the order message format expected by odin-api-wrapper
type OrderMessage struct {
	OrderID    string  `json:"order_id"`
	UserID     string  `json:"user_id"`
	StrategyID string  `json:"strategy_id"`
	EventID    string  `json:"event_id,omitempty"`
	Token      int64   `json:"token"` // Stock code
	Symbol     string  `json:"symbol"`
	Exchange   string  `json:"exchange"`
	OrderType  string  `json:"order_type"` // MARKET or LIMIT
	OrderSide  string  `json:"order_side"` // BUY or SELL
	Quantity   int32   `json:"quantity"`
	Price      float64 `json:"price"`
	StopLoss   float64 `json:"stop_loss,omitempty"`
	TakeProfit float64 `json:"take_profit,omitempty"`
	Validity   string  `json:"validity"`
	Timestamp  string  `json:"timestamp"`
}

// NewRabbitMQPublisher creates a new RabbitMQ publisher
func NewRabbitMQPublisher(url, exchange, routingKey string, logger *zap.Logger) (*RabbitMQPublisher, error) {
	// Connect to RabbitMQ
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Create channel
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	// Enable publisher confirms for reliability
	if err := channel.Confirm(false); err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	// Declare exchange (topic)
	err = channel.ExchangeDeclare(
		exchange,
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Create confirmation channel
	confirmChan := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	logger.Info("RabbitMQ publisher initialized",
		zap.String("url", url),
		zap.String("exchange", exchange),
		zap.String("routing_key", routingKey))

	return &RabbitMQPublisher{
		conn:        conn,
		channel:     channel,
		exchange:    exchange,
		routingKey:  routingKey,
		logger:      logger,
		confirmChan: confirmChan,
	}, nil
}

// PublishOrder publishes an order to RabbitMQ for odin-api-wrapper to consume
func (p *RabbitMQPublisher) PublishOrder(ctx context.Context, order *models.Order) error {
	// Convert Order to OrderMessage format expected by odin-api-wrapper
	orderMsg := OrderMessage{
		OrderID:    order.OrderID.String(),
		UserID:     order.UserID,
		StrategyID: order.StrategyID,
		EventID:    order.EventID.String(),
		Token:      order.StockCode,
		Symbol:     order.Symbol,
		Exchange:   string(order.Exchange),
		OrderType:  string(order.OrderType),
		OrderSide:  string(order.OrderSide),
		Quantity:   order.Quantity,
		Price:      *order.Price,
		Validity:   order.Validity,
		Timestamp:  time.Now().Format(time.RFC3339),
	}

	if order.StopLoss != nil {
		orderMsg.StopLoss = *order.StopLoss
	}
	if order.TakeProfit != nil {
		orderMsg.TakeProfit = *order.TakeProfit
	}

	// Marshal to JSON
	body, err := json.Marshal(orderMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal order message: %w", err)
	}

	// Publish message
	err = p.channel.Publish(
		p.exchange,   // exchange
		p.routingKey, // routing key
		true,         // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // persistent
			Timestamp:    time.Now(),
			MessageId:    order.OrderID.String(),
			Body:         body,
			Headers: amqp.Table{
				"order_id":  order.OrderID.String(),
				"user_id":   order.UserID,
				"symbol":    order.Symbol,
				"exchange":  string(order.Exchange),
				"timestamp": time.Now().Format(time.RFC3339),
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish order to RabbitMQ: %w", err)
	}

	// Wait for confirmation with timeout
	select {
	case confirm := <-p.confirmChan:
		if !confirm.Ack {
			return fmt.Errorf("message not acknowledged by broker")
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for publish confirmation")
	}

	p.logger.Info("Order published to RabbitMQ",
		zap.String("exchange", p.exchange),
		zap.String("routing_key", p.routingKey),
		zap.String("order_id", order.OrderID.String()),
		zap.String("symbol", order.Symbol),
		zap.Int32("quantity", order.Quantity))

	return nil
}

// Close closes the RabbitMQ connection
func (p *RabbitMQPublisher) Close() error {
	p.logger.Info("Closing RabbitMQ publisher")

	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			p.logger.Error("Failed to close RabbitMQ channel", zap.Error(err))
		}
	}

	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			p.logger.Error("Failed to close RabbitMQ connection", zap.Error(err))
		}
	}

	return nil
}
