package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/config"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

// Publisher publishes orders to RabbitMQ
type Publisher struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	cfg            *config.RabbitMQConfig
	logger         *zap.Logger
	circuitBreaker *CircuitBreaker
	mu             sync.RWMutex
	closed         bool
	reconnecting   bool
}

// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
	maxFailures int
	timeout     time.Duration
	failures    int
	lastFailure time.Time
	state       string // "closed", "open", "half-open"
	mu          sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures: maxFailures,
		timeout:     timeout,
		state:       "closed",
	}
}

// Call executes a function with circuit breaker protection
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.RLock()
	state := cb.state
	cb.mu.RUnlock()

	if state == "open" {
		cb.mu.RLock()
		timeSinceFailure := time.Since(cb.lastFailure)
		cb.mu.RUnlock()

		if timeSinceFailure < cb.timeout {
			return fmt.Errorf("circuit breaker is open")
		}

		// Try half-open state
		cb.mu.Lock()
		cb.state = "half-open"
		cb.mu.Unlock()
	}

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()

		if cb.failures >= cb.maxFailures {
			cb.state = "open"
		}
		return err
	}

	// Success - reset circuit breaker
	cb.failures = 0
	cb.state = "closed"
	return nil
}

// GetState returns the current circuit breaker state
func (cb *CircuitBreaker) GetState() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// NewPublisher creates a new RabbitMQ publisher
func NewPublisher(cfg *config.RabbitMQConfig, logger *zap.Logger) (*Publisher, error) {
	p := &Publisher{
		cfg:    cfg,
		logger: logger,
		circuitBreaker: NewCircuitBreaker(
			5,              // max failures
			60*time.Second, // timeout
		),
	}

	if err := p.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	logger.Info("RabbitMQ publisher initialized",
		zap.String("exchange", cfg.Exchange),
		zap.String("queue", cfg.Queue))

	// Start connection monitor
	go p.monitorConnection()

	return p, nil
}

// connect establishes connection to RabbitMQ
func (p *Publisher) connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close existing connection if any
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}

	// Connect to RabbitMQ
	conn, err := amqp.Dial(p.cfg.URL)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Create channel
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare exchange
	err = ch.ExchangeDeclare(
		p.cfg.Exchange,     // name
		p.cfg.ExchangeType, // type
		p.cfg.Durable,      // durable
		p.cfg.AutoDelete,   // auto-deleted
		false,              // internal
		p.cfg.NoWait,       // no-wait
		nil,                // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Declare queue
	_, err = ch.QueueDeclare(
		p.cfg.Queue,      // name
		p.cfg.Durable,    // durable
		p.cfg.AutoDelete, // delete when unused
		p.cfg.Exclusive,  // exclusive
		p.cfg.NoWait,     // no-wait
		nil,              // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind queue to exchange
	err = ch.QueueBind(
		p.cfg.Queue,      // queue name
		p.cfg.RoutingKey, // routing key
		p.cfg.Exchange,   // exchange
		p.cfg.NoWait,     // no-wait
		nil,              // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	p.conn = conn
	p.channel = ch
	p.reconnecting = false

	p.logger.Info("Connected to RabbitMQ")
	return nil
}

// monitorConnection monitors the RabbitMQ connection
func (p *Publisher) monitorConnection() {
	for {
		p.mu.RLock()
		closed := p.closed
		p.mu.RUnlock()

		if closed {
			return
		}

		p.mu.RLock()
		connClosed := p.conn == nil || p.conn.IsClosed()
		p.mu.RUnlock()

		if connClosed && !p.reconnecting {
			p.logger.Warn("RabbitMQ connection lost, attempting to reconnect")
			p.reconnect()
		}

		time.Sleep(5 * time.Second)
	}
}

// reconnect attempts to reconnect to RabbitMQ
func (p *Publisher) reconnect() {
	p.mu.Lock()
	if p.reconnecting {
		p.mu.Unlock()
		return
	}
	p.reconnecting = true
	p.mu.Unlock()

	for attempt := 1; attempt <= p.cfg.MaxReconnectAttempts; attempt++ {
		p.logger.Info("Reconnecting to RabbitMQ",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", p.cfg.MaxReconnectAttempts))

		if err := p.connect(); err != nil {
			p.logger.Error("Failed to reconnect",
				zap.Error(err),
				zap.Int("attempt", attempt))
			time.Sleep(p.cfg.ReconnectDelay)
			continue
		}

		p.logger.Info("Successfully reconnected to RabbitMQ")
		return
	}

	p.logger.Error("Failed to reconnect after max attempts")
}

// PublishOrder publishes an order to RabbitMQ
func (p *Publisher) PublishOrder(ctx context.Context, order *models.OrderRequest) error {
	return p.circuitBreaker.Call(func() error {
		return p.publishOrderInternal(ctx, order)
	})
}

// publishOrderInternal is the internal publish implementation
func (p *Publisher) publishOrderInternal(ctx context.Context, order *models.OrderRequest) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return fmt.Errorf("publisher is closed")
	}

	if p.channel == nil {
		return fmt.Errorf("channel is not initialized")
	}

	// Marshal order to JSON
	body, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	// Publish message
	err = p.channel.Publish(
		p.cfg.Exchange,   // exchange
		p.cfg.RoutingKey, // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			MessageId:    order.OrderID,
			Priority:     0,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	p.logger.Debug("Order published to RabbitMQ",
		zap.String("order_id", order.OrderID),
		zap.String("routing_key", p.cfg.RoutingKey))

	return nil
}

// Close closes the publisher
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	var errs []error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close channel: %w", err))
		}
	}

	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}

	p.logger.Info("RabbitMQ publisher closed")

	if len(errs) > 0 {
		return fmt.Errorf("errors closing publisher: %v", errs)
	}

	return nil
}

// GetCircuitBreakerState returns the current circuit breaker state
func (p *Publisher) GetCircuitBreakerState() string {
	return p.circuitBreaker.GetState()
}

// IsHealthy checks if the publisher is healthy
func (p *Publisher) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed || p.conn == nil || p.conn.IsClosed() {
		return false
	}

	return p.circuitBreaker.GetState() != "open"
}
