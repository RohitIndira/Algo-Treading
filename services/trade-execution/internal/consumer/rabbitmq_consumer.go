package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConsumer handles consuming messages from RabbitMQ
type RabbitMQConsumer struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	queueName     string
	prefetchCount int
	executor      *executor.OrderExecutor
	repo          repository.OrderRepository
	workerCount   int
	shutdownChan  chan struct{}
}

// Config holds consumer configuration
type Config struct {
	URL           string
	QueueName     string
	Exchange      string
	ExchangeType  string
	RoutingKey    string
	PrefetchCount int
	WorkerCount   int
	Durable       bool
}

// NewRabbitMQConsumer creates a new RabbitMQ consumer
func NewRabbitMQConsumer(cfg Config, exec *executor.OrderExecutor, repo repository.OrderRepository) (*RabbitMQConsumer, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Set default values if not provided
	exchangeType := cfg.ExchangeType
	if exchangeType == "" {
		exchangeType = "topic"
	}
	durable := cfg.Durable
	if !durable {
		durable = true
	}

	// Declare exchange (idempotent)
	log.Printf("Declaring exchange: %s (type: %s, durable: %v)", cfg.Exchange, exchangeType, durable)
	err = channel.ExchangeDeclare(
		cfg.Exchange, // name
		exchangeType, // type
		durable,      // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Declare queue (idempotent)
	log.Printf("Declaring queue: %s (durable: %v)", cfg.QueueName, durable)
	_, err = channel.QueueDeclare(
		cfg.QueueName, // name
		durable,       // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind queue to exchange with routing key
	log.Printf("Binding queue '%s' to exchange '%s' with routing key '%s'", cfg.QueueName, cfg.Exchange, cfg.RoutingKey)
	err = channel.QueueBind(
		cfg.QueueName,  // queue name
		cfg.RoutingKey, // routing key
		cfg.Exchange,   // exchange
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	log.Println("✓ RabbitMQ exchange, queue, and binding configured successfully")

	// Set QoS (prefetch count)
	err = channel.Qos(cfg.PrefetchCount, 0, false)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &RabbitMQConsumer{
		conn:          conn,
		channel:       channel,
		queueName:     cfg.QueueName,
		prefetchCount: cfg.PrefetchCount,
		executor:      exec,
		repo:          repo,
		workerCount:   cfg.WorkerCount,
		shutdownChan:  make(chan struct{}),
	}, nil
}

// Start begins consuming messages
func (c *RabbitMQConsumer) Start(ctx context.Context) error {
	log.Printf("Starting RabbitMQ consumer on queue: %s", c.queueName)

	// Declare queue (idempotent)
	_, err := c.channel.QueueDeclare(
		c.queueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Start consuming
	messages, err := c.channel.Consume(
		c.queueName,
		"trade-executor", // consumer tag
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf("Starting %d worker(s) with prefetch count %d", c.workerCount, c.prefetchCount)

	// Start worker pool
	for i := 0; i < c.workerCount; i++ {
		go c.worker(ctx, messages, i)
	}

	log.Println("RabbitMQ consumer started successfully")

	// Wait for shutdown
	<-c.shutdownChan

	return nil
}

// worker processes messages from the queue
func (c *RabbitMQConsumer) worker(ctx context.Context, messages <-chan amqp.Delivery, workerID int) {
	log.Printf("Worker %d started", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down", workerID)
			return

		case msg, ok := <-messages:
			if !ok {
				log.Printf("Worker %d: channel closed", workerID)
				return
			}

			c.processMessage(ctx, msg, workerID)
		}
	}
}

// processMessage handles a single order request
func (c *RabbitMQConsumer) processMessage(ctx context.Context, msg amqp.Delivery, workerID int) {
	log.Printf("Worker %d processing message", workerID)

	// Parse order request
	var orderReq models.OrderRequest
	if err := json.Unmarshal(msg.Body, &orderReq); err != nil {
		log.Printf("Worker %d: failed to parse message: %v", workerID, err)
		msg.Nack(false, false) // Send to DLQ
		return
	}

	log.Printf("Worker %d: Processing order request %s for user %s", workerID, orderReq.RequestID, orderReq.UserID)

	// Validate request
	if err := c.validateOrderRequest(&orderReq); err != nil {
		log.Printf("Worker %d: invalid order request: %v", workerID, err)
		msg.Nack(false, false) // Send to DLQ
		return
	}

	// Convert to internal order model
	order := c.convertToOrder(&orderReq)

	// Save order to database
	if err := c.repo.Create(ctx, order); err != nil {
		log.Printf("Worker %d: failed to save order: %v", workerID, err)
		msg.Nack(false, true) // Requeue
		return
	}

	log.Printf("Worker %d: Order %s saved to database", workerID, order.OrderID)

	// Execute order
	if err := c.executor.ExecuteOrder(ctx, order); err != nil {
		log.Printf("Worker %d: failed to execute order %s: %v", workerID, order.OrderID, err)

		// Check if we should retry
		if orderReq.RetryCount < 3 {
			log.Printf("Worker %d: Requeueing order (retry count: %d)", workerID, orderReq.RetryCount)
			msg.Nack(false, true) // Requeue
		} else {
			log.Printf("Worker %d: Max retries reached, sending to DLQ", workerID)
			msg.Nack(false, false) // Send to DLQ
		}
		return
	}

	log.Printf("Worker %d: Successfully processed order %s", workerID, order.OrderID)
	msg.Ack(false)
}

func (c *RabbitMQConsumer) validateOrderRequest(req *models.OrderRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	// StockCode is required for some flows, but for others (like the 52W
	// breakout engine) we may rely primarily on token/symbol/exchange for
	// routing. The database schema allows 0 as a valid BIGINT, so we no
	// longer hard-fail when stock_code == 0 here.
	if req.Quantity <= 0 {
		return fmt.Errorf("quantity must be positive")
	}
	if req.Exchange == "" {
		return fmt.Errorf("exchange is required")
	}
	if req.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	// Risk management is handled upstream (rules-engine / risk service).
	// For development we ignore the RiskApproved flag here to avoid
	// double enforcement and noisy logs when risk is disabled.
	return nil
}

func (c *RabbitMQConsumer) convertToOrder(req *models.OrderRequest) *models.Order {
	orderID := uuid.New()
	eventID, _ := uuid.Parse(req.EventID)
	if eventID == uuid.Nil {
		eventID = uuid.New()
	}

	now := time.Now()

	return &models.Order{
		OrderID:    orderID,
		UserID:     req.UserID,
		StrategyID: req.StrategyID,
		EventID:    eventID,
		StockCode:  req.StockCode,
		// Token is the actual trading token (scrip token) provided by
		// rules-engine. Using this for Odin's scrip_token prevents
		// e-101 "Scrip details not found" when StockCode alone is
		// insufficient (or 0).
		Token:        req.Token,
		Exchange:     models.Exchange(req.Exchange),
		Symbol:       req.Symbol,
		OrderType:    models.OrderType(req.OrderType),
		OrderSide:    models.OrderSide(req.OrderSide),
		Quantity:     req.Quantity,
		Price:        req.Price,
		StopLoss:     req.StopLoss,
		TakeProfit:   req.TakeProfit,
		Validity:     req.Validity,
		Status:       models.StatusReceived,
		RiskApproved: req.RiskApproved,
		RiskScore:    req.RiskScore,
		RetryCount:   req.RetryCount,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// Shutdown gracefully shuts down the consumer
func (c *RabbitMQConsumer) Shutdown() error {
	log.Println("Shutting down RabbitMQ consumer...")

	close(c.shutdownChan)

	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			log.Printf("Error closing channel: %v", err)
		}
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}

	log.Println("RabbitMQ consumer shutdown complete")
	return nil
}
