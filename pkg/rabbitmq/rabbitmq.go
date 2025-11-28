package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// Config holds RabbitMQ configuration
type Config struct {
	URL            string
	PrefetchCount  int
	PrefetchSize   int
	ReconnectDelay time.Duration
	MaxReconnect   int
}

// Producer wraps RabbitMQ connection for publishing messages
type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  Config
}

// Consumer wraps RabbitMQ connection for consuming messages
type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  Config
}

// NewProducer creates a new RabbitMQ producer
func NewProducer(config Config) (*Producer, error) {
	// Set default values
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = 5 * time.Second
	}
	if config.MaxReconnect == 0 {
		config.MaxReconnect = 10
	}

	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Create channel
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return &Producer{
		conn:    conn,
		channel: channel,
		config:  config,
	}, nil
}

// Publish publishes a message to a queue
func (p *Producer) Publish(ctx context.Context, queue string, body []byte) error {
	// Declare queue (idempotent)
	_, err := p.channel.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Publish message
	err = p.channel.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// PublishToExchange publishes a message to an exchange
func (p *Producer) PublishToExchange(ctx context.Context, exchange, routingKey string, body []byte) error {
	err := p.channel.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to exchange: %w", err)
	}

	return nil
}

// Close closes the producer connection
func (p *Producer) Close() error {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// NewConsumer creates a new RabbitMQ consumer
func NewConsumer(config Config) (*Consumer, error) {
	// Set default values
	if config.PrefetchCount == 0 {
		config.PrefetchCount = 10
	}
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = 5 * time.Second
	}
	if config.MaxReconnect == 0 {
		config.MaxReconnect = 10
	}

	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Create channel
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Set QoS
	err = channel.Qos(
		config.PrefetchCount, // prefetch count
		config.PrefetchSize,  // prefetch size
		false,                // global
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &Consumer{
		conn:    conn,
		channel: channel,
		config:  config,
	}, nil
}

// Consume starts consuming messages from a queue
func (c *Consumer) Consume(ctx context.Context, queue string, autoAck bool) (<-chan amqp.Delivery, error) {
	// Declare queue (idempotent)
	_, err := c.channel.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Start consuming
	msgs, err := c.channel.Consume(
		queue,   // queue
		"",      // consumer
		autoAck, // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	return msgs, nil
}

// Close closes the consumer connection
func (c *Consumer) Close() error {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// DeclareQueue declares a queue
func DeclareQueue(url, queueName string, durable bool) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer channel.Close()

	_, err = channel.QueueDeclare(
		queueName, // name
		durable,   // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	return err
}

// DeclareExchange declares an exchange
func DeclareExchange(url, exchangeName, exchangeType string, durable bool) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer channel.Close()

	return channel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type (direct, fanout, topic, headers)
		durable,      // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
}

// BindQueue binds a queue to an exchange
func BindQueue(url, queueName, exchangeName, routingKey string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer channel.Close()

	return channel.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
}
