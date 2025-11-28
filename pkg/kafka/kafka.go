package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// ProducerConfig holds Kafka producer configuration
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	BatchTimeout time.Duration
	MaxAttempts  int
}

// ConsumerConfig holds Kafka consumer configuration
type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	StartOffset    int64 // kafka.FirstOffset or kafka.LastOffset
	CommitInterval time.Duration
	MaxBytes       int
}

// Producer wraps kafka.Writer with additional functionality
type Producer struct {
	writer *kafka.Writer
	config ProducerConfig
}

// Consumer wraps kafka.Reader with additional functionality
type Consumer struct {
	reader *kafka.Reader
	config ConsumerConfig
}

// NewProducer creates a new Kafka producer
func NewProducer(config ProducerConfig) (*Producer, error) {
	// Set default values
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	if config.BatchTimeout == 0 {
		config.BatchTimeout = 10 * time.Millisecond
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 3
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
		MaxAttempts:  config.MaxAttempts,
		Async:        false, // Synchronous by default for reliability
	}

	return &Producer{
		writer: writer,
		config: config,
	}, nil
}

// WriteMessage writes a single message to Kafka
func (p *Producer) WriteMessage(ctx context.Context, key, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	return p.writer.WriteMessages(ctx, msg)
}

// WriteMessages writes multiple messages to Kafka
func (p *Producer) WriteMessages(ctx context.Context, messages []kafka.Message) error {
	return p.writer.WriteMessages(ctx, messages...)
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.writer.Close()
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	// Set default values
	if config.StartOffset == 0 {
		config.StartOffset = kafka.LastOffset
	}
	if config.CommitInterval == 0 {
		config.CommitInterval = 1 * time.Second
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = 10e6 // 10MB
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        config.GroupID,
		StartOffset:    config.StartOffset,
		CommitInterval: config.CommitInterval,
		MaxBytes:       config.MaxBytes,
	})

	return &Consumer{
		reader: reader,
		config: config,
	}, nil
}

// ReadMessage reads a single message from Kafka
func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

// FetchMessage fetches a message without committing
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// CommitMessages commits messages
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

// Close closes the consumer
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Stats returns consumer statistics
func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

// SetOffset sets the consumer offset
func (c *Consumer) SetOffset(offset int64) error {
	return c.reader.SetOffset(offset)
}

// CreateTopic creates a new Kafka topic
func CreateTopic(brokers []string, topic string, numPartitions int, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to dial Kafka: %w", err)
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	return conn.CreateTopics(topicConfig)
}

// ListTopics lists all Kafka topics
func ListTopics(brokers []string) ([]string, error) {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to dial Kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	topicMap := make(map[string]bool)
	for _, p := range partitions {
		topicMap[p.Topic] = true
	}

	topics := make([]string, 0, len(topicMap))
	for topic := range topicMap {
		topics = append(topics, topic)
	}

	return topics, nil
}

// EnsureTopicExists checks if the topic exists; if not, creates it.
func EnsureTopicExists(brokers []string, topic string, numPartitions, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to dial Kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	// Check if topic already exists
	for _, p := range partitions {
		if p.Topic == topic {
			return nil // already exists
		}
	}

	// Create topic if missing
	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
	if err := conn.CreateTopics(topicConfig); err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}
