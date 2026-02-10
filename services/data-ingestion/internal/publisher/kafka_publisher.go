package publisher

import (
	"context"
	"fmt"

	kafkapkg "github.com/RohitIndira/Algo-Treading/pkg/kafka"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/models"
)

// KafkaPublisher publishes breakout events to Kafka
type KafkaPublisher interface {
	PublishBreakout(ctx context.Context, event *models.BreakoutEvent) error
	Close() error
}

// kafkaPublisher implements KafkaPublisher
type kafkaPublisher struct {
	producer *kafkapkg.Producer
	topic    string
}

// NewKafkaPublisher creates a new Kafka publisher for breakout events
func NewKafkaPublisher(producer *kafkapkg.Producer, topic string) KafkaPublisher {
	return &kafkaPublisher{
		producer: producer,
		topic:    topic,
	}
}

// PublishBreakout publishes a breakout event to Kafka
func (p *kafkaPublisher) PublishBreakout(ctx context.Context, event *models.BreakoutEvent) error {
	if event == nil {
		return fmt.Errorf("cannot publish nil breakout event")
	}

	// Serialize to JSON
	value, err := event.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize breakout event: %w", err)
	}

	// Use token as the message key for consistent partitioning
	key := event.Key()

	// Publish to Kafka
	if err := p.producer.WriteMessage(ctx, key, value); err != nil {
		return fmt.Errorf("failed to publish to kafka: %w", err)
	}

	return nil
}

// Close closes the Kafka producer
func (p *kafkaPublisher) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}
