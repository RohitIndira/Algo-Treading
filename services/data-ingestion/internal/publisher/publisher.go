package publisher

import (
	"context"

	kafkapkg "github.com/RohitIndira/Algo-Treading/pkg/kafka"
)

// Publisher defines a simple publishing interface used by the watcher
type Publisher interface {
	Publish(ctx context.Context, key, value []byte) error
}

// KafkaPublisher publishes messages using the repo kafka Producer
type KafkaPublisher struct {
	prod  *kafkapkg.Producer
	topic string
}

// NewKafkaPublisher constructs a KafkaPublisher
func NewKafkaPublisher(prod *kafkapkg.Producer, topic string) Publisher {
	return &KafkaPublisher{prod: prod, topic: topic}
}

func (k *KafkaPublisher) Publish(ctx context.Context, key, value []byte) error {
	return k.prod.WriteMessage(ctx, key, value)
}
