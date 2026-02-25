package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// KafkaReader is a mockable subset of kafka-go Reader.
type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}
