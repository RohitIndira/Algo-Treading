package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cash52w"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Cash52WConfigConsumer consumes 52W user configuration events from the
// user-configs.cash52w topic and keeps the in-memory ConfigStore in sync.
type Cash52WConfigConsumer struct {
	reader *kafka.Reader
	store  *cash52w.ConfigStore
	logger *zap.Logger
}

func NewCash52WConfigConsumer(brokers []string, topic, groupID string, store *cash52w.ConfigStore, logger *zap.Logger) (*Cash52WConfigConsumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("cash52w config topic cannot be empty")
	}
	// NOTE:
	// We intentionally DO NOT use a Kafka consumer group for this topic.
	// Reason: ConfigStore is purely in-memory. If we use a group, Kafka will
	// resume from the last committed offsets after a restart, but our in-memory
	// store will be empty, so rules-engine would think there are no configured
	// users until a new config event arrives.
	//
	// By consuming without a group from the earliest offset, every restart
	// rebuilds the full config state from the retained topic history.
	_ = groupID // kept for backward compatibility with call sites
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})

	logger.Info("Cash52W config Kafka consumer created (no group, replay from earliest)",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic))

	return &Cash52WConfigConsumer{
		reader: reader,
		store:  store,
		logger: logger,
	}, nil
}

func (c *Cash52WConfigConsumer) Start(ctx context.Context) error {
	if c.reader == nil {
		return fmt.Errorf("cash52w config consumer has no reader configured")
	}

	c.logger.Info("Starting Cash52W config Kafka consumer")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Cash52W config Kafka consumer stopping", zap.Error(ctx.Err()))
			return ctx.Err()
		default:
		}

		// ReadMessage advances the reader offset automatically. We don't commit
		// because we are not using a consumer group.
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				c.logger.Info("Cash52W config consumer context cancelled")
				return nil
			}
			c.logger.Error("Failed to fetch Cash52W config message", zap.Error(err))
			continue
		}

		var ev cash52w.ConfigEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			c.logger.Error("Failed to unmarshal Cash52W ConfigEvent",
				zap.Error(err))
			continue
		}

		c.store.ApplyEvent(ev)
	}
}

func (c *Cash52WConfigConsumer) Close() error {
	if c.reader != nil {
		c.logger.Info("Closing Cash52W config Kafka consumer")
		return c.reader.Close()
	}
	return nil
}
