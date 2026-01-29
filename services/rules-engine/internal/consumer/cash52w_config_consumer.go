package consumer

import (
	"context"
	"encoding/json"
	"fmt"

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
	if groupID == "" {
		groupID = "rules-engine-cash52w-config-v1"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 1 * 1e9, // 1s
	})

	logger.Info("Cash52W config Kafka consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group_id", groupID))

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

		msg, err := c.reader.FetchMessage(ctx)
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
			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				c.logger.Error("Failed to commit malformed Cash52W config message", zap.Error(commitErr))
			}
			continue
		}

		c.store.ApplyEvent(ev)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("Failed to commit Cash52W config message", zap.Error(err))
			continue
		}
	}
}

func (c *Cash52WConfigConsumer) Close() error {
	if c.reader != nil {
		c.logger.Info("Closing Cash52W config Kafka consumer")
		return c.reader.Close()
	}
	return nil
}
