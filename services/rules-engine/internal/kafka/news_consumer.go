package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// NewsConsumer consumes market news events and invokes the handler.
// It supports Stop() to stop fetching new Kafka messages for graceful shutdown.
type NewsConsumer struct {
	reader  KafkaReader
	handler consumer.EventHandler
	logger  *zap.Logger

	stopOnce sync.Once
	stopCh   chan struct{}
}

func NewNewsConsumer(reader KafkaReader, handler consumer.EventHandler, logger *zap.Logger) *NewsConsumer {
	return &NewsConsumer{reader: reader, handler: handler, logger: logger, stopCh: make(chan struct{})}
}

// Stop stops accepting new news messages (does not kill in-flight handler calls).
func (n *NewsConsumer) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		_ = n.reader.Close()
	})
}

func (n *NewsConsumer) Start(ctx context.Context) error {
	n.logger.Info("Starting news consumer")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return nil
		default:
		}

		msg, err := n.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			select {
			case <-n.stopCh:
				return nil
			default:
			}
			n.logger.Warn("News consumer fetch error", zap.Error(err))
			time.Sleep(250 * time.Millisecond)
			continue
		}

		// Parse MongoDB event
		var mongoEvent models.MongoDBEvent
		if err := json.Unmarshal(msg.Value, &mongoEvent); err != nil {
			n.logger.Error("Failed to unmarshal MongoDB event", zap.Error(err))
			_ = n.reader.CommitMessages(ctx, msg)
			continue
		}
		event, err := mongoEvent.ToMarketEvent()
		if err != nil {
			n.logger.Error("Failed to convert MongoDB event", zap.Error(err))
			_ = n.reader.CommitMessages(ctx, msg)
			continue
		}
		if err := event.Validate(); err != nil {
			n.logger.Warn("Invalid event received", zap.Error(err))
			_ = n.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := n.handler.HandleEvent(ctx, event); err != nil {
			// fail-fast per event: do not commit to allow retry.
			return fmt.Errorf("handler error: %w", err)
		}

		_ = n.reader.CommitMessages(ctx, msg)
	}
}
