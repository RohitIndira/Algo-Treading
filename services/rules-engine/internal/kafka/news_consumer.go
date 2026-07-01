package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/correlation"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const maxHandlerRetries = 3

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

		// Extract correlation ID from Kafka headers and propagate through context.
		corrID := extractCorrelationID(msg)
		if corrID == "" {
			corrID = correlation.NewID()
		}
		msgCtx := correlation.WithContext(ctx, corrID)

		// Parse MongoDB event
		var mongoEvent models.MongoDBEvent
		if err := json.Unmarshal(msg.Value, &mongoEvent); err != nil {
			n.logger.Error("Failed to unmarshal MongoDB event",
				zap.Error(err),
				zap.String("correlation_id", corrID))
			_ = n.reader.CommitMessages(ctx, msg)
			continue
		}
		event, err := mongoEvent.ToMarketEvent()
		if err != nil {
			n.logger.Error("Failed to convert MongoDB event",
				zap.Error(err),
				zap.String("correlation_id", corrID))
			_ = n.reader.CommitMessages(ctx, msg)
			continue
		}
		if err := event.Validate(); err != nil {
			n.logger.Warn("Invalid event received",
				zap.Error(err),
				zap.String("correlation_id", corrID))
			_ = n.reader.CommitMessages(ctx, msg)
			continue
		}

		var handleErr error
		for attempt := 1; attempt <= maxHandlerRetries; attempt++ {
			handleErr = n.callHandlerRecovered(msgCtx, event)
			if handleErr == nil {
				break
			}
			n.logger.Warn("Handler error, retrying",
				zap.Error(handleErr),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", maxHandlerRetries),
				zap.String("event_id", event.EventID),
				zap.String("correlation_id", corrID))
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		if handleErr != nil {
			n.logger.Error("Handler failed after all retries, skipping event",
				zap.Error(handleErr),
				zap.String("event_id", event.EventID),
				zap.Int64("stock_code", event.StockData.StockCode),
				zap.String("correlation_id", corrID))
		}

		_ = n.reader.CommitMessages(ctx, msg)
	}
}

// callHandlerRecovered invokes the handler with panic recovery. This is the
// backstop for the sequential parts of HandleEvent that run outside the
// engine's worker pool (e.g. processMatch, order construction, Kafka/DB
// writes) — without it, a panic there kills the whole consumer loop instead
// of just failing this one event, which the retry loop above then handles
// like any other transient error.
func (n *NewsConsumer) callHandlerRecovered(ctx context.Context, event *models.MarketEvent) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			n.logger.Error("PANIC RECOVERED in news handler",
				zap.String("event_id", event.EventID),
				zap.Any("panic", rec),
				zap.String("stacktrace", string(debug.Stack())))
			err = fmt.Errorf("panic recovered in handler: %v", rec)
		}
	}()
	return n.handler.HandleEvent(ctx, event)
}

// extractCorrelationID reads the correlation ID from a Kafka message's headers.
func extractCorrelationID(msg kafka.Message) string {
	for _, h := range msg.Headers {
		if h.Key == correlation.KafkaHeaderKey {
			return string(h.Value)
		}
	}
	return ""
}
