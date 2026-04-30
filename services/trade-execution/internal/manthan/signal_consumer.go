package manthan

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// SignalConsumer reads MANTHAN_* orders from the trade-signals Kafka topic
// and persists them into signal_inbox. The actual broker work happens in
// InboxWorker — this consumer is intentionally fast and side-effect-free
// beyond the inbox INSERT.
//
// Why we split it
// ───────────────
// kafka-go's ReadMessage auto-commits on the next call regardless of
// handler outcome, so a handler that fails (e.g. AU004 broker session
// expired) silently advances the offset and the message is lost. With
// the inbox in front, the consumer commits ONLY after a durable INSERT;
// the worker handles retry / DLQ / observability. See migration 012.
type SignalConsumer struct {
	reader *kafka.Reader
	repo   *Repository
	worker *InboxWorker // optional — wakes the worker after each INSERT
	logger *zap.Logger
}

type SignalConsumerConfig struct {
	KafkaBrokers []string
	Topic        string // default: trade-signals
	GroupID      string // default: trade-exec-manthan
}

// NewSignalConsumer wires the inbox-only consumer. The handlers (EntryHandler,
// SLHandler) used to be passed in here and called inline; they're now
// invoked by the InboxWorker, so this constructor no longer takes them.
func NewSignalConsumer(
	cfg SignalConsumerConfig,
	repo *Repository,
	logger *zap.Logger,
) *SignalConsumer {
	if cfg.Topic == "" {
		cfg.Topic = "trade-signals"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "trade-exec-manthan"
	}

	// FetchMessage + manual CommitMessages — see Start for why.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &SignalConsumer{
		reader: reader,
		repo:   repo,
		logger: logger,
	}
}

// SetInboxWorker wires the worker so the consumer can poke it on each
// INSERT — drives a 0-latency processing tick instead of waiting for the
// worker's poll interval. Optional; nil is fine (worker still polls).
func (c *SignalConsumer) SetInboxWorker(w *InboxWorker) {
	c.worker = w
}

// Start begins consuming. Blocks until ctx cancelled.
//
// Delivery model: at-least-once via the inbox.
//   1. FetchMessage  (does NOT commit offset)
//   2. INSERT into signal_inbox  (durable; ON CONFLICT DO NOTHING)
//   3. CommitMessages  (offset advances only after step 2 succeeded)
//
// If step 2 fails or the process dies between 2 and 3, the consumer
// re-reads from the last committed offset on restart and the ON CONFLICT
// makes the second insert a no-op.
func (c *SignalConsumer) Start(ctx context.Context) {
	c.logger.Info("Manthan signal consumer started (inbox mode)",
		zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("Manthan signal consumer stopped")
				return
			}
			c.logger.Error("Kafka fetch error", zap.Error(err))
			continue
		}

		orderType := extractHeader(msg.Headers, "order_type")
		if !strings.HasPrefix(orderType, "MANTHAN_") {
			// Not for us — commit so we don't re-read it after restart.
			if cmErr := c.reader.CommitMessages(ctx, msg); cmErr != nil {
				c.logger.Warn("Commit (skip non-MANTHAN) failed", zap.Error(cmErr))
			}
			continue
		}

		// Pull just the routing fields off the payload — handlers re-parse
		// the full body from row.Payload when they pick the row up.
		var env struct {
			OrderID string `json:"order_id"`
			UserID  string `json:"user_id"`
		}
		if err := json.Unmarshal(msg.Value, &env); err != nil || env.OrderID == "" || env.UserID == "" {
			// Poison message — can't even insert (no signal_id for the unique
			// constraint). Skip + commit; this never recovers anyway.
			c.logger.Error("Malformed MANTHAN_* signal — skipping",
				zap.String("order_type", orderType),
				zap.String("key", string(msg.Key)),
				zap.Error(err))
			if cmErr := c.reader.CommitMessages(ctx, msg); cmErr != nil {
				c.logger.Warn("Commit (poison skip) failed", zap.Error(cmErr))
			}
			continue
		}

		inserted, err := c.repo.InsertInbox(ctx, env.OrderID, env.UserID, orderType,
			json.RawMessage(msg.Value),
			msg.Topic, msg.Partition, msg.Offset)
		if err != nil {
			// DB write failed — DON'T commit. We'll retry the same offset
			// on the next FetchMessage (which will block until the broker
			// resends; in practice it's already in our local fetch buffer).
			c.logger.Error("Inbox INSERT failed — will retry without commit",
				zap.String("signal_id", env.OrderID),
				zap.String("order_type", orderType),
				zap.Error(err))
			continue
		}

		if cmErr := c.reader.CommitMessages(ctx, msg); cmErr != nil {
			c.logger.Warn("Inbox commit failed — message may be re-inserted on restart (idempotent)",
				zap.String("signal_id", env.OrderID),
				zap.Error(cmErr))
		}

		if inserted {
			c.logger.Info("Manthan signal queued",
				zap.String("order_type", orderType),
				zap.String("signal_id", env.OrderID),
				zap.String("user", env.UserID))
		} else {
			// Already in inbox — Kafka redelivery, treat as success.
			c.logger.Debug("Manthan signal redelivered — already in inbox",
				zap.String("signal_id", env.OrderID))
		}

		// Wake the worker so it processes immediately instead of waiting
		// for its next poll tick. Non-blocking.
		if c.worker != nil {
			c.worker.Notify()
		}
	}
}

// Stop closes the Kafka reader.
func (c *SignalConsumer) Stop() error {
	return c.reader.Close()
}

func extractHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
