package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Consumer reads from the `manthan.notifications` Kafka topic and hands
// each well-formed message to the Broadcaster. One Consumer per gateway
// process. Stop by cancelling its ctx.
//
// Why a dedicated group id per gateway instance? Notifications are *per
// user* push events — every gateway instance must receive every message
// (so the user's WS connection on any instance gets it). Using a unique
// group id forces kafka to deliver to all members. If you scale to N
// gateway pods, set GATEWAY_INSTANCE_ID per pod (or accept the random
// suffix added below).
type Consumer struct {
	reader *kafka.Reader
	bc     *Broadcaster
	logger *zap.Logger
}

// Config captures every knob ops might need to tweak. All have sane
// defaults; only Brokers is required.
type Config struct {
	Brokers     []string      // required — e.g. ["localhost:9092"]
	Topic       string        // default: "manthan.notifications"
	GroupID     string        // default: "api-gateway-notifications-<pid>"
	MinBytes    int           // default: 1
	MaxBytes    int           // default: 1MB
	MaxWait     time.Duration // default: 500ms
	StartOffset int64         // default: kafka.LastOffset (don't replay history)
}

// NewConsumer wires the Kafka reader. The reader is lazy — it doesn't
// connect until Run starts.
func NewConsumer(cfg Config, bc *Broadcaster, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("notifications consumer: at least one Kafka broker required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "manthan.notifications"
	}
	if cfg.GroupID == "" {
		// Random suffix so every gateway instance is its own consumer
		// group → every instance sees every message (correct fan-out
		// semantics for a push channel).
		cfg.GroupID = fmt.Sprintf("api-gateway-notifications-%d", time.Now().UnixNano())
	}
	if cfg.MinBytes == 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 1 << 20
	}
	if cfg.MaxWait == 0 {
		cfg.MaxWait = 500 * time.Millisecond
	}
	if cfg.StartOffset == 0 {
		cfg.StartOffset = kafka.LastOffset
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    cfg.MinBytes,
		MaxBytes:    cfg.MaxBytes,
		MaxWait:     cfg.MaxWait,
		StartOffset: cfg.StartOffset,
	})

	return &Consumer{
		reader: reader,
		bc:     bc,
		logger: logger.Named("notifications.consumer"),
	}, nil
}

// Run blocks until ctx is cancelled or the reader hits a fatal error.
// Transient parse errors and bad payloads are logged and skipped — the
// stream keeps moving.
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("starting notification consumer",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group_id", c.reader.Config().GroupID))
	defer func() {
		if err := c.reader.Close(); err != nil {
			c.logger.Warn("kafka reader close error", zap.Error(err))
		}
		c.logger.Info("notification consumer stopped")
	}()

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			// ctx cancelled or unrecoverable error — return.
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka read: %w", err)
		}

		var n Notification
		if err := json.Unmarshal(msg.Value, &n); err != nil {
			c.logger.Warn("dropping malformed notification payload",
				zap.Error(err),
				zap.ByteString("raw", msg.Value))
			continue
		}
		if n.UserID == "" {
			// The Broadcaster also guards this, but logging at the
			// consumer surfaces the producer that owes us a fix.
			c.logger.Warn("dropping notification with empty user_id",
				zap.String("type", n.Type),
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition))
			continue
		}
		c.bc.Broadcast(n)
	}
}
