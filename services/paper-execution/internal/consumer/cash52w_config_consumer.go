package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Cash52WConfigEvent mirrors the compact user-config event on topic
// user-configs.cash52w.
type Cash52WConfigEvent struct {
	EventType       string  `json:"event_type"`
	UserID          string  `json:"user_id"`
	Enabled         bool    `json:"enabled"`
	CapitalPerStock float64 `json:"capital_per_stock"`
	TradingMode     string  `json:"trading_mode"`
	Timestamp       int64   `json:"timestamp"`
}

// Cash52WConfigHandler is invoked when a user disables/deletes the managed
// CASH_52W_HIGH strategy.
type Cash52WConfigHandler interface {
	OnCash52WDisabled(ctx context.Context, userID string) error
}

// Cash52WConfigConsumer listens to user-configs.cash52w so paper-execution can
// force-close open PAPER positions when a user disables the strategy.
type Cash52WConfigConsumer struct {
	reader  *kafka.Reader
	handler Cash52WConfigHandler
	logger  *zap.Logger
}

func NewCash52WConfigConsumer(brokers []string, topic string, groupID string, handler Cash52WConfigHandler, logger *zap.Logger) (*Cash52WConfigConsumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers cannot be empty")
	}
	if topic == "" {
		topic = "user-configs.cash52w"
	}
	if groupID == "" {
		groupID = "paper-execution-cash52w-config-v1"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
		MaxWait:        1 * time.Second,
	})

	logger.Info("Paper cash52w-config consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group_id", groupID))

	return &Cash52WConfigConsumer{reader: r, handler: handler, logger: logger}, nil
}

func (c *Cash52WConfigConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

func (c *Cash52WConfigConsumer) Start(ctx context.Context) error {
	if c.reader == nil {
		return fmt.Errorf("cash52w-config reader is nil")
	}

	c.logger.Info("Starting paper cash52w-config consumer")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Warn("Failed to fetch cash52w-config message", zap.Error(err))
			time.Sleep(250 * time.Millisecond)
			continue
		}

		if err := c.processMessage(ctx, msg); err != nil {
			c.logger.Warn("Failed to process cash52w-config message", zap.Error(err))
			// commit anyway; a single bad message should not block the stream
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		_ = c.reader.CommitMessages(ctx, msg)
	}
}

func (c *Cash52WConfigConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	var ev Cash52WConfigEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("unmarshal cash52w-config: %w", err)
	}

	uid := strings.TrimSpace(ev.UserID)
	if uid == "" {
		return nil
	}

	// Treat as disabled when:
	// - event_type == DELETE
	// - OR enabled == false (some clients may send UPDATE with enabled=false)
	etype := strings.ToUpper(strings.TrimSpace(ev.EventType))
	if etype == "DELETE" || !ev.Enabled {
		c.logger.Info("Cash52W disabled -> force closing paper positions",
			zap.String("user_id", uid),
			zap.String("event_type", etype),
			zap.Bool("enabled", ev.Enabled))

		if c.handler != nil {
			return c.handler.OnCash52WDisabled(ctx, uid)
		}
	}

	return nil
}
