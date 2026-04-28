package manthan

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// SignalConsumer reads MANTHAN_* orders from trade-signals Kafka topic
// and routes them to the appropriate handler.
//
// Message routing by header "order_type":
//   MANTHAN_ENTRY     → EntryHandler.ExecuteEntry()
//   MANTHAN_SL_MODIFY → SLHandler.ModifyTrail()
//   MANTHAN_SL_EXIT   → SLHandler.EmergencySell()
type SignalConsumer struct {
	reader       *kafka.Reader
	entryHandler *EntryHandler
	slHandler    *SLHandler
	repo         *Repository // for MANTHAN_ENTRY idempotency check
	logger       *zap.Logger
}

type SignalConsumerConfig struct {
	KafkaBrokers []string
	Topic        string // default: trade-signals
	GroupID      string // default: trade-exec-manthan
}

func NewSignalConsumer(
	cfg SignalConsumerConfig,
	entryHandler *EntryHandler,
	slHandler *SLHandler,
	repo *Repository,
	logger *zap.Logger,
) *SignalConsumer {
	if cfg.Topic == "" {
		cfg.Topic = "trade-signals"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "trade-exec-manthan"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &SignalConsumer{
		reader:       reader,
		entryHandler: entryHandler,
		slHandler:    slHandler,
		repo:         repo,
		logger:       logger,
	}
}

// Start begins consuming. Blocks until ctx cancelled.
func (c *SignalConsumer) Start(ctx context.Context) {
	c.logger.Info("Manthan signal consumer started (trade-execution)",
		zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("Manthan signal consumer stopped")
				return
			}
			c.logger.Error("Kafka read error", zap.Error(err))
			continue
		}

		orderType := extractHeader(msg.Headers, "order_type")
		if !strings.HasPrefix(orderType, "MANTHAN_") {
			continue // not a Manthan order — skip
		}

		c.logger.Info("Manthan order received",
			zap.String("order_type", orderType),
			zap.String("key", string(msg.Key)))

		switch orderType {
		case "MANTHAN_ENTRY":
			c.handleEntry(ctx, msg.Value)
		case "MANTHAN_TOPUP":
			// Top-up: same as entry but skips the "already holding" duplicate
			// check. Carries top_up_for_signal_id linking to the parent
			// position so rules-engine projector can ADD to the parent's row.
			c.handleEntry(ctx, msg.Value)
		case "MANTHAN_SL_MODIFY":
			c.handleSLModify(ctx, msg.Value)
		case "MANTHAN_SL_EXIT":
			c.handleSLExit(ctx, msg.Value)
		default:
			c.logger.Warn("Unknown Manthan order type", zap.String("type", orderType))
		}
	}
}

// Stop closes the Kafka reader.
func (c *SignalConsumer) Stop() error {
	return c.reader.Close()
}

func (c *SignalConsumer) handleEntry(ctx context.Context, value []byte) {
	var signal ManthanSignal
	if err := json.Unmarshal(value, &signal); err != nil {
		c.logger.Error("Failed to unmarshal MANTHAN_ENTRY", zap.Error(err))
		return
	}

	// Idempotency: if an order with this signal_id already exists in the DB,
	// this Kafka message is a redelivery (consumer rebalance, retry, etc).
	// Placing another broker order would double the position and margin hit —
	// silently skip. The original handler is tracking the real order.
	if c.repo != nil && signal.OrderID != "" {
		if existing, err := c.repo.GetOrderBySignalID(ctx, signal.OrderID); err != nil {
			c.logger.Warn("Dedup lookup failed — proceeding (risk: possible duplicate)",
				zap.String("signal_id", signal.OrderID), zap.Error(err))
		} else if existing != nil {
			c.logger.Info("Duplicate MANTHAN_ENTRY skipped (idempotency)",
				zap.String("signal_id", signal.OrderID),
				zap.String("symbol", signal.Symbol),
				zap.Int64("existing_order_id", existing.ID),
				zap.String("existing_status", string(existing.Status)))
			return
		}
	}

	orderID, err := c.entryHandler.ExecuteEntry(ctx, signal)
	if err != nil {
		c.logger.Error("Entry execution failed",
			zap.String("symbol", signal.Symbol),
			zap.String("user", signal.UserID),
			zap.Error(err))
		return
	}

	c.logger.Info("Entry executed",
		zap.String("symbol", signal.Symbol),
		zap.String("user", signal.UserID),
		zap.Int64("order_id", orderID))
}

func (c *SignalConsumer) handleSLModify(ctx context.Context, value []byte) {
	var signal SLModifySignal
	if err := json.Unmarshal(value, &signal); err != nil {
		c.logger.Error("Failed to unmarshal MANTHAN_SL_MODIFY", zap.Error(err))
		return
	}

	if err := c.slHandler.ModifyTrail(ctx, signal); err != nil {
		c.logger.Error("SL modify failed",
			zap.String("symbol", signal.Symbol),
			zap.Error(err))
	}
}

func (c *SignalConsumer) handleSLExit(ctx context.Context, value []byte) {
	var signal SLExitSignal
	if err := json.Unmarshal(value, &signal); err != nil {
		c.logger.Error("Failed to unmarshal MANTHAN_SL_EXIT", zap.Error(err))
		return
	}

	if err := c.slHandler.EmergencySell(ctx, signal); err != nil {
		c.logger.Error("Emergency sell failed",
			zap.String("symbol", signal.Symbol),
			zap.Error(err))
	}
}

func extractHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
