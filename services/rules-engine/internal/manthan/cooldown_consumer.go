package manthan

// CooldownConsumer subscribes to position.events and INSERTs manthan_cooldown
// rows for MANTHAN positions that exited via SL_TRIGGER. Follows the ratified
// design in docs/positions_service_design.md §5.3 (position.events consumers).
//
// Filter chain per §5.3:
//   origin      = MANTHAN         (skip USER_MANUAL — user's manual book)
//   event_type  = POSITION_EXITED (skip OPENED / MODIFIED / etc.)
//   exit_reason = SL_TRIGGER      (skip MANUAL_EXIT / STRATEGY_EXIT — cooldown
//                                    only applies to trail-triggered exits)
//
// UPSERT semantics: the manthan_cooldown UNIQUE (strategy_id, symbol) means a
// re-exit for a symbol already in cooldown overwrites with the fresh clock.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// CooldownConsumer reads position.events → INSERTs manthan_cooldown.
type CooldownConsumer struct {
	reader *kafka.Reader
	db     *sql.DB
	logger *zap.Logger
}

// CooldownConsumerConfig — Kafka wiring for the position.events subscription.
type CooldownConsumerConfig struct {
	KafkaBrokers []string
	Topic        string // default: position.events
	GroupID      string // default: rules-engine-cooldown
}

// positionEventEnvelope mirrors the JSON published by positions svc's
// internal/publisher/publisher.go — parsed locally (wire is the contract).
type positionEventEnvelope struct {
	EventID      string `json:"event_id"`
	EventType    string `json:"event_type"`
	ProducedAtMs int64  `json:"produced_at_ms"`

	PositionID string `json:"position_id"`
	Origin     string `json:"origin"`
	UserID     string `json:"user_id"`
	StrategyID string `json:"strategy_id,omitempty"`
	SignalID   string `json:"signal_id,omitempty"`
	Symbol     string `json:"symbol"`

	Action   string  `json:"action"`
	Price    float64 `json:"price,omitempty"`
	Quantity int     `json:"quantity,omitempty"`

	ExitReason  string  `json:"exit_reason,omitempty"`
	RealizedPnL float64 `json:"realized_pnl,omitempty"`

	BrokerOrderID string `json:"broker_order_id,omitempty"`
}

// NewCooldownConsumer wires the Kafka reader + DB handle. GroupID defaults
// pin us to a durable consumer group so restarts resume where they left off.
func NewCooldownConsumer(cfg CooldownConsumerConfig, db *sql.DB, logger *zap.Logger) *CooldownConsumer {
	if cfg.Topic == "" {
		cfg.Topic = "position.events"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "rules-engine-cooldown"
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	return &CooldownConsumer{
		reader: reader,
		db:     db,
		logger: logger,
	}
}

// Start blocks until ctx cancelled. At-least-once: commit AFTER SQL upsert.
// The UNIQUE (strategy_id, symbol) constraint + DO UPDATE means replayed
// events are idempotent (last-writer-wins on the same key).
func (c *CooldownConsumer) Start(ctx context.Context) {
	c.logger.Info("position.events cooldown consumer started",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group_id", c.reader.Config().GroupID))
	defer c.logger.Info("position.events cooldown consumer stopped")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("position.events fetch error", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			c.logger.Warn("position.events handle failed — not committing offset (will re-deliver)",
				zap.Int64("offset", msg.Offset),
				zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Warn("position.events commit failed",
				zap.Int64("offset", msg.Offset), zap.Error(err))
		}
	}
}

// Close stops the reader — call before shutdown to flush the group leave.
func (c *CooldownConsumer) Close() error {
	return c.reader.Close()
}

// handleMessage parses one Kafka message, applies the filter chain, and
// UPSERTs manthan_cooldown when it survives. Non-matching messages log at
// debug and commit cleanly (they're not errors — filtering out is the
// expected common case: 3+ event types × 2 origins).
func (c *CooldownConsumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var ev positionEventEnvelope
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		// Bad JSON isn't fixable by re-delivery — log + skip (nil so we commit).
		c.logger.Warn("position.events unmarshal failed — dropping",
			zap.Int64("offset", msg.Offset), zap.Error(err))
		return nil
	}

	// Filter — cheap headers-first check would be nicer but envelope parse is fine
	if !strings.EqualFold(ev.Origin, "MANTHAN") {
		return nil // USER_MANUAL cooldown isn't a thing
	}
	if !strings.EqualFold(ev.EventType, "POSITION_EXITED") {
		return nil // OPENED / MODIFIED / etc — not our concern
	}
	if !strings.EqualFold(ev.ExitReason, "SL_TRIGGER") {
		// MANUAL_EXIT / STRATEGY_EXIT / LIQUIDATION — no cooldown per Manthan spec
		c.logger.Debug("cooldown skip: exit_reason not SL_TRIGGER",
			zap.String("symbol", ev.Symbol),
			zap.String("exit_reason", ev.ExitReason))
		return nil
	}
	if ev.StrategyID == "" || ev.Symbol == "" || ev.Price == 0 {
		c.logger.Warn("cooldown skip: missing required fields",
			zap.String("strategy_id", ev.StrategyID),
			zap.String("symbol", ev.Symbol),
			zap.Float64("price", ev.Price))
		return nil
	}

	// TODO(rules-engine): query manthan_signals for the symbol's true ath_close
	// so ath_at_exit is accurate. For now the exit_price is a reasonable
	// approximation — Manthan's SL fires when price drops significantly, so
	// exit_price is typically well below the recent ATH. Refine once P.F
	// backfill is done and we can calibrate against historical data.
	athAtExit := ev.Price
	reentryBelow := ev.Price * 0.80 // Manthan spec §2.4: ATH × 0.80 gate

	if err := c.upsertCooldown(ctx, ev.StrategyID, ev.Symbol, athAtExit, ev.Price, reentryBelow); err != nil {
		return fmt.Errorf("upsertCooldown: %w", err)
	}

	c.logger.Info("cooldown row upserted",
		zap.String("strategy_id", ev.StrategyID),
		zap.String("symbol", ev.Symbol),
		zap.Float64("exit_price", ev.Price),
		zap.Float64("reentry_below", reentryBelow),
		zap.String("position_id", ev.PositionID))
	return nil
}

// upsertCooldown UPSERTs the cooldown row. UNIQUE (strategy_id, symbol)
// enforces one active cooldown per pair; a re-exit overwrites with a fresh
// exit_time and CLEARED status resets to FALSE (fresh cooldown).
func (c *CooldownConsumer) upsertCooldown(ctx context.Context, strategyID, symbol string, athAtExit, exitPrice, reentryBelow float64) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO manthan_cooldown
			(strategy_id, symbol, ath_at_exit, exit_price, reentry_below, exit_time, cleared, cleared_at)
		VALUES
			($1::uuid, $2, $3, $4, $5, NOW(), FALSE, NULL)
		ON CONFLICT (strategy_id, symbol) DO UPDATE SET
			ath_at_exit   = EXCLUDED.ath_at_exit,
			exit_price    = EXCLUDED.exit_price,
			reentry_below = EXCLUDED.reentry_below,
			exit_time     = EXCLUDED.exit_time,
			cleared       = FALSE,
			cleared_at    = NULL`,
		strategyID, symbol, athAtExit, exitPrice, reentryBelow,
	)
	return err
}
