package manthan

// ManualExitLedgerConsumer — closes the manthan_orders ledger when a user
// exits a Manthan position from their broker app (the 2026-09-03 ghost/churn
// investigation's formation gap).
//
// The canonical manual-exit chain already works WITHOUT this consumer:
//
//	third-party SELL → orderstatus svc (whole-book WSS+REST observation)
//	  → order.events → positions svc manual-FIFO (handleManualSellFill)
//	  → positions_db EXITED + POSITION_EXITED on position.events
//	  → rules-engine ExitConfirmed → slot released + manthan_positions EXITED
//
// What that chain does NOT touch is trade-execution's own ledger
// (manthan_orders): the entry BUY keeps net>0, so ListPositionsNeedingProtection
// keeps returning the position and EOD Phase A re-attempts an AMO SL every
// evening against shares that are gone (broker rejects; ≤5/night forever).
//
// This consumer closes that gap by PROJECTING the authoritative positions-svc
// verdict into our own ledger: on POSITION_EXITED with origin=MANTHAN and
// exit_reason=MANUAL_EXIT, insert one synthetic FILLED SELL row. No detection
// logic lives here — the FIFO attribution across mixed manual/algo books
// (which shares were whose) is decided ONCE, by positions svc, and we record
// its outcome. Deliberately NOT handled:
//
//   - partial manual exits (MANUAL_INTERRUPT): the position legitimately
//     stays open for the remainder; the reconciler surfaces the qty drift as
//     QTY_MISMATCH. Logged loudly, no ledger write.
//   - origin=USER_MANUAL exits: not our ledger's positions.
//   - non-manual exit reasons (SL_TRIGGER etc.): those SELLs are OUR orders
//     and already land in the ledger through the normal fill path.
//
// Idempotency: signal_id = "manualexit-<position_id>" — a positions-svc lot
// can fully close exactly once, and manthan_orders UNIQUE(signal_id) makes
// replays (Kafka re-delivery, group rebalance, restart) no-ops.
//
// Rollback: MANTHAN_MANUAL_EXIT_LEDGER=off disables the consumer at boot
// (default on — this is the formation fix). See manthan_init.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// manualExitEnvelope mirrors the position.events JSON contract
// (services/positions/internal/publisher/publisher.go PositionEvent).
type manualExitEnvelope struct {
	EventID       string  `json:"event_id"`
	EventType     string  `json:"event_type"`
	PositionID    string  `json:"position_id"`
	Origin        string  `json:"origin"`
	UserID        string  `json:"user_id"`
	StrategyID    string  `json:"strategy_id"`
	SignalID      string  `json:"signal_id"`
	Symbol        string  `json:"symbol"`
	Price         float64 `json:"price"`
	Quantity      int     `json:"quantity"`
	ExitReason    string  `json:"exit_reason"`
	BrokerOrderID string  `json:"broker_order_id"`
}

// manualExitLedger is the repository seam (tests fake it; *Repository
// satisfies it via InsertManualExitLedgerSell).
type manualExitLedger interface {
	InsertManualExitLedgerSell(ctx context.Context, e ManualExitLedgerRow) (bool, error)
}

// ManualExitLedgerRow is the synthetic SELL to record.
type ManualExitLedgerRow struct {
	SignalID      string // "manualexit-<position_id>" — the idempotency key
	StrategyID    string
	UserID        string
	Symbol        string
	Qty           int
	ExitPrice     float64
	BrokerOrderID string // the third-party SELL's broker id, for audit
	SourceEventID string // position.events event_id, for audit
}

// ManualExitLedgerConsumer reads position.events with its own group so its
// offsets are independent of rules-engine's cooldown consumer.
type ManualExitLedgerConsumer struct {
	reader *kafka.Reader
	ledger manualExitLedger
	logger *zap.Logger
}

type ManualExitLedgerConfig struct {
	KafkaBrokers []string
	Topic        string // default: position.events
	GroupID      string // default: trade-exec-manual-exit-ledger
}

func NewManualExitLedgerConsumer(cfg ManualExitLedgerConfig, ledger manualExitLedger, logger *zap.Logger) *ManualExitLedgerConsumer {
	if cfg.Topic == "" {
		cfg.Topic = "position.events"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "trade-exec-manual-exit-ledger"
	}
	return &ManualExitLedgerConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.KafkaBrokers,
			Topic:          cfg.Topic,
			GroupID:        cfg.GroupID,
			MinBytes:       1,
			MaxBytes:       1 << 20,
			CommitInterval: 0, // manual commit — at-least-once + idempotent insert
		}),
		ledger: ledger,
		logger: logger,
	}
}

// Start blocks until ctx cancels. Run in a goroutine.
func (c *ManualExitLedgerConsumer) Start(ctx context.Context) {
	c.logger.Info("manual-exit ledger consumer started",
		zap.String("topic", "position.events"),
		zap.String("group_id", "trade-exec-manual-exit-ledger"))
	defer c.logger.Info("manual-exit ledger consumer stopped")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("manual-exit ledger: fetch error", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		if err := c.HandleMessage(ctx, msg.Value); err != nil {
			// Transient (DB down): do NOT commit — Kafka re-delivers, the
			// UNIQUE(signal_id) insert makes the retry safe.
			c.logger.Warn("manual-exit ledger: handle failed — not committing (will re-deliver)",
				zap.Int64("offset", msg.Offset), zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Warn("manual-exit ledger: commit failed",
				zap.Int64("offset", msg.Offset), zap.Error(err))
		}
	}
}

func (c *ManualExitLedgerConsumer) Close() error { return c.reader.Close() }

// HandleMessage applies the filter chain and records the ledger SELL.
// Exported for tests. Returns an error ONLY for retryable failures (DB);
// events filtered out or malformed return nil so the offset commits.
func (c *ManualExitLedgerConsumer) HandleMessage(ctx context.Context, value []byte) error {
	var ev manualExitEnvelope
	if err := json.Unmarshal(value, &ev); err != nil {
		c.logger.Warn("manual-exit ledger: unmarshal failed — dropping", zap.Error(err))
		return nil
	}

	if !strings.EqualFold(ev.Origin, "MANTHAN") {
		return nil // USER_MANUAL lots are not in our ledger
	}
	switch {
	case strings.EqualFold(ev.EventType, "POSITION_EXITED") &&
		strings.EqualFold(ev.ExitReason, "MANUAL_EXIT"):
		// fall through — the one case we project
	case strings.EqualFold(ev.EventType, "MANUAL_INTERRUPT"):
		// Partial manual exit: position legitimately remains open for the
		// remainder — no full-close ledger row. Loud log so the operator
		// sees the qty drift before the reconciler flags QTY_MISMATCH.
		c.logger.Warn("manual-exit ledger: PARTIAL manual exit observed — ledger unchanged, reconciler will show QTY_MISMATCH",
			zap.String("user_id", ev.UserID), zap.String("symbol", ev.Symbol),
			zap.Int("qty_sold", ev.Quantity), zap.String("position_id", ev.PositionID))
		return nil
	default:
		return nil // OPENED / MODIFIED / SL-driven exits: normal paths cover these
	}

	if ev.PositionID == "" || ev.UserID == "" || ev.Symbol == "" || ev.Quantity <= 0 || ev.StrategyID == "" {
		c.logger.Warn("manual-exit ledger: MANUAL_EXIT event missing required fields — dropping",
			zap.String("event_id", ev.EventID), zap.String("position_id", ev.PositionID),
			zap.String("user_id", ev.UserID), zap.String("symbol", ev.Symbol))
		return nil
	}

	row := ManualExitLedgerRow{
		SignalID:      "manualexit-" + ev.PositionID,
		StrategyID:    ev.StrategyID,
		UserID:        ev.UserID,
		Symbol:        ev.Symbol,
		Qty:           ev.Quantity,
		ExitPrice:     ev.Price,
		BrokerOrderID: ev.BrokerOrderID,
		SourceEventID: ev.EventID,
	}
	inserted, err := c.ledger.InsertManualExitLedgerSell(ctx, row)
	if err != nil {
		return fmt.Errorf("manual-exit ledger insert: %w", err)
	}
	if inserted {
		c.logger.Info("MANUAL_EXIT_PROJECTED — ledger SELL recorded, EOD protection query will release this position",
			zap.String("user_id", ev.UserID), zap.String("symbol", ev.Symbol),
			zap.Int("qty", ev.Quantity), zap.Float64("exit_price", ev.Price),
			zap.String("signal_id", row.SignalID),
			zap.String("third_party_broker_order", ev.BrokerOrderID))
	} else {
		c.logger.Debug("manual-exit ledger: replay dedup — row already recorded",
			zap.String("signal_id", row.SignalID))
	}
	return nil
}
