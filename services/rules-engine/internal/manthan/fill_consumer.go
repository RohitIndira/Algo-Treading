package manthan

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// FillConsumer reads broker events from manthan.execution.events and projects
// them onto the rules-engine state (in-memory portfolio + Postgres + Redis).
//
// Delivery semantics: AT-LEAST-ONCE.
//
//   FetchMessage  →  process  →  CommitMessages
//
// Offsets advance ONLY after processing succeeds. A handler that returns
// a transient error (DB blip, projector TX rollback) keeps the offset
// un-committed; the next fetch retrieves the same message and retries.
// This closes the silent-message-loss gap that ReadMessage (auto-commit)
// had — previously a failed projector.Apply would still advance the
// offset, dropping the fill event forever.
//
// Two modes (selected at startup by DecisionLogEnabled):
//
//   Legacy (DecisionLogEnabled=false):
//     - FILL_CONFIRMED / FILL_UPDATE / ENTRY_FILLED / ENTRY_PARTIAL_FILL
//       drive in-memory portfolio + UpdatePositionFill DB write.
//     - SL_PLACED / SL_MODIFIED sync broker reality back to in-memory
//       pos.CurrentSL and manthan_positions.current_sl. Without this the
//       trail math runs on stale state and can lower the broker SL.
//
//   CQRS (DecisionLogEnabled=true):
//     - ALL events run through PositionProjector.Apply for an atomic
//       INSERT manthan_position_events + UPSERT manthan_positions +
//       UPDATE manthan_signal_decisions transaction.
//     - Idempotency via ON CONFLICT (signal_id, event_seq) DO NOTHING.
//     - In-memory portfolio still updated alongside so TSL / cooldown
//       / allocator stay current.
type FillConsumer struct {
	reader       *kafka.Reader
	portfolioMgr *PortfolioManager
	slMgr        *TrailingSLManager
	publisher    OrderPublisher
	projector    *PositionProjector
	slPct        float64 // 20
	logger       *zap.Logger

	decisionLogEnabled bool
}

type FillConsumerConfig struct {
	KafkaBrokers       []string
	Topic              string // default: manthan.execution.events
	GroupID            string // default: rule-engine-manthan-fills
	DecisionLogEnabled bool
}

func NewFillConsumer(
	cfg FillConsumerConfig,
	portfolioMgr *PortfolioManager,
	slMgr *TrailingSLManager,
	publisher OrderPublisher,
	projector *PositionProjector,
	slPct float64,
	logger *zap.Logger,
) *FillConsumer {
	if cfg.Topic == "" {
		cfg.Topic = "manthan.execution.events"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "rule-engine-manthan-fills"
	}

	// MinBytes:1 ensures we don't sit waiting for batches — fills are bursty
	// and need to land fast for TSL math to use fresh state.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &FillConsumer{
		reader:             reader,
		portfolioMgr:       portfolioMgr,
		slMgr:              slMgr,
		publisher:          publisher,
		projector:          projector,
		slPct:              slPct,
		logger:             logger,
		decisionLogEnabled: cfg.DecisionLogEnabled,
	}
}

func (c *FillConsumer) Start(ctx context.Context) {
	c.logger.Info("Fill consumer started (manual-commit mode)",
		zap.String("topic", c.reader.Config().Topic),
		zap.Bool("decision_log", c.decisionLogEnabled))

	// Restart-safe idempotency for the legacy path: the projector has its
	// own (signal_id, event_seq) UNIQUE in DB. This in-memory set guards
	// the legacy portfolio-update code from double-applying within a single
	// process lifetime (e.g. consumer rebalance redelivery).
	processed := make(map[string]bool)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("Fill consumer stopped")
				return
			}
			c.logger.Error("Fill consumer fetch error", zap.Error(err))
			continue
		}

		if retryable := c.processOne(ctx, msg, processed); retryable {
			// Transient failure — don't commit. Same message will be
			// re-fetched on the next loop iteration.
			c.logger.Warn("Fill consumer: leaving offset un-committed for retry",
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset))
			// Tiny back-off so we don't hot-loop on a persistent failure.
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		if cmErr := c.reader.CommitMessages(ctx, msg); cmErr != nil {
			c.logger.Warn("Fill consumer: commit failed (will redeliver, handler is idempotent)",
				zap.Error(cmErr))
		}
	}
}

// processOne handles a single message and returns true ONLY when the caller
// should leave the offset un-committed for retry. All non-retryable outcomes
// (success, parse failure, idempotent duplicate) return false so the offset
// advances.
func (c *FillConsumer) processOne(ctx context.Context, msg kafka.Message, processed map[string]bool) (retryable bool) {
	var event FillEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		// Poison message — can never be processed. Commit and move on.
		c.logger.Warn("Fill event unmarshal failed — dropping",
			zap.Error(err), zap.String("raw", string(msg.Value)))
		return false
	}

	// CQRS path — projector owns DB writes via one atomic TX. Failure here
	// is treated as retryable: the next attempt re-runs the same TX with
	// the same (signal_id, event_seq), and the UNIQUE constraint makes a
	// successful prior write idempotent.
	//
	// Idempotency contract: when projector says `applied=false` (event seen
	// before — DB UNIQUE (signal_id, event_seq) deduped it), the in-memory
	// state was ALREADY updated on the first arrival. Re-applying would
	// regress good state: e.g. a stale SL_PLACED with a bogus trigger could
	// overwrite the correct in-memory CurrentSL while the DB stays correct
	// (the projector's `event_seq < $2` guard protects DB but applyInMemory
	// has no such guard). So we skip in-memory on duplicate.
	if c.decisionLogEnabled && c.projector != nil {
		applied, err := c.projector.Apply(context.Background(), &event)
		if err != nil {
			c.logger.Error("Projector.Apply failed (retryable)",
				zap.String("type", event.Type),
				zap.String("signal_id", event.ResolvedSignalID()),
				zap.Error(err))
			return true // RETRY
		}
		if !applied {
			c.logger.Debug("Projector.Apply: duplicate event — skipping in-memory too",
				zap.String("signal_id", event.ResolvedSignalID()),
				zap.Int64("event_seq", event.ResolvedEventSeq()))
			return false // commit, do NOT re-apply in-memory
		}
	}

	// First-time event (or legacy mode): drive in-memory state for events
	// the portfolio cares about. The projector has already committed DB
	// (CQRS path) or applyInMemory will write DB directly (legacy path).
	c.applyInMemory(ctx, &event, processed)
	return false
}

// applyInMemory drives the in-memory portfolio for the events that affect
// trailing-SL math, allocator decisions, or cooldown state. In LEGACY mode
// it also persists the DB side-effects (UpdatePositionFill / UpdatePositionSL
// / UpdatePortfolioState) since the projector is bypassed.
func (c *FillConsumer) applyInMemory(ctx context.Context, event *FillEvent, processed map[string]bool) {
	switch event.Type {

	// ── Entry fills ────────────────────────────────────────────────────
	case "FILL_CONFIRMED", "FILL_UPDATE", "ENTRY_FILLED", "ENTRY_PARTIAL_FILL":
		if event.EventID != "" {
			if processed[event.EventID] {
				return
			}
			processed[event.EventID] = true
		}
		fillPrice := event.ResolvedFillPrice()
		fillQty := event.ResolvedFillQty()
		isFull := event.Type == "FILL_CONFIRMED" || event.Type == "ENTRY_FILLED" ||
			fillQty >= event.ExpectedQty

		c.logger.Info("Entry fill — syncing in-memory portfolio",
			zap.String("type", event.Type),
			zap.String("symbol", event.Symbol),
			zap.Float64("fill_price", fillPrice),
			zap.Int32("filled", fillQty),
			zap.Int32("expected", event.ExpectedQty),
			zap.Bool("is_full", isFull))

		c.portfolioMgr.ProcessFillEvent(
			event.StrategyID, event.Symbol,
			fillPrice, fillQty, event.ExpectedQty, isFull,
			c.slMgr, c.slPct,
		)

		// Legacy-mode DB persistence (CQRS projector already wrote authoritatively).
		if mp, ok := c.publisher.(*ManthanPublisher); ok {
			if p, pok := c.portfolioMgr.Get(event.StrategyID); pok {
				if isFull && !c.decisionLogEnabled {
					var slPrice float64
					if pos, ok := p.Positions[event.Symbol]; ok {
						slPrice = pos.CurrentSL
					}
					mp.UpdatePositionFill(ctx, event.StrategyID, event.Symbol,
						fillPrice, fillQty, slPrice)
				}
				// Portfolio summary refresh runs in both modes — active_count
				// changes whenever a position flips Active.
				mp.UpdatePortfolioState(ctx, p)
			}
		}

	// ── SL placement / modification — broker truth writeback ───────────
	//
	// CRITICAL: without this branch, the broker may DPR-clamp our requested
	// SL (e.g. 332.56 → 390) but in-memory and DB still show the requested
	// value. The next trail tick computes new_sl off the stale value and
	// emits an SL_MODIFY that LOWERS the broker stop. Yesterday's NATIONALUM
	// divergence was exactly this.
	case "SL_PLACED", "SL_MODIFIED":
		if event.SLTrigger <= 0 {
			// Defensive — old producer didn't include SLTrigger; skip.
			c.logger.Warn("SL event has no trigger price — skipping in-memory sync",
				zap.String("type", event.Type),
				zap.String("symbol", event.Symbol))
			return
		}
		c.logger.Info("Broker SL event — syncing rules-engine state",
			zap.String("type", event.Type),
			zap.String("symbol", event.Symbol),
			zap.Float64("broker_trigger", event.SLTrigger),
			zap.String("sl_broker_id", event.SLBrokerID))

		// 1. In-memory — the TSL tick handler ratchet check reads pos.CurrentSL.
		c.portfolioMgr.UpdateSLFromBroker(event.StrategyID, event.Symbol, event.SLTrigger)

		// 2. DB — only in legacy mode (projector writes authoritatively in CQRS).
		if !c.decisionLogEnabled {
			if mp, ok := c.publisher.(*ManthanPublisher); ok {
				mp.UpdatePositionSL(ctx, event.StrategyID, event.Symbol, event.SLTrigger)
			}
		}

	// ── Exits — driven primarily by projector in CQRS, but in-memory still
	//    needs the capital-rebalance + cooldown side-effects of ExitPosition.
	case "SL_FILLED", "EXIT_FILLED":
		fillPrice := event.ResolvedFillPrice()
		if fillPrice > 0 {
			c.portfolioMgr.ExitPosition(event.StrategyID, event.Symbol, fillPrice)
		}
		// Portfolio summary refresh — active_count drops, capital recomputed.
		if mp, ok := c.publisher.(*ManthanPublisher); ok {
			if p, pok := c.portfolioMgr.Get(event.StrategyID); pok {
				mp.UpdatePortfolioState(ctx, p)
			}
		}

	// Everything else (RECONCILER_DRIFT_FIX, MANUAL_EXIT_DETECTED, etc.) is
	// projector-only — the in-memory portfolio doesn't have a meaningful
	// projection for those (or it's redundant with the projector's work).
	default:
		// no-op
	}
}

func (c *FillConsumer) Stop() error {
	return c.reader.Close()
}
