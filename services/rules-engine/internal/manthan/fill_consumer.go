package manthan

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// FillConsumer reads broker events from manthan.execution.events and projects
// them onto the rules-engine state.
//
// Two modes:
//
//   Legacy (DecisionLogEnabled=false):
//     - Handles FILL_CONFIRMED/FILL_UPDATE only
//     - Updates in-memory portfolio + writes manthan_positions optimistically
//       (preserved exactly to avoid behavior change during cutover)
//
//   CQRS (DecisionLogEnabled=true):
//     - Handles ALL event types (ENTRY_FILLED, ENTRY_REJECTED, SL_PLACED,
//       SL_MODIFIED, SL_FILLED, EXIT_FILLED, RECONCILER_DRIFT_FIX, …)
//     - Routes every event through PositionProjector.Apply for an atomic
//       INSERT manthan_position_events + UPSERT manthan_positions +
//       UPDATE manthan_signal_decisions transaction
//     - Idempotency via ON CONFLICT (signal_id, event_seq) DO NOTHING
//     - Still updates in-memory portfolio so TSL / cooldown / allocator stay current
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
	c.logger.Info("Fill consumer started", zap.String("topic", c.reader.Config().Topic))

	// Idempotency: track processed event IDs (in-memory, resets on restart)
	processed := make(map[string]bool)

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("Fill consumer stopped")
				return
			}
			c.logger.Error("Fill consumer read error", zap.Error(err))
			continue
		}

		var event FillEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Warn("Fill event unmarshal failed", zap.Error(err))
			continue
		}

		// CQRS path — when DecisionLogEnabled=true, every event runs through
		// the projector's atomic TX. The projector handles ALL event types
		// (entry, SL, exit, reconciler) including the idempotency gate via
		// (signal_id, event_seq). The in-memory portfolio is still updated
		// alongside so TSL / cooldown / allocator stay current.
		if c.decisionLogEnabled && c.projector != nil {
			applied, err := c.projector.Apply(context.Background(), &event)
			if err != nil {
				c.logger.Error("Projector.Apply failed",
					zap.String("type", event.Type),
					zap.String("signal_id", event.ResolvedSignalID()),
					zap.Error(err))
				// Don't continue — try the legacy path so we at least update
				// in-memory portfolio. But mark that DB projection failed for
				// observability.
			} else if !applied {
				c.logger.Debug("Projector.Apply: duplicate event (idempotent skip)",
					zap.String("signal_id", event.ResolvedSignalID()),
					zap.Int64("event_seq", event.ResolvedEventSeq()))
			}
			// For non-fill event types we're done — they don't drive in-memory
			// portfolio. (Entry-related events still need portfolio update
			// below; we let them fall through.)
			if !isPortfolioRelevantType(event.Type) {
				continue
			}
		}

		// Legacy path / portfolio update path: only fill events drive
		// in-memory state; other event types are projector-only.
		if event.Type != "FILL_CONFIRMED" && event.Type != "FILL_UPDATE" &&
			event.Type != "ENTRY_FILLED" && event.Type != "ENTRY_PARTIAL_FILL" {
			continue
		}

		// Idempotency check (in-memory; restart safe via projector when CQRS on)
		if event.EventID != "" {
			if processed[event.EventID] {
				c.logger.Debug("Duplicate fill event skipped", zap.String("event_id", event.EventID))
				continue
			}
			processed[event.EventID] = true
		}

		// Normalize aliases — accept new + legacy field names
		fillPrice := event.ResolvedFillPrice()
		fillQty := event.ResolvedFillQty()
		isFull := event.Type == "FILL_CONFIRMED" || event.Type == "ENTRY_FILLED" ||
			fillQty >= event.ExpectedQty

		c.logger.Info("Fill event received — syncing reality",
			zap.String("type", event.Type),
			zap.String("symbol", event.Symbol),
			zap.Float64("fill_price", fillPrice),
			zap.Int32("filled", fillQty),
			zap.Int32("expected", event.ExpectedQty),
			zap.Bool("is_full", isFull))

		c.portfolioMgr.ProcessFillEvent(
			event.StrategyID,
			event.Symbol,
			fillPrice,
			fillQty,
			event.ExpectedQty,
			isFull,
			c.slMgr,
			c.slPct,
		)

		// Persist the fill into DB + Redis and refresh portfolio_state so a
		// restart rehydrates the real fill price (not the optimistic
		// LTP-at-signal-time) and active_count reflects the live count.
		// In CQRS mode the projector already wrote manthan_positions
		// transactionally — we only refresh portfolio_state and Redis here
		// (the optimistic UpdatePositionFill would re-stamp values that the
		// projector already authoritatively set).
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
				// Always refresh portfolio_state — active_count changes when
				// ProcessFillEvent flips a position's Active flag.
				mp.UpdatePortfolioState(ctx, p)
			}
		}
	}
}

// isPortfolioRelevantType reports whether an event should also drive the
// in-memory portfolio update (TSL state, active_count, allocator inputs).
// Non-fill events like SL_PLACED / RECONCILER_DRIFT_FIX are projector-only.
func isPortfolioRelevantType(t string) bool {
	switch t {
	case "FILL_CONFIRMED", "FILL_UPDATE",
		"ENTRY_FILLED", "ENTRY_PARTIAL_FILL",
		"SL_FILLED", "EXIT_FILLED":
		return true
	}
	return false
}

func (c *FillConsumer) Stop() error {
	return c.reader.Close()
}
