package manthan

import (
	"context"

	"go.uber.org/zap"
)

// TickHandler processes real-time LTP ticks for all active MANTHAN positions.
// Called from the websocket tick feed for every price update.
//
// Responsibilities:
//   - Update trailing SL for each active position
//   - Publish SL modify orders when trail triggers
//   - Publish exit orders when SL is hit
//   - Update portfolio capital on exits (real-time rebalancing)
type TickHandler struct {
	slMgr        *TrailingSLManager
	portfolioMgr *PortfolioManager
	orderGen     *OrderGenerator
	publisher    OrderPublisher
	strategyFn   func(strategyID string) *UserStrategy // get strategy by ID
	logger       *zap.Logger
}

func NewTickHandler(
	slMgr *TrailingSLManager,
	portfolioMgr *PortfolioManager,
	orderGen *OrderGenerator,
	publisher OrderPublisher,
	strategyFn func(strategyID string) *UserStrategy,
	logger *zap.Logger,
) *TickHandler {
	return &TickHandler{
		slMgr:        slMgr,
		portfolioMgr: portfolioMgr,
		orderGen:     orderGen,
		publisher:    publisher,
		strategyFn:   strategyFn,
		logger:       logger,
	}
}

// ProcessTick handles a single LTP tick for a symbol.
// Called for every tick from the websocket feed.
func (h *TickHandler) ProcessTick(ctx context.Context, symbol string, ltp float64) {
	for _, portfolio := range h.portfolioMgr.AllPortfolios() {
		pos, ok := portfolio.Positions[symbol]
		if !ok || !pos.Active || pos.State != StateActive {
			continue // skip PENDING_ENTRY and EXITED positions
		}

		strategy := h.strategyFn(portfolio.StrategyID)
		if strategy == nil {
			continue
		}

		update := h.slMgr.ProcessTick(pos, ltp, strategy.StopLossPct, strategy.TrailingSLPct)

		switch update.Action {
		case SLModify:
			order := h.orderGen.GenerateSLModify(*strategy, update)
			if err := h.publisher.PublishSLModify(ctx, order); err != nil {
				h.logger.Error("Failed to publish SL modify",
					zap.String("symbol", symbol),
					zap.Error(err),
				)
			}

		case SLTriggered:
			pnl := h.portfolioMgr.ExitPosition(portfolio.StrategyID, symbol, update.ExitPrice)
			order := h.orderGen.GenerateSLExit(*strategy, pos, update.ExitPrice, pnl)
			if err := h.publisher.PublishSLExit(ctx, order); err != nil {
				h.logger.Error("Failed to publish SL exit",
					zap.String("symbol", symbol),
					zap.Error(err),
				)
			}

			h.logger.Info("MANTHAN SL triggered — position exited",
				zap.String("user", strategy.UserID),
				zap.String("symbol", symbol),
				zap.Float64("entry", pos.EntryPrice),
				zap.Float64("exit", update.ExitPrice),
				zap.Float64("pnl", pnl),
				zap.Float64("new_capital", portfolio.CurrentCapital),
			)
		}
	}
}
