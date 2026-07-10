package manthan

import (
	"context"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
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
	strategyFn   func(strategyID string) *types.UserStrategy // get strategy by ID
	logger       *zap.Logger
}

func NewTickHandler(
	slMgr *TrailingSLManager,
	portfolioMgr *PortfolioManager,
	orderGen *OrderGenerator,
	publisher OrderPublisher,
	strategyFn func(strategyID string) *types.UserStrategy,
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
		// Tight critical section: read pos under Lock, let slMgr.ProcessTick
		// mutate pos.CurrentSL / pos.HighSinceEntry in-place under the same
		// lock, snapshot what we need for the post-release work, release.
		// Releasing before pm.ExitPosition is essential — that method
		// re-acquires Mu.Lock(), and sync.RWMutex is non-reentrant
		// (nested lock would deadlock the goroutine).
		portfolio.Mu.Lock()
		pos, ok := portfolio.Positions[symbol]
		if !ok || !pos.Active || pos.State != types.StateActive {
			portfolio.Mu.Unlock()
			continue
		}
		strategy := h.strategyFn(portfolio.StrategyID)
		if strategy == nil {
			portfolio.Mu.Unlock()
			continue
		}
		update := h.slMgr.ProcessTick(pos, ltp, strategy.StopLossPct, strategy.TrailingSLPct)
		strategyID := portfolio.StrategyID
		posSnap := *pos // value copy for use after Unlock
		portfolio.Mu.Unlock()

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
			pnl := h.portfolioMgr.ExitPosition(strategyID, symbol, update.ExitPrice)
			order := h.orderGen.GenerateSLExit(*strategy, &posSnap, update.ExitPrice, pnl)
			if err := h.publisher.PublishSLExit(ctx, order); err != nil {
				h.logger.Error("Failed to publish SL exit",
					zap.String("symbol", symbol),
					zap.Error(err),
				)
			}

			// Capital was just updated inside ExitPosition; re-read under
			// RLock for the log so the printed value reflects the new state.
			portfolio.Mu.RLock()
			newCapital := portfolio.CurrentCapital
			portfolio.Mu.RUnlock()

			h.logger.Info("MANTHAN SL triggered — position exited",
				zap.String("user", strategy.UserID),
				zap.String("symbol", symbol),
				zap.Float64("entry", posSnap.EntryPrice),
				zap.Float64("exit", update.ExitPrice),
				zap.Float64("pnl", pnl),
				zap.Float64("new_capital", newCapital),
			)
		}
	}
}
