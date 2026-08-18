package manthan

import (
	"context"
	"time"

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
		if ok && pos.Active && pos.State == types.StateExitPending &&
			!pos.ExitPendingSince.IsZero() && time.Since(pos.ExitPendingSince) > ExitPendingTTL {
			// Exit command never confirmed — see ExitPendingTTL. Resume trailing.
			h.logger.Warn("EXIT_PENDING timed out without broker confirmation — reverting to ACTIVE (exit will re-fire under a fresh id)",
				zap.String("strategy_id", portfolio.StrategyID),
				zap.String("symbol", symbol),
				zap.Duration("pending_for", time.Since(pos.ExitPendingSince)),
				zap.Float64("current_sl", pos.CurrentSL))
			pos.State = types.StateActive
			pos.ExitPendingSince = time.Time{}
		}
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
			// FIX F: persist the ratcheted trail so a restart resumes at this
			// exact level. posSnap holds the just-updated CurrentSL/High/LastTrail.
			// Best-effort + safe-if-stale (SL only moves up).
			_ = h.publisher.PersistTrail(ctx, strategyID, posSnap)

		case SLTriggered:
			// Trail crossed → command the exit, but do NOT book it. The
			// position stays on the books (EXIT_PENDING) until the
			// position.events POSITION_EXITED confirmation arrives from the
			// positions service — i.e. until the sell REALLY filled at the
			// broker. Booking at trail-cross (the old behavior) exited 4
			// positions on 2026-08-18 whose SL_EXIT orders died at
			// trade-execution (dead auth / DLQ) — the book said gone, the
			// broker still held them, and rehydrate+caps worked off fiction.
			// EXIT_PENDING stops further ticking (the loop gates on
			// StateActive) so the exit command fires exactly once per cross;
			// capital/slot release and PersistExit now happen only in the
			// confirmed-exit callback (wire.go / cooldown consumer).
			h.portfolioMgr.MarkExitPending(strategyID, symbol)
			pnlEstimate := (update.ExitPrice - posSnap.EntryPrice) * float64(posSnap.Quantity)
			order := h.orderGen.GenerateSLExit(*strategy, &posSnap, update.ExitPrice, pnlEstimate)
			if err := h.publisher.PublishSLExit(ctx, order); err != nil {
				h.logger.Error("Failed to publish SL exit",
					zap.String("symbol", symbol),
					zap.Error(err),
				)
			}
			pnl := pnlEstimate

			// Capital is NOT yet released — that happens on the confirmed
			// exit. Log the current (unchanged) figure for context.
			portfolio.Mu.RLock()
			newCapital := portfolio.CurrentCapital
			portfolio.Mu.RUnlock()

			h.logger.Info("MANTHAN SL triggered — exit ORDERED (pending broker confirmation)",
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
