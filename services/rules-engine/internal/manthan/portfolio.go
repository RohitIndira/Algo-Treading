package manthan

import (
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PortfolioManager manages all users' MANTHAN portfolios.
// Thread-safe — accessed from both signal consumer and tick handler goroutines.
type PortfolioManager struct {
	mu         sync.RWMutex
	portfolios map[string]*Portfolio // strategyID → portfolio
	logger     *zap.Logger
}

func NewPortfolioManager(logger *zap.Logger) *PortfolioManager {
	return &PortfolioManager{
		portfolios: make(map[string]*Portfolio),
		logger:     logger,
	}
}

// GetOrCreate returns the portfolio for a strategy, creating it if needed.
func (pm *PortfolioManager) GetOrCreate(strategy UserStrategy) *Portfolio {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	p, ok := pm.portfolios[strategy.StrategyID]
	if ok {
		return p
	}

	p = &Portfolio{
		UserID:         strategy.UserID,
		StrategyID:     strategy.StrategyID,
		InitialCapital: strategy.TotalCapital,
		CurrentCapital: strategy.TotalCapital,
		MaxPositions:   strategy.MaxPositions,
		PerStockBase:   strategy.TotalCapital / float64(strategy.MaxPositions),
		Positions:      make(map[string]*Position),
		Cooldown:       make(map[string]*CooldownEntry),
	}
	pm.portfolios[strategy.StrategyID] = p
	return p
}

// Get returns a portfolio if it exists.
func (pm *PortfolioManager) Get(strategyID string) (*Portfolio, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	p, ok := pm.portfolios[strategyID]
	return p, ok
}

// Remove deletes a portfolio (when strategy is deleted).
func (pm *PortfolioManager) Remove(strategyID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.portfolios, strategyID)
}

// AllPortfolios returns a snapshot of all portfolios (for tick processing).
func (pm *PortfolioManager) AllPortfolios() []*Portfolio {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	out := make([]*Portfolio, 0, len(pm.portfolios))
	for _, p := range pm.portfolios {
		out = append(out, p)
	}
	return out
}

// AddPosition adds a new position to a portfolio after an order is placed.
//
// slMgr / slPct removed 2026-06-25 (audit finding #3) — SL initialisation
// happens in ProcessFillEvent once we have the real broker price, so passing
// the SL machinery here was dead weight that lied about behaviour.
func (pm *PortfolioManager) AddPosition(strategyID string, alloc AllocationResult) {
	pm.mu.RLock()
	p, ok := pm.portfolios[strategyID]
	pm.mu.RUnlock()
	if !ok {
		return
	}

	pos := &Position{
		Symbol:      alloc.Symbol,
		ISIN:        alloc.ISIN,
		Industry:    alloc.Industry,
		MCapBucket:  alloc.MCapBucket,
		IndexName:   alloc.IndexName,
		EntryPrice:  alloc.EntryPrice, // provisional — overwritten by fill confirmation
		EntryTime:   time.Now(),
		Quantity:    alloc.Quantity,
		InvestedAmt: alloc.PerCallActual,
		State:       StatePendingEntry, // NOT active until fill confirmed
		Active:      false,             // trailing SL disabled until ACTIVE
	}
	// Inner write — Mu guards Positions against concurrent external readers
	// (LTPFeed poll, allocator cap-check, publisher snapshot).
	p.Mu.Lock()
	p.Positions[alloc.Symbol] = pos
	p.Mu.Unlock()
}

// ProcessFillEvent handles both partial and full fill events.
// Partial: updates price/qty, stays in PARTIALLY_FILLED
// Full: transitions to ACTIVE, enables SL + trailing
func (pm *PortfolioManager) ProcessFillEvent(strategyID, symbol string, avgFillPrice float64, filledQty, expectedQty int32, isFull bool, slMgr *TrailingSLManager, slPct float64) {
	pm.mu.RLock()
	p, ok := pm.portfolios[strategyID]
	pm.mu.RUnlock()
	if !ok {
		return
	}

	// Inner mutation — Mu guards Positions/CurrentSL against external readers.
	p.Mu.Lock()
	defer p.Mu.Unlock()

	pos, ok := p.Positions[symbol]
	if !ok {
		return
	}

	// Idempotency: if already ACTIVE with same or higher qty, skip
	if pos.State == StateActive && pos.Quantity >= filledQty {
		return
	}

	// Overwrite with reality (weighted avg from broker — always authoritative)
	pos.EntryPrice = avgFillPrice
	pos.Quantity = filledQty
	pos.InvestedAmt = avgFillPrice * float64(filledQty)

	if isFull || filledQty >= expectedQty {
		// Full fill — activate position, enable SL + trailing
		pos.State = StateActive
		pos.Active = true
		slMgr.InitPosition(pos, slPct)

		pm.logger.Info("Position ACTIVE — full fill confirmed",
			zap.String("symbol", symbol),
			zap.Float64("fill_price", avgFillPrice),
			zap.Int32("qty", filledQty),
			zap.Float64("sl", pos.CurrentSL))
	} else {
		// Partial fill — update price/qty but keep locked
		pos.State = StatePartiallyFilled
		pos.Active = false

		pm.logger.Info("Position partially filled — waiting for rest",
			zap.String("symbol", symbol),
			zap.Float64("avg_price", avgFillPrice),
			zap.Int32("filled", filledQty),
			zap.Int32("expected", expectedQty))
	}
}

// ConfirmFill is a convenience wrapper for full fills (backward compat).
func (pm *PortfolioManager) ConfirmFill(strategyID, symbol string, avgFillPrice float64, filledQty int32, slMgr *TrailingSLManager, slPct float64) {
	pm.ProcessFillEvent(strategyID, symbol, avgFillPrice, filledQty, filledQty, true, slMgr, slPct)
}

// UpdateSLFromBroker syncs the in-memory SL trigger to whatever the broker
// actually has. Called from FillConsumer when SL_PLACED / SL_MODIFIED events
// arrive carrying the broker's accepted trigger price.
//
// Why this is critical: the broker may DPR-clamp our requested SL (e.g. we
// ask for 332.56, broker accepts 390 because that's the lowest legal stop
// within today's lower circuit). If we don't sync, the TickHandler's
// trailing-SL math keeps using our stale internal value, eventually emits
// an SL_MODIFY with new_sl below the broker's actual trigger, and trade-
// execution would LOWER the broker stop — widening risk on a long position.
//
// The trail ratchet check inside TrailingSLManager.ProcessTick
// (if newSL <= pos.CurrentSL → no action) does the right thing once
// pos.CurrentSL reflects broker reality. So all this function needs to do
// is overwrite CurrentSL when the broker reports back.
//
// HighSinceEntry and LastTrailLevel are intentionally NOT touched — those
// describe price history, not SL state. Touching them would corrupt the
// trail math.
//
// Idempotent: same broker trigger arriving twice is a no-op.
func (pm *PortfolioManager) UpdateSLFromBroker(strategyID, symbol string, brokerTrigger float64) {
	if brokerTrigger <= 0 {
		return
	}
	pm.mu.RLock()
	p, ok := pm.portfolios[strategyID]
	pm.mu.RUnlock()
	if !ok {
		return
	}

	p.Mu.Lock()
	defer p.Mu.Unlock()

	pos, ok := p.Positions[symbol]
	if !ok {
		return
	}
	// Float equality is a lie at the bit level — broker may report 82.10
	// while we hold 82.099999999. Use half-a-paise epsilon so legitimate
	// re-broadcasts of the same trigger are idempotent.
	const slEpsilon = 0.005
	if math.Abs(pos.CurrentSL-brokerTrigger) < slEpsilon {
		return
	}
	pm.logger.Info("In-memory SL synced from broker (closes rules-engine/broker divergence)",
		zap.String("strategy_id", strategyID),
		zap.String("symbol", symbol),
		zap.Float64("old_internal_sl", pos.CurrentSL),
		zap.Float64("new_broker_sl", brokerTrigger))
	pos.CurrentSL = brokerTrigger
}

// ExitPosition marks a position as exited after SL triggered.
// Adds to cooldown for re-entry logic. Returns realized P&L.
func (pm *PortfolioManager) ExitPosition(strategyID, symbol string, exitPrice float64) float64 {
	pm.mu.RLock()
	p, ok := pm.portfolios[strategyID]
	pm.mu.RUnlock()
	if !ok {
		return 0
	}

	p.Mu.Lock()
	defer p.Mu.Unlock()

	pos, ok := p.Positions[symbol]
	if !ok || !pos.Active {
		return 0
	}

	pos.Active = false
	realizedPnL := (exitPrice - pos.EntryPrice) * float64(pos.Quantity)

	// Adjust current capital for reinvestment
	p.CurrentCapital += realizedPnL

	// Recalc per-stock base (real-time capital rebalancing)
	p.PerStockBase = p.CurrentCapital / float64(p.MaxPositions)

	// Reentry guard against ATH ≈ 0 — if HighSinceEntry never got updated
	// (e.g. exit before any tick arrived for a fresh position), don't
	// install a 0-threshold cooldown that's always satisfied. Fall back
	// to exitPrice × 0.80 as a conservative floor.
	ath := pos.HighSinceEntry
	if ath <= 0 {
		ath = exitPrice
	}

	// Add to cooldown — stock must correct 20% from ATH before re-entry
	p.Cooldown[symbol] = &CooldownEntry{
		Symbol:       symbol,
		ATHAtExit:    ath,
		ExitPrice:    exitPrice,
		ExitTime:     time.Now(),
		ReentryBelow: ath * 0.80, // 20% below ATH
	}

	pm.logger.Info("Position exited",
		zap.String("user", p.UserID),
		zap.String("symbol", symbol),
		zap.Float64("entry", pos.EntryPrice),
		zap.Float64("exit", exitPrice),
		zap.Float64("pnl", realizedPnL),
		zap.Float64("new_capital", p.CurrentCapital),
		zap.Float64("new_per_stock", p.PerStockBase),
	)

	return realizedPnL
}

// ActiveSymbols returns all symbols with active positions across all portfolios.
// Used by websocket manager to know which ticks to process.
func (pm *PortfolioManager) ActiveSymbols() map[string]bool {
	pm.mu.RLock()
	portfolios := make([]*Portfolio, 0, len(pm.portfolios))
	for _, p := range pm.portfolios {
		portfolios = append(portfolios, p)
	}
	pm.mu.RUnlock()

	// Inner maps must be read under each Portfolio's own RLock —
	// pm.mu only protects the outer map, not Positions inside each.
	syms := make(map[string]bool)
	for _, p := range portfolios {
		p.Mu.RLock()
		for sym, pos := range p.Positions {
			if pos.Active {
				syms[sym] = true
			}
		}
		p.Mu.RUnlock()
	}
	return syms
}
