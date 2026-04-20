package paper

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

const (
	// wssGracePeriod: time without a WSS price before Redis fallback kicks in.
	wssGracePeriod = 5 * time.Second
	// redisPollInterval: how often the Redis PnL ticker runs.
	redisPollInterval = 2 * time.Second
	// redisTPCheckInterval: how often the Redis TP safety-net runs.
	// Catches TP hits that WSS ticks may have skipped over.
	redisTPCheckInterval = 1 * time.Second
)

// symbolMeta holds the exchange and instrument token needed to build a Redis market key.
type symbolMeta struct {
	exchange string // "NSE" or "BSE"
	token    int64  // StockCode / instrument token
}

// trailSLState tracks in-memory trailing stop-loss state for a paper order.
type trailSLState struct {
	bestPrice float64 // highest LTP seen for BUY; lowest for SELL
	currentSL float64 // current trailing SL price (moves with bestPrice)
	trailPct  float64 // trailing percentage (e.g. 0.5 → 0.5%)
}

// MLGroupCanceller cancels active multi-level exit groups.
// Implemented by *multilevel.Manager; defined here to avoid import cycles.
type MLGroupCanceller interface {
	CancelGroup(ctx context.Context, entryOrderID uuid.UUID)
	CancelGroupsBySymbol(ctx context.Context, userID string, symbol string)
}

// PaperTradeMonitor is the SL/TP engine and live PnL broadcaster.
//
// Price source priority:
//   - WSS (PaperMarketClient):  SL/TP checks + PnL broadcast — always preferred
//   - Redis (RedisPriceClient): PnL broadcast + SL/TP safety-net — fallback when WSS is silent
type PaperTradeMonitor struct {
	repo         repository.OrderRepository
	paperExec    *executor.PaperOrderExecutor
	wsServer     *PaperWSServer
	marketClient *PaperMarketClient
	redisPrices  *RedisPriceClient // nil-safe; Redis PnL fallback

	// mlCanceller cancels multi-level groups on force-exit.
	// Set via SetMLCanceller; nil-safe.
	mlCanceller MLGroupCanceller

	// symbol → open orders cache (refreshed from DB on change events)
	orderCache   map[string][]*models.Order
	symbolMeta   map[string]symbolMeta // exchange+token per symbol — same cacheMu
	tokenToSym   map[string]string     // numeric token string → canonical DB symbol
	cacheMu      sync.RWMutex

	// latest known LTP and WSS activity — protected by priceMu
	latestLTP   map[string]float64
	wssLastSeen map[string]time.Time
	priceMu     sync.RWMutex

	// track orders currently being exited to avoid duplicates (sync.Map[uuid.UUID → bool])
	exiting sync.Map

	// trailing SL state per order — protected by trailMu
	trailStates map[uuid.UUID]*trailSLState
	trailMu     sync.Mutex
}

// SetMLCanceller wires the multi-level group canceller.
// Must be called before any ForceExit operations.
func (m *PaperTradeMonitor) SetMLCanceller(c MLGroupCanceller) {
	m.mlCanceller = c
}

// NewPaperTradeMonitor creates a new monitor.
// redisPrices may be nil; pass nil to disable the Redis PnL fallback.
func NewPaperTradeMonitor(
	repo repository.OrderRepository,
	paperExec *executor.PaperOrderExecutor,
	wsServer *PaperWSServer,
	marketClient *PaperMarketClient,
	redisPrices *RedisPriceClient,
) *PaperTradeMonitor {
	return &PaperTradeMonitor{
		repo:         repo,
		paperExec:    paperExec,
		wsServer:     wsServer,
		marketClient: marketClient,
		redisPrices:  redisPrices,
		orderCache:   make(map[string][]*models.Order),
		symbolMeta:   make(map[string]symbolMeta),
		tokenToSym:   make(map[string]string),
		latestLTP:    make(map[string]float64),
		wssLastSeen:  make(map[string]time.Time),
		trailStates:  make(map[uuid.UUID]*trailSLState),
	}
}

// Initialize loads all active paper orders from DB and subscribes their symbols.
// If a RedisPriceClient is configured, it also starts the Redis PnL fallback ticker.
func (m *PaperTradeMonitor) Initialize(ctx context.Context) error {
	orders, err := m.repo.GetAllActivePaperOrders(ctx)
	if err != nil {
		return fmt.Errorf("paper monitor init: %w", err)
	}
	log.Printf("[paper-monitor] Loaded %d active paper orders on startup", len(orders))
	for _, o := range orders {
		slType := "<nil>"
		if o.StopLossType != nil {
			slType = *o.StopLossType
		}
		log.Printf("[paper-monitor]   order=%s sym=%-12s side=%s entry=%.2f sl=%.2f tp=%.2f slType=%s qty=%d",
			o.OrderID, o.Symbol, o.OrderSide, safeF(o.FilledPrice), safeF(o.StopLoss), safeF(o.TakeProfit), slType, o.FilledQuantity)
	}

	m.cacheMu.Lock()
	for _, o := range orders {
		// Deduplicate: skip if already registered via AddOrder during startup window.
		duplicate := false
		for _, existing := range m.orderCache[o.Symbol] {
			if existing.OrderID == o.OrderID {
				duplicate = true
				break
			}
		}
		if duplicate {
			log.Printf("[paper-monitor] Skipping duplicate order %s for %s (already tracked)", o.OrderID, o.Symbol)
			continue
		}
		m.orderCache[o.Symbol] = append(m.orderCache[o.Symbol], o)
		if _, ok := m.symbolMeta[o.Symbol]; !ok {
			m.symbolMeta[o.Symbol] = symbolMeta{
				exchange: string(o.Exchange),
				token:    o.StockCode,
			}
		}
		if o.StockCode > 0 {
			tok := strconv.FormatInt(o.StockCode, 10)
			m.tokenToSym[tok] = o.Symbol
		}

		// Initialise trailing SL state for TRAILING orders loaded from DB on startup.
		// Two ways to detect a trailing order:
		//   a) stop_loss_type = 'TRAILING' in DB (saved correctly by new Create)
		//   b) trailing_sl_pct > 0 (non-zero broker-modify threshold also implies trailing)
		// Legacy orders created before stop_loss_type was persisted show 'FIXED' in DB;
		// the SQL migration "UPDATE orders SET stop_loss_type='TRAILING' WHERE ..." fixes those.
		isTrailing := (o.StopLossType != nil && *o.StopLossType == "TRAILING") ||
			(o.TrailingSLPct != nil && *o.TrailingSLPct > 0)
		if isTrailing {
			entry := safeF(o.FilledPrice)
			bestPrice := entry
			// If a highest_price was persisted to DB, resume from there instead of entry.
			if o.HighestPrice != nil && *o.HighestPrice > bestPrice {
				bestPrice = *o.HighestPrice
			}
			currentSL := safeF(o.StopLoss)
			// Derive the trailing distance from the bestPrice↔SL relationship.
			// BUY: slDistancePct = (bestPrice - SL) / bestPrice * 100
			// SELL: slDistancePct = (SL - bestPrice) / bestPrice * 100
			var slDistancePct float64
			if bestPrice > 0 && currentSL > 0 {
				if o.OrderSide == models.OrderSideBuy {
					slDistancePct = (bestPrice - currentSL) / bestPrice * 100
				} else {
					slDistancePct = (currentSL - bestPrice) / bestPrice * 100
				}
			}
			if slDistancePct > 0 {
				m.trailMu.Lock()
				m.trailStates[o.OrderID] = &trailSLState{
					bestPrice: bestPrice,
					currentSL: currentSL,
					trailPct:  slDistancePct,
				}
				m.trailMu.Unlock()
				log.Printf("[paper-monitor] Restored trailing SL state for order %s (trail=%.2f%% bestPrice=%.2f currentSL=%.2f)",
					o.OrderID, slDistancePct, bestPrice, currentSL)
			}
		}
	}
	m.cacheMu.Unlock()

	// Subscribe by symbol name AND numeric token for each symbol.
	// The server accepts both (see socket.md §4). Sending both maximises chances
	// of a successful subscription even when one form isn't recognised.
	var subs []string
	seen := map[string]bool{}
	m.cacheMu.RLock()
	for sym, meta := range m.symbolMeta {
		if !seen[sym] {
			subs = append(subs, sym)
			seen[sym] = true
		}
		if meta.token > 0 {
			tok := strconv.FormatInt(meta.token, 10)
			if !seen[tok] {
				subs = append(subs, tok)
				seen[tok] = true
			}
		}
	}
	m.cacheMu.RUnlock()
	if len(subs) > 0 {
		m.marketClient.Subscribe(subs)
	}

	if m.redisPrices != nil {
		go m.runRedisPnLTicker(ctx)
		go m.runRedisTPSafetyNet(ctx)
		log.Printf("[paper-monitor] Redis fallback started (poll=%s, wss-grace=%s, sl-tp-safety=%s)",
			redisPollInterval, wssGracePeriod, redisTPCheckInterval)
	}

	return nil
}

// AddOrder registers a newly filled paper order so SL/TP is monitored.
func (m *PaperTradeMonitor) AddOrder(order *models.Order) {
	m.cacheMu.Lock()
	m.orderCache[order.Symbol] = append(m.orderCache[order.Symbol], order)
	if _, ok := m.symbolMeta[order.Symbol]; !ok {
		m.symbolMeta[order.Symbol] = symbolMeta{
			exchange: string(order.Exchange),
			token:    order.StockCode,
		}
	}
	if order.StockCode > 0 {
		m.tokenToSym[strconv.FormatInt(order.StockCode, 10)] = order.Symbol
	}
	m.cacheMu.Unlock()

	// Initialise trailing SL state for TRAILING orders.
	// TrailingSLPct is the broker-modify threshold, NOT the SL distance — derive
	// the trailing distance from the fill price and initial stop loss instead.
	if order.StopLossType != nil && *order.StopLossType == "TRAILING" {
		entry := safeF(order.FilledPrice)
		initialSL := safeF(order.StopLoss)
		var slDistancePct float64
		if entry > 0 && initialSL > 0 {
			if order.OrderSide == models.OrderSideBuy {
				slDistancePct = (entry - initialSL) / entry * 100
			} else {
				slDistancePct = (initialSL - entry) / entry * 100
			}
		}
		if slDistancePct > 0 {
			m.trailMu.Lock()
			m.trailStates[order.OrderID] = &trailSLState{
				bestPrice: entry,
				currentSL: initialSL,
				trailPct:  slDistancePct,
			}
			m.trailMu.Unlock()
			log.Printf("[paper-monitor] Trailing SL enabled for order %s (trail=%.2f%% initialSL=%.2f)",
				order.OrderID, slDistancePct, initialSL)
		}
	}

	subs := []string{order.Symbol}
	if order.StockCode > 0 {
		subs = append(subs, strconv.FormatInt(order.StockCode, 10))
	}
	m.marketClient.Subscribe(subs)
	log.Printf("[paper-monitor] Tracking new paper order %s for %s (SL=%.2f TP=%.2f)",
		order.OrderID, order.Symbol, safeF(order.StopLoss), safeF(order.TakeProfit))

	if m.wsServer != nil {
		m.wsServer.Broadcast(order.UserID, PaperUpdate{
			Type:   "new_order",
			UserID: order.UserID,
			Order:  order,
		})
	}
}

// RemoveOrder removes a paper order from the in-memory cache and cleans up its
// trailing SL state. Called when an ML group fully completes so the order is no
// longer monitored (prevents "zero fill data" warn-spam after all levels exit).
func (m *PaperTradeMonitor) RemoveOrder(orderID uuid.UUID) {
	m.cacheMu.Lock()
	var symbol string
	var meta symbolMeta
	for sym, orders := range m.orderCache {
		remaining := make([]*models.Order, 0, len(orders))
		for _, o := range orders {
			if o.OrderID == orderID {
				symbol = sym
				meta = m.symbolMeta[sym]
			} else {
				remaining = append(remaining, o)
			}
		}
		if symbol == sym {
			if len(remaining) == 0 {
				delete(m.orderCache, sym)
				delete(m.symbolMeta, sym)
			} else {
				m.orderCache[sym] = remaining
			}
			break
		}
	}
	m.cacheMu.Unlock()

	if symbol == "" {
		return // order was not in cache
	}

	// Unsubscribe from market feed if no more orders for this symbol.
	m.cacheMu.RLock()
	stillTracked := len(m.orderCache[symbol]) > 0
	m.cacheMu.RUnlock()
	if !stillTracked {
		unsubs := []string{symbol}
		tok := ""
		if meta.token > 0 {
			tok = strconv.FormatInt(meta.token, 10)
			unsubs = append(unsubs, tok)
		}
		m.marketClient.Unsubscribe(unsubs)
		m.priceMu.Lock()
		delete(m.latestLTP, symbol)
		delete(m.wssLastSeen, symbol)
		m.priceMu.Unlock()
		if tok != "" {
			m.cacheMu.Lock()
			delete(m.tokenToSym, tok)
			m.cacheMu.Unlock()
		}
	}

	m.trailMu.Lock()
	delete(m.trailStates, orderID)
	m.trailMu.Unlock()

	m.exiting.Delete(orderID)
}

// UpdateCachedOrderQty updates the in-memory FilledQuantity for a paper order
// after a partial ML exit so that subsequent PnL broadcasts use the remaining qty.
func (m *PaperTradeMonitor) UpdateCachedOrderQty(orderID uuid.UUID, remainingQty int32) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	for _, orders := range m.orderCache {
		for _, o := range orders {
			if o.OrderID == orderID {
				o.FilledQuantity = remainingQty
				return
			}
		}
	}
}

// UpdateCachedOrderSL updates the in-memory StopLoss for a paper order after a
// partial ML TP exit moves the effective stop to breakeven or a prior TP price.
// This ensures the regular SL monitor uses the new (tighter/safer) stop level.
func (m *PaperTradeMonitor) UpdateCachedOrderSL(orderID uuid.UUID, newSL float64) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	for _, orders := range m.orderCache {
		for _, o := range orders {
			if o.OrderID == orderID {
				o.StopLoss = &newSL
				return
			}
		}
	}
}

// advanceTrailingSL updates the trailing SL for an order based on the new LTP.
// Returns the effective SL price and whether it was updated this tick.
// If the order has no trailing SL state, returns order.StopLoss unchanged with updated=false.
func (m *PaperTradeMonitor) advanceTrailingSL(order *models.Order, ltp float64) (effectiveSL float64, slUpdated bool) {
	m.trailMu.Lock()
	ts, ok := m.trailStates[order.OrderID]
	if !ok {
		m.trailMu.Unlock()
		return safeF(order.StopLoss), false
	}

	if order.OrderSide == models.OrderSideBuy && ltp > ts.bestPrice {
		ts.bestPrice = ltp
		newSL := ltp * (1 - ts.trailPct/100)
		if newSL > ts.currentSL {
			ts.currentSL = newSL
			slUpdated = true
		}
	} else if order.OrderSide == models.OrderSideSell && ltp < ts.bestPrice {
		ts.bestPrice = ltp
		newSL := ltp * (1 + ts.trailPct/100)
		if newSL < ts.currentSL || ts.currentSL == 0 {
			ts.currentSL = newSL
			slUpdated = true
		}
	}

	effectiveSL = ts.currentSL
	m.trailMu.Unlock()

	if slUpdated {
		newSL := effectiveSL
		orderID := order.OrderID
		// Update the in-memory order so REST fetches return the current TSL immediately.
		m.UpdateCachedOrderSL(orderID, newSL)
		go func() {
			m.repo.UpdateTrailingSL(context.Background(), orderID, newSL)
			m.repo.RecordExecutionEvent(context.Background(), orderID, "PAPER_TRAILING_SL_UPDATED", map[string]interface{}{
				"new_sl": newSL,
				"ltp":    ltp,
			})
		}()
		log.Printf("[paper-monitor] Trailing SL updated for %s: SL=%.2f (ltp=%.2f)", orderID, newSL, ltp)
	}

	return effectiveSL, slUpdated
}

// OnPriceUpdate is called by PaperMarketClient on every new LTP from WSS.
// This is the primary price source and triggers SL/TP exits.
//
// The enhanced-stream server sometimes echoes back the numeric instrument token as the
// "symbol" field when the subscription was made by token. The tokenToSym reverse-lookup
// maps numeric tokens → canonical DB symbol so these ticks are still processed correctly.
func (m *PaperTradeMonitor) OnPriceUpdate(symbol string, ltp float64) {
	m.cacheMu.RLock()
	orders := m.orderCache[symbol]
	if len(orders) == 0 {
		// The enhanced-stream server may return the symbol with an exchange suffix
		// (e.g. "COROMANDEL (NSE)") when subscribed via numeric token, or echo the
		// numeric token itself. Try both normalisation strategies in order.
		if canonical, ok := m.tokenToSym[symbol]; ok {
			// Numeric token → canonical DB symbol
			symbol = canonical
			orders = m.orderCache[symbol]
		} else if norm := normaliseSymbol(symbol); norm != symbol {
			// Strip exchange suffix like " (NSE)" / " (BSE)" / "-EQ" etc.
			symbol = norm
			orders = m.orderCache[symbol]
		}
	}
	m.cacheMu.RUnlock()

	if len(orders) == 0 {
		return
	}

	// Mark WSS as active and cache the latest price
	m.priceMu.Lock()
	m.wssLastSeen[symbol] = time.Now()
	m.latestLTP[symbol] = ltp
	m.priceMu.Unlock()

	ctx := context.Background()

	for _, order := range orders {
		if _, alreadyExiting := m.exiting.Load(order.OrderID); alreadyExiting {
			continue
		}
		// Skip orders that never completed their fill (corrupted state guard).
		if order.FilledQuantity == 0 || order.FilledPrice == nil {
			log.Printf("[paper-monitor] WARN: skipping order %s (symbol=%s) with zero fill data — FilledQty=%d FilledPrice=%v",
				order.OrderID, order.Symbol, order.FilledQuantity, order.FilledPrice)
			continue
		}

		// Advance trailing SL before evaluating exit conditions.
		sl, slMoved := m.advanceTrailingSL(order, ltp)
		tp := safeF(order.TakeProfit)

		// SL/TP check — WSS only
		var reason string
		if sl > 0 && order.OrderSide == models.OrderSideBuy && ltp <= sl {
			reason = "STOP_LOSS"
		} else if sl > 0 && order.OrderSide == models.OrderSideSell && ltp >= sl {
			reason = "STOP_LOSS"
		} else if tp > 0 && order.OrderSide == models.OrderSideBuy && ltp >= tp {
			reason = "TAKE_PROFIT"
		} else if tp > 0 && order.OrderSide == models.OrderSideSell && ltp <= tp {
			reason = "TAKE_PROFIT"
		}

		if reason != "" {
			m.exiting.Store(order.OrderID, true)

			// Broadcast the triggering LTP as a final pnl_update BEFORE the exit so
			// the user can see the exact price that crossed SL/TP in the open positions view.
			if m.wsServer != nil {
				m.wsServer.Broadcast(order.UserID, PaperUpdate{
					Type:    "pnl_update",
					OrderID: order.OrderID.String(),
					Symbol:  symbol,
					LTP:     ltp,
					LivePnL: computePnL(order, ltp),
				})
			}

			go m.exitPosition(ctx, order, ltp, reason)
			continue
		}

		// Live PnL broadcast — include updated trailing SL so the UI stays in sync.
		livePnL := computePnL(order, ltp)
		var currentSL float64
		if slMoved {
			currentSL = sl
		}
		if m.wsServer != nil {
			m.wsServer.Broadcast(order.UserID, PaperUpdate{
				Type:      "pnl_update",
				OrderID:   order.OrderID.String(),
				Symbol:    symbol,
				LTP:       ltp,
				LivePnL:   livePnL,
				CurrentSL: currentSL,
			})
		}
	}
}

// runRedisPnLTicker polls Redis every redisPollInterval.
// For symbols where WSS has been silent longer than wssGracePeriod, it fetches the
// LTP from Redis and broadcasts a PnL update — it does NOT trigger SL/TP exits.
func (m *PaperTradeMonitor) runRedisPnLTicker(ctx context.Context) {
	ticker := time.NewTicker(redisPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.tickRedisPnL(ctx)
		}
	}
}

func (m *PaperTradeMonitor) tickRedisPnL(ctx context.Context) {
	// Snapshot orders without holding the lock during Redis calls
	type snap struct {
		symbol string
		meta   symbolMeta
		orders []*models.Order
	}

	m.cacheMu.RLock()
	snaps := make([]snap, 0, len(m.orderCache))
	for sym, orders := range m.orderCache {
		if len(orders) == 0 {
			continue
		}
		cp := make([]*models.Order, len(orders))
		copy(cp, orders)
		snaps = append(snaps, snap{sym, m.symbolMeta[sym], cp})
	}
	m.cacheMu.RUnlock()

	for _, s := range snaps {
		// Skip if WSS sent a price recently — WSS takes priority
		m.priceMu.RLock()
		lastSeen := m.wssLastSeen[s.symbol]
		m.priceMu.RUnlock()

		if time.Since(lastSeen) < wssGracePeriod {
			continue
		}

		if s.meta.token == 0 {
			continue // missing instrument token, cannot build Redis key
		}

		rCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ltp, err := m.redisPrices.GetLTP(rCtx, s.meta.exchange, s.meta.token)
		cancel()

		if err != nil || ltp <= 0 {
			continue
		}

		// Cache the latest price
		m.priceMu.Lock()
		m.latestLTP[s.symbol] = ltp
		m.priceMu.Unlock()

		// Broadcast PnL update — no SL/TP check (WSS only)
		for _, order := range s.orders {
			if _, alreadyExiting := m.exiting.Load(order.OrderID); alreadyExiting {
				continue
			}
			if order.FilledQuantity == 0 || order.FilledPrice == nil {
				continue
			}

			if m.wsServer != nil {
				m.wsServer.Broadcast(order.UserID, PaperUpdate{
					Type:    "pnl_update",
					OrderID: order.OrderID.String(),
					Symbol:  s.symbol,
					LTP:     ltp,
					LivePnL: computePnL(order, ltp),
				})
			}
		}
	}
}

// runRedisTPSafetyNet polls Redis every redisTPCheckInterval as a safety net for SL/TP
// exits that WSS may have skipped over (e.g. when the WSS connection is down or a tick
// is missed). Both SL and TP are checked; exiting.LoadOrStore prevents double-exits.
func (m *PaperTradeMonitor) runRedisTPSafetyNet(ctx context.Context) {
	if m.redisPrices == nil {
		return
	}
	ticker := time.NewTicker(redisTPCheckInterval)
	logTicker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	defer logTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkRedisSLTP(ctx)
		case <-logTicker.C:
			m.logRedisScan(ctx)
		}
	}
}

// logRedisScan emits a periodic log showing Redis LTPs for all monitored positions.
// This confirms the safety-net is active and Redis prices are being received.
func (m *PaperTradeMonitor) logRedisScan(ctx context.Context) {
	m.cacheMu.RLock()
	type entry struct {
		sym    string
		meta   symbolMeta
		orders []*models.Order
	}
	entries := make([]entry, 0, len(m.orderCache))
	for sym, orders := range m.orderCache {
		if len(orders) > 0 {
			cp := make([]*models.Order, len(orders))
			copy(cp, orders)
			entries = append(entries, entry{sym, m.symbolMeta[sym], cp})
		}
	}
	m.cacheMu.RUnlock()

	for _, e := range entries {
		if e.meta.token == 0 {
			continue
		}
		rCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		ltp, err := m.redisPrices.GetLTP(rCtx, e.meta.exchange, e.meta.token)
		cancel()
		if err != nil {
			log.Printf("[paper-monitor] Redis scan: %s → error: %v", e.sym, err)
			continue
		}
		for _, o := range e.orders {
			sl := m.effectiveSL(o)
			tp := safeF(o.TakeProfit)
			log.Printf("[paper-monitor] Redis scan: %-12s ltp=%.2f sl=%.2f tp=%.2f side=%s qty=%d",
				e.sym, ltp, sl, tp, o.OrderSide, o.FilledQuantity)
		}
	}
}

func (m *PaperTradeMonitor) checkRedisSLTP(ctx context.Context) {
	type snap struct {
		symbol string
		meta   symbolMeta
		orders []*models.Order
	}

	m.cacheMu.RLock()
	snaps := make([]snap, 0, len(m.orderCache))
	for sym, orders := range m.orderCache {
		if len(orders) == 0 {
			continue
		}
		cp := make([]*models.Order, len(orders))
		copy(cp, orders)
		snaps = append(snaps, snap{sym, m.symbolMeta[sym], cp})
	}
	m.cacheMu.RUnlock()

	for _, s := range snaps {
		if s.meta.token == 0 {
			continue
		}

		rCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		ltp, err := m.redisPrices.GetLTP(rCtx, s.meta.exchange, s.meta.token)
		cancel()
		if err != nil || ltp <= 0 {
			continue
		}

		for _, order := range s.orders {
			if _, alreadyExiting := m.exiting.Load(order.OrderID); alreadyExiting {
				continue
			}
			if order.FilledQuantity == 0 || order.FilledPrice == nil {
				continue
			}

			// For trailing SL, use the in-memory trailing state so the Redis check uses
			// the same (potentially advanced) SL level that WSS would have used.
			sl := m.effectiveSL(order)
			tp := safeF(order.TakeProfit)

			var reason string
			if tp > 0 {
				if order.OrderSide == models.OrderSideBuy && ltp >= tp {
					reason = "TAKE_PROFIT"
				} else if order.OrderSide == models.OrderSideSell && ltp <= tp {
					reason = "TAKE_PROFIT"
				}
			}
			if reason == "" && sl > 0 {
				if order.OrderSide == models.OrderSideBuy && ltp <= sl {
					reason = "STOP_LOSS"
				} else if order.OrderSide == models.OrderSideSell && ltp >= sl {
					reason = "STOP_LOSS"
				}
			}

			if reason == "" {
				continue
			}
			if _, alreadyExiting := m.exiting.LoadOrStore(order.OrderID, true); alreadyExiting {
				continue
			}
			exitAt := ltp
			if reason == "TAKE_PROFIT" {
				exitAt = tp // exit at the exact TP level for clean P&L
			}
			log.Printf("[paper-monitor] Redis SL/TP safety-net: %s %s ltp=%.2f sl=%.2f tp=%.2f → %s",
				order.OrderID, s.symbol, ltp, sl, tp, reason)
			go m.exitPosition(ctx, order, exitAt, reason)
		}
	}
}

// effectiveSL returns the current effective stop-loss price for an order.
// For trailing orders it uses the in-memory trailState (which may have advanced beyond the DB value).
// For regular orders it returns order.StopLoss.
func (m *PaperTradeMonitor) effectiveSL(order *models.Order) float64 {
	m.trailMu.Lock()
	ts, ok := m.trailStates[order.OrderID]
	if ok {
		sl := ts.currentSL
		m.trailMu.Unlock()
		return sl
	}
	m.trailMu.Unlock()
	return safeF(order.StopLoss)
}

// ForceExitAll exits all open paper positions for a given user at the best available price.
func (m *PaperTradeMonitor) ForceExitAll(ctx context.Context, userID string) error {
	allOrders, err := m.repo.GetFilledPaperOrdersByUser(ctx, userID)
	if err != nil {
		return fmt.Errorf("force exit: failed to fetch positions: %w", err)
	}

	// Only exit truly FILLED positions that have a valid entry price and haven't been exited yet.
	// This guards against accidentally exiting PENDING/SUBMITTED/RECEIVED orders that are not
	// yet open positions, or re-exiting orders that were already closed.
	var orders []*models.Order
	for _, o := range allOrders {
		if models.IsFilledStatus(o.Status) && o.FilledPrice != nil && o.PaperExitPrice == nil {
			orders = append(orders, o)
		}
	}

	log.Printf("[paper-monitor] Force-exiting %d positions for user %s (skipped %d non-filled/already-exited)",
		len(orders), userID, len(allOrders)-len(orders))

	var wg sync.WaitGroup
	for _, order := range orders {
		if _, already := m.exiting.LoadOrStore(order.OrderID, true); already {
			continue
		}

		// Cancel any active ML group for this order so the price-monitor goroutine
		// stops and doesn't try to place further exits after force-exit.
		if m.mlCanceller != nil {
			m.mlCanceller.CancelGroup(ctx, order.OrderID)
		}

		exitPrice := m.resolveExitPrice(ctx, order)
		wg.Add(1)
		go func(o *models.Order, price float64) {
			defer wg.Done()
			m.exitPosition(context.Background(), o, price, "FORCE_EXIT")
		}(order, exitPrice)
	}

	// Wait for all exits to complete before broadcasting done.
	wg.Wait()

	if m.wsServer != nil {
		m.wsServer.Broadcast(userID, PaperUpdate{
			Type:   "force_exit_done",
			UserID: userID,
		})
	}
	return nil
}

// ForceExitByStrategy exits all open paper positions for a given user+strategy at the best available price.
// Uses GetExitablePaperOrdersByStrategy which includes CANCELLED orders (e.g. after strategy
// deletion) so force-exit still works even if the strategy was deleted first.
func (m *PaperTradeMonitor) ForceExitByStrategy(ctx context.Context, userID, strategyID string) error {
	orders, err := m.repo.GetExitablePaperOrdersByStrategy(ctx, strategyID, userID)
	if err != nil {
		return fmt.Errorf("force exit strategy: failed to fetch positions: %w", err)
	}

	log.Printf("[paper-monitor] Force-exiting %d positions for user %s strategy %s",
		len(orders), userID, strategyID)

	var wg sync.WaitGroup
	for _, order := range orders {
		if _, already := m.exiting.LoadOrStore(order.OrderID, true); already {
			continue
		}

		// Cancel any active ML group for this order so its goroutine stops.
		if m.mlCanceller != nil {
			m.mlCanceller.CancelGroup(ctx, order.OrderID)
		}

		exitPrice := m.resolveExitPrice(ctx, order)
		wg.Add(1)
		go func(o *models.Order, price float64) {
			defer wg.Done()
			m.exitPosition(context.Background(), o, price, "FORCE_EXIT")
		}(order, exitPrice)
	}

	wg.Wait()

	if m.wsServer != nil {
		m.wsServer.Broadcast(userID, PaperUpdate{
			Type:   "force_exit_done",
			UserID: userID,
		})
	}
	return nil
}

// ResolveExitPrice is the public version of resolveExitPrice, usable by ws_server for live orders.
func (m *PaperTradeMonitor) ResolveExitPrice(ctx context.Context, order *models.Order) float64 {
	return m.resolveExitPrice(ctx, order)
}

// resolveExitPrice returns the best current price for a position exit.
//
// Priority:
//  1. Latest cached LTP (from WSS or Redis ticker)
//  2. Fresh Redis lookup
//  3. Stop-loss price (if set) — better than entry price for a forced-exit fallback
//  4. Original fill price (last resort — produces 0 PnL; logged as a warning)
func (m *PaperTradeMonitor) resolveExitPrice(ctx context.Context, order *models.Order) float64 {
	m.priceMu.RLock()
	price := m.latestLTP[order.Symbol]
	m.priceMu.RUnlock()

	if price > 0 {
		return price
	}

	if m.redisPrices != nil {
		rCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ltp, err := m.redisPrices.GetLTP(rCtx, string(order.Exchange), order.StockCode)
		cancel()
		if err == nil && ltp > 0 {
			return ltp
		}
	}

	// Use stop-loss as a proxy exit price when market data is unavailable — it is at
	// least directionally correct (below entry for BUY), unlike falling back to entry
	// price which would produce a misleading 0 PnL.
	if sl := safeF(order.StopLoss); sl > 0 {
		log.Printf("[paper-monitor] WARN: no live price for %s — using SL price %.2f as exit fallback", order.Symbol, sl)
		return sl
	}

	log.Printf("[paper-monitor] WARN: no live price or SL for %s — exiting at entry price %.2f (PnL will be 0)", order.Symbol, safeF(order.FilledPrice))
	return safeF(order.FilledPrice)
}

// exitPosition performs the actual DB update and WebSocket broadcast for an exit.
func (m *PaperTradeMonitor) exitPosition(ctx context.Context, order *models.Order, exitPrice float64, reason string) {
	defer m.exiting.Delete(order.OrderID)

	// Cancel any active ML group for this order BEFORE writing to DB so the ML
	// price-monitor goroutine stops immediately and cannot place further partial
	// exits on a position that is already being closed by the regular monitor.
	if m.mlCanceller != nil {
		m.mlCanceller.CancelGroup(ctx, order.OrderID)
	}

	if err := m.paperExec.ExitPaperPosition(ctx, order, exitPrice, reason); err != nil {
		log.Printf("[paper-monitor] Failed to exit position %s: %v — removing from cache to prevent retry loop", order.OrderID, err)
		// Remove from cache so the same order is not retried on every price tick.
		m.cacheMu.Lock()
		remaining := make([]*models.Order, 0, len(m.orderCache[order.Symbol]))
		for _, o := range m.orderCache[order.Symbol] {
			if o.OrderID != order.OrderID {
				remaining = append(remaining, o)
			}
		}
		if len(remaining) == 0 {
			delete(m.orderCache, order.Symbol)
			delete(m.symbolMeta, order.Symbol)
		} else {
			m.orderCache[order.Symbol] = remaining
		}
		m.cacheMu.Unlock()
		return
	}

	// Remove from order cache and clean up symbol tracking if no more orders
	m.cacheMu.Lock()
	remaining := make([]*models.Order, 0, len(m.orderCache[order.Symbol]))
	for _, o := range m.orderCache[order.Symbol] {
		if o.OrderID != order.OrderID {
			remaining = append(remaining, o)
		}
	}
	symbolDone := len(remaining) == 0
	if symbolDone {
		meta := m.symbolMeta[order.Symbol] // read before deleting
		delete(m.orderCache, order.Symbol)
		delete(m.symbolMeta, order.Symbol)
		unsubs := []string{order.Symbol}
		if meta.token > 0 {
			unsubs = append(unsubs, strconv.FormatInt(meta.token, 10))
		}
		m.marketClient.Unsubscribe(unsubs)
	} else {
		m.orderCache[order.Symbol] = remaining
	}
	m.cacheMu.Unlock()

	if symbolDone {
		m.priceMu.Lock()
		delete(m.latestLTP, order.Symbol)
		delete(m.wssLastSeen, order.Symbol)
		m.priceMu.Unlock()
	}

	// Clean up trailing SL state.
	m.trailMu.Lock()
	delete(m.trailStates, order.OrderID)
	m.trailMu.Unlock()

	finalPnL := computePnL(order, exitPrice)

	if m.wsServer != nil {
		m.wsServer.Broadcast(order.UserID, PaperUpdate{
			Type:       "position_exit",
			OrderID:    order.OrderID.String(),
			Symbol:     order.Symbol,
			LTP:        exitPrice,
			FinalPnL:   finalPnL,
			Reason:     reason,
			ExitPrice:  exitPrice,
			EntryPrice: safeF(order.FilledPrice),
		})
	}

	log.Printf("[paper-monitor] Position %s exited @ %.2f pnl=%.2f reason=%s",
		order.OrderID, exitPrice, finalPnL, reason)
}

func (m *PaperTradeMonitor) uniqueTokens() []string {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()
	tokens := make([]string, 0, len(m.symbolMeta))
	for _, meta := range m.symbolMeta {
		if meta.token > 0 {
			tokens = append(tokens, strconv.FormatInt(meta.token, 10))
		}
	}
	return tokens
}

// GetCurrentPnLSnapshot returns live PnL updates for all active positions of a given user
// using the latest cached LTP. Falls back to Redis if no WSS price is cached.
// Used to immediately populate PnL for newly connected WebSocket clients.
func (m *PaperTradeMonitor) GetCurrentPnLSnapshot(ctx context.Context, userID string) []PaperUpdate {
	// Snapshot the orders for this user without holding the lock during Redis calls
	type snap struct {
		order  *models.Order
		symbol string
		meta   symbolMeta
	}

	m.cacheMu.RLock()
	var snaps []snap
	for symbol, orders := range m.orderCache {
		for _, o := range orders {
			if o.UserID == userID {
				snaps = append(snaps, snap{o, symbol, m.symbolMeta[symbol]})
			}
		}
	}
	m.cacheMu.RUnlock()

	var updates []PaperUpdate
	for _, s := range snaps {
		m.priceMu.RLock()
		ltp := m.latestLTP[s.symbol]
		m.priceMu.RUnlock()

		// Try Redis if no WSS price is cached yet
		if ltp <= 0 && m.redisPrices != nil && s.meta.token > 0 {
			rCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			if price, err := m.redisPrices.GetLTP(rCtx, s.meta.exchange, s.meta.token); err == nil && price > 0 {
				ltp = price
			}
			cancel()
		}

		if ltp <= 0 {
			continue
		}
		if s.order.FilledQuantity == 0 || s.order.FilledPrice == nil {
			continue
		}

		updates = append(updates, PaperUpdate{
			Type:    "pnl_update",
			OrderID: s.order.OrderID.String(),
			UserID:  userID,
			Symbol:  s.symbol,
			LTP:     ltp,
			LivePnL: computePnL(s.order, ltp),
		})
	}
	return updates
}

func computePnL(order *models.Order, ltp float64) float64 {
	entry := safeF(order.FilledPrice)
	qty := float64(order.FilledQuantity)
	if order.OrderSide == models.OrderSideBuy {
		return (ltp - entry) * qty
	}
	return (entry - ltp) * qty
}

// SquareOffAll closes all open paper positions across all users.
// Called by the AutoSquareOffScheduler at market close (15:05 IST).
func (m *PaperTradeMonitor) SquareOffAll(ctx context.Context) error {
	// Collect distinct user IDs from the in-memory cache.
	m.cacheMu.RLock()
	userSet := make(map[string]struct{})
	for _, orders := range m.orderCache {
		for _, o := range orders {
			userSet[o.UserID] = struct{}{}
		}
	}
	m.cacheMu.RUnlock()

	if len(userSet) == 0 {
		log.Println("[paper-monitor] Auto square-off: no open paper positions")
		return nil
	}

	log.Printf("[paper-monitor] Auto square-off: closing positions for %d user(s)", len(userSet))
	var wg sync.WaitGroup
	for uid := range userSet {
		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			if err := m.ForceExitAll(ctx, userID); err != nil {
				log.Printf("[paper-monitor] Auto square-off failed for user %s: %v", userID, err)
			}
		}(uid)
	}
	wg.Wait()
	return nil
}

func safeF(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

// normaliseSymbol strips exchange suffixes that the enhanced-stream server appends
// when a subscription was made by numeric token rather than by symbol name.
// Examples: "COROMANDEL (NSE)" → "COROMANDEL", "RELIANCE-EQ" → "RELIANCE".
func normaliseSymbol(s string) string {
	// " (NSE)", " (BSE)", " (NFO)", " (MCX)" suffixes
	for _, exch := range []string{" (NSE)", " (BSE)", " (NFO)", " (MCX)", " (CDS)", " (BFO)"} {
		if len(s) > len(exch) && s[len(s)-len(exch):] == exch {
			return s[:len(s)-len(exch)]
		}
	}
	// "-EQ", "-BE", "-SM" segment suffixes (BSE equities)
	for _, sfx := range []string{"-EQ", "-BE", "-SM", "-BL"} {
		if len(s) > len(sfx) && s[len(s)-len(sfx):] == sfx {
			return s[:len(s)-len(sfx)]
		}
	}
	return s
}
