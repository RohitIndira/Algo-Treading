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
)

const (
	// wssGracePeriod: time without a WSS price before Redis fallback kicks in.
	wssGracePeriod = 5 * time.Second
	// redisPollInterval: how often the Redis PnL ticker runs.
	redisPollInterval = 2 * time.Second
)

// symbolMeta holds the exchange and instrument token needed to build a Redis market key.
type symbolMeta struct {
	exchange string // "NSE" or "BSE"
	token    int64  // StockCode / instrument token
}

// PaperTradeMonitor is the SL/TP engine and live PnL broadcaster.
//
// Price source priority:
//   - WSS (PaperMarketClient):  SL/TP checks + PnL broadcast — always preferred
//   - Redis (RedisPriceClient): PnL broadcast ONLY — fallback when WSS is silent
type PaperTradeMonitor struct {
	repo         repository.OrderRepository
	paperExec    *executor.PaperOrderExecutor
	wsServer     *PaperWSServer
	marketClient *PaperMarketClient
	redisPrices  *RedisPriceClient // nil-safe; Redis PnL fallback

	// symbol → open orders cache (refreshed from DB on change events)
	orderCache map[string][]*models.Order
	symbolMeta map[string]symbolMeta // exchange+token per symbol — same cacheMu
	cacheMu    sync.RWMutex

	// latest known LTP and WSS activity — protected by priceMu
	latestLTP   map[string]float64
	wssLastSeen map[string]time.Time
	priceMu     sync.RWMutex

	// track orders currently being exited to avoid duplicates (sync.Map[uuid.UUID → bool])
	exiting sync.Map
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
		orderCache:  make(map[string][]*models.Order),
		symbolMeta:  make(map[string]symbolMeta),
		latestLTP:   make(map[string]float64),
		wssLastSeen: make(map[string]time.Time),
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
		log.Printf("[paper-monitor] Redis PnL fallback started (poll=%s, wss-grace=%s)",
			redisPollInterval, wssGracePeriod)
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
	m.cacheMu.Unlock()

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

// OnPriceUpdate is called by PaperMarketClient on every new LTP from WSS.
// This is the primary price source and is the ONLY path that triggers SL/TP exits.
func (m *PaperTradeMonitor) OnPriceUpdate(symbol string, ltp float64) {
	m.cacheMu.RLock()
	orders := m.orderCache[symbol]
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

		sl := safeF(order.StopLoss)
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

		// Live PnL broadcast
		livePnL := computePnL(order, ltp)
		if m.wsServer != nil {
			m.wsServer.Broadcast(order.UserID, PaperUpdate{
				Type:    "pnl_update",
				OrderID: order.OrderID.String(),
				Symbol:  symbol,
				LTP:     ltp,
				LivePnL: livePnL,
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

// ResolveExitPrice is the public version of resolveExitPrice, usable by ws_server for live orders.
func (m *PaperTradeMonitor) ResolveExitPrice(ctx context.Context, order *models.Order) float64 {
	return m.resolveExitPrice(ctx, order)
}

// resolveExitPrice returns the best current price for a position exit.
//
// Priority:
//  1. Latest cached LTP (from WSS or Redis ticker)
//  2. Fresh Redis lookup
//  3. Original fill price (last resort)
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

	return safeF(order.FilledPrice)
}

// exitPosition performs the actual DB update and WebSocket broadcast for an exit.
func (m *PaperTradeMonitor) exitPosition(ctx context.Context, order *models.Order, exitPrice float64, reason string) {
	defer m.exiting.Delete(order.OrderID)

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

func safeF(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
