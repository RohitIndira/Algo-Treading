package paper

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

const (
	// liveReconcileInterval: how often the monitor re-reads open live positions
	// from the DB to detect new entries and closed exits.
	liveReconcileInterval = 3 * time.Second
	// liveRedisPollInterval: how often the Redis P&L fallback ticker runs for
	// symbols the market WSS has been silent on.
	liveRedisPollInterval = 2 * time.Second
)

// LiveOrderMonitor mirrors PaperTradeMonitor for LIVE positions, but is
// display-only: it never triggers exits (the broker's OCO/multi-level SL/TP
// legs do that). Its job is to bring /ws/live-orders to parity with
// /ws/paper-trades by emitting the same per-position lifecycle events:
//
//   - new_order     : broadcast when a newly filled live position is discovered
//   - pnl_update    : broadcast on every market tick — running P&L for a position
//   - position_exit : broadcast when a tracked position is closed in the DB
//                      (live_exit_price set by force-exit / OCO / ML / square-off)
//
// P&L is anchored to the order's FilledPrice — the broker's actual VWAP fill
// price recorded by statusservice — so the entry price shown always matches
// what was executed at the broker. Exit price / time / reason on position_exit
// come straight from the DB columns written by whichever path closed the
// position, so they are exact, never estimated here.
type LiveOrderMonitor struct {
	repo         repository.OrderRepository
	wsServer     *PaperWSServer
	marketClient *PaperMarketClient
	redisPrices  *RedisPriceClient // nil-safe; Redis P&L fallback

	// symbol → open live orders — protected by cacheMu
	orderCache map[string][]*models.Order
	symbolMeta map[string]symbolMeta // exchange+token per symbol
	tokenToSym map[string]string     // numeric token string → canonical DB symbol
	tracked    map[uuid.UUID]struct{} // every order_id currently being monitored
	cacheMu    sync.RWMutex

	// latest known LTP and WSS activity — protected by priceMu
	latestLTP   map[string]float64
	wssLastSeen map[string]time.Time
	priceMu     sync.RWMutex
}

// NewLiveOrderMonitor creates a live-position P&L monitor.
// redisPrices may be nil; pass nil to disable the Redis P&L fallback.
func NewLiveOrderMonitor(
	repo repository.OrderRepository,
	wsServer *PaperWSServer,
	marketClient *PaperMarketClient,
	redisPrices *RedisPriceClient,
) *LiveOrderMonitor {
	return &LiveOrderMonitor{
		repo:         repo,
		wsServer:     wsServer,
		marketClient: marketClient,
		redisPrices:  redisPrices,
		orderCache:   make(map[string][]*models.Order),
		symbolMeta:   make(map[string]symbolMeta),
		tokenToSym:   make(map[string]string),
		tracked:      make(map[uuid.UUID]struct{}),
		latestLTP:    make(map[string]float64),
		wssLastSeen:  make(map[string]time.Time),
	}
}

// Initialize loads all open live positions, subscribes their symbols on the
// market feed, and starts the reconcile + Redis-fallback loops.
func (m *LiveOrderMonitor) Initialize(ctx context.Context) error {
	orders, err := m.repo.GetAllActiveLivePositions(ctx)
	if err != nil {
		return err
	}
	log.Printf("[live-monitor] Loaded %d open live position(s) on startup", len(orders))
	for _, o := range orders {
		m.register(o)
	}
	m.subscribeAll()

	go m.runReconcileLoop(ctx)
	if m.redisPrices != nil {
		go m.runRedisPnLTicker(ctx)
		log.Printf("[live-monitor] Redis P&L fallback started (poll=%s)", liveRedisPollInterval)
	}
	return nil
}

// register adds an order to the in-memory cache. Caller need not hold cacheMu.
func (m *LiveOrderMonitor) register(o *models.Order) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	if _, ok := m.tracked[o.OrderID]; ok {
		return
	}
	m.tracked[o.OrderID] = struct{}{}
	m.orderCache[o.Symbol] = append(m.orderCache[o.Symbol], o)
	if _, ok := m.symbolMeta[o.Symbol]; !ok {
		m.symbolMeta[o.Symbol] = symbolMeta{exchange: string(o.Exchange), token: o.StockCode}
	}
	if o.StockCode > 0 {
		m.tokenToSym[strconv.FormatInt(o.StockCode, 10)] = o.Symbol
	}
}

// subscribeAll subscribes every cached symbol (by name and numeric token) on
// the market feed.
func (m *LiveOrderMonitor) subscribeAll() {
	m.cacheMu.RLock()
	var subs []string
	seen := map[string]bool{}
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
}

// OnPriceUpdate is called by the market client on every LTP tick. It computes
// and broadcasts running P&L for every open live position on that symbol.
func (m *LiveOrderMonitor) OnPriceUpdate(symbol string, ltp float64) {
	if ltp <= 0 {
		return
	}
	m.cacheMu.RLock()
	orders := m.orderCache[symbol]
	if len(orders) == 0 {
		// The enhanced-stream server may echo the numeric token or append an
		// exchange suffix — try both normalisation strategies.
		if canonical, ok := m.tokenToSym[symbol]; ok {
			symbol = canonical
			orders = m.orderCache[symbol]
		} else if norm := normaliseSymbol(symbol); norm != symbol {
			symbol = norm
			orders = m.orderCache[symbol]
		}
	}
	// Copy slice header so we can release the lock before broadcasting.
	cp := make([]*models.Order, len(orders))
	copy(cp, orders)
	m.cacheMu.RUnlock()

	if len(cp) == 0 {
		return
	}

	m.priceMu.Lock()
	m.wssLastSeen[symbol] = time.Now()
	m.latestLTP[symbol] = ltp
	m.priceMu.Unlock()

	for _, order := range cp {
		if order.FilledQuantity == 0 || order.FilledPrice == nil {
			continue
		}
		m.broadcastPnL(order, symbol, ltp)
	}
}

// broadcastPnL sends a single pnl_update for one open live position.
func (m *LiveOrderMonitor) broadcastPnL(order *models.Order, symbol string, ltp float64) {
	if m.wsServer == nil {
		return
	}
	m.wsServer.BroadcastLiveOrder(order.UserID, LiveOrderUpdate{
		Type:    "pnl_update",
		UserID:  order.UserID,
		OrderID: order.OrderID.String(),
		Symbol:  symbol,
		LTP:     ltp,
		LivePnL: computePnL(order, ltp),
	})
}

// runReconcileLoop periodically diffs the open-live-positions DB view against
// the in-memory cache: new positions → new_order, vanished positions → exit.
func (m *LiveOrderMonitor) runReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(liveReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

// reconcile re-reads open live positions and emits new_order / position_exit.
func (m *LiveOrderMonitor) reconcile(ctx context.Context) {
	orders, err := m.repo.GetAllActiveLivePositions(ctx)
	if err != nil {
		log.Printf("[live-monitor] reconcile: failed to fetch positions: %v", err)
		return
	}

	live := make(map[uuid.UUID]*models.Order, len(orders))
	for _, o := range orders {
		live[o.OrderID] = o
	}

	// Snapshot currently-tracked IDs.
	m.cacheMu.RLock()
	tracked := make([]uuid.UUID, 0, len(m.tracked))
	for id := range m.tracked {
		tracked = append(tracked, id)
	}
	m.cacheMu.RUnlock()

	// Vanished IDs → position closed.
	for _, id := range tracked {
		if _, stillOpen := live[id]; !stillOpen {
			m.handleExit(ctx, id)
		}
	}

	// New IDs → freshly opened position.
	var added bool
	for id, o := range live {
		m.cacheMu.RLock()
		_, known := m.tracked[id]
		m.cacheMu.RUnlock()
		if known {
			continue
		}
		m.register(o)
		added = true
		if m.wsServer != nil {
			m.wsServer.BroadcastLiveOrder(o.UserID, LiveOrderUpdate{
				Type:   "new_order",
				UserID: o.UserID,
				Order:  o,
			})
		}
		log.Printf("[live-monitor] Tracking new live position %s (%s qty=%d entry=%.2f)",
			o.OrderID, o.Symbol, o.FilledQuantity, safeF(o.FilledPrice))
	}
	if added {
		m.subscribeAll()
	}
}

// handleExit removes a closed position from the cache and broadcasts
// position_exit using the exact exit columns recorded in the DB.
func (m *LiveOrderMonitor) handleExit(ctx context.Context, orderID uuid.UUID) {
	// Read the closed row so the broadcast carries the exact exit price /
	// P&L / time / reason written by whichever path closed the position.
	closed, err := m.repo.GetByID(ctx, orderID)
	if err != nil || closed == nil {
		log.Printf("[live-monitor] handleExit: could not load closed order %s: %v", orderID, err)
		m.removeFromCache(orderID)
		return
	}

	m.removeFromCache(orderID)

	// Only emit position_exit when the row was genuinely exited (exit price
	// recorded). A bare CANCELLED with no exit price is not a position close.
	if closed.LiveExitPrice == nil {
		return
	}
	if m.wsServer == nil {
		return
	}

	reason := ""
	if closed.LiveExitReason != nil {
		reason = *closed.LiveExitReason
	}
	now := time.Now()
	exitTime := closed.LiveExitTime
	if exitTime == nil {
		exitTime = &now
	}
	m.wsServer.BroadcastLiveOrder(closed.UserID, LiveOrderUpdate{
		Type:       "position_exit",
		UserID:     closed.UserID,
		OrderID:    closed.OrderID.String(),
		Symbol:     closed.Symbol,
		LTP:        safeF(closed.LiveExitPrice),
		FinalPnL:   safeF(closed.LivePnL),
		Reason:     reason,
		ExitPrice:  safeF(closed.LiveExitPrice),
		EntryPrice: safeF(closed.FilledPrice),
		EntryTime:  closed.ExecutedAt,
		ExitTime:   exitTime,
	})
	log.Printf("[live-monitor] Position %s exited @ %.2f pnl=%.2f reason=%s",
		closed.OrderID, safeF(closed.LiveExitPrice), safeF(closed.LivePnL), reason)
}

// removeFromCache drops an order from all in-memory maps and unsubscribes its
// symbol from the market feed when no other live position needs it.
func (m *LiveOrderMonitor) removeFromCache(orderID uuid.UUID) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	delete(m.tracked, orderID)

	var symbol string
	var meta symbolMeta
	for sym, orders := range m.orderCache {
		remaining := make([]*models.Order, 0, len(orders))
		found := false
		for _, o := range orders {
			if o.OrderID == orderID {
				found = true
			} else {
				remaining = append(remaining, o)
			}
		}
		if !found {
			continue
		}
		symbol = sym
		meta = m.symbolMeta[sym]
		if len(remaining) == 0 {
			delete(m.orderCache, sym)
			delete(m.symbolMeta, sym)
		} else {
			m.orderCache[sym] = remaining
		}
		break
	}

	if symbol == "" {
		return
	}
	if _, stillTracked := m.orderCache[symbol]; stillTracked {
		return // another position still needs this symbol's feed
	}

	unsubs := []string{symbol}
	if meta.token > 0 {
		tok := strconv.FormatInt(meta.token, 10)
		unsubs = append(unsubs, tok)
		delete(m.tokenToSym, tok)
	}
	m.marketClient.Unsubscribe(unsubs)
	m.priceMu.Lock()
	delete(m.latestLTP, symbol)
	delete(m.wssLastSeen, symbol)
	m.priceMu.Unlock()
}

// runRedisPnLTicker polls Redis for symbols the market WSS has been silent on
// (beyond wssGracePeriod) and broadcasts P&L so positions on quiet instruments
// still get live updates. It never triggers exits.
func (m *LiveOrderMonitor) runRedisPnLTicker(ctx context.Context) {
	ticker := time.NewTicker(liveRedisPollInterval)
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

func (m *LiveOrderMonitor) tickRedisPnL(ctx context.Context) {
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
		m.priceMu.RLock()
		lastSeen := m.wssLastSeen[s.symbol]
		m.priceMu.RUnlock()
		if time.Since(lastSeen) < wssGracePeriod {
			continue // WSS is live for this symbol — it owns the broadcast
		}
		if s.meta.token == 0 {
			continue
		}
		rCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ltp, err := m.redisPrices.GetLTP(rCtx, s.meta.exchange, s.meta.token)
		cancel()
		if err != nil || ltp <= 0 {
			continue
		}
		m.priceMu.Lock()
		m.latestLTP[s.symbol] = ltp
		m.priceMu.Unlock()
		for _, order := range s.orders {
			if order.FilledQuantity == 0 || order.FilledPrice == nil {
				continue
			}
			m.broadcastPnL(order, s.symbol, ltp)
		}
	}
}

// GetCurrentPnLSnapshot returns one pnl_update per open live position for a
// user, using the latest cached LTP (Redis fallback when no WSS price yet).
// Used to immediately populate P&L for a newly connected /ws/live-orders client.
func (m *LiveOrderMonitor) GetCurrentPnLSnapshot(ctx context.Context, userID string) []LiveOrderUpdate {
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

	var updates []LiveOrderUpdate
	for _, s := range snaps {
		m.priceMu.RLock()
		ltp := m.latestLTP[s.symbol]
		m.priceMu.RUnlock()
		if ltp <= 0 && m.redisPrices != nil && s.meta.token > 0 {
			rCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			if price, err := m.redisPrices.GetLTP(rCtx, s.meta.exchange, s.meta.token); err == nil && price > 0 {
				ltp = price
			}
			cancel()
		}
		if ltp <= 0 || s.order.FilledQuantity == 0 || s.order.FilledPrice == nil {
			continue
		}
		updates = append(updates, LiveOrderUpdate{
			Type:    "pnl_update",
			UserID:  userID,
			OrderID: s.order.OrderID.String(),
			Symbol:  s.symbol,
			LTP:     ltp,
			LivePnL: computePnL(s.order, ltp),
		})
	}
	return updates
}
