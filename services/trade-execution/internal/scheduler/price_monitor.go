package scheduler

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

// LTPProvider fetches the latest traded price from Redis.
// *paper.RedisPriceClient satisfies this interface.
type LTPProvider interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
	// GetLTPs fetches multiple LTPs in a single Redis MGET round-trip.
	// keys are in the format "exchange:token" (e.g. "nse:2475").
	// Returns a map of key → LTP. Missing/invalid keys are omitted from the result.
	GetLTPs(ctx context.Context, keys []string) (map[string]float64, error)
}

// OrderExecutorFunc is defined in auto_square_off.go (shared interface in this package).
// It is the callback the monitor invokes when a price condition is met.

// maxTriggerAttempts is the maximum number of times the price monitor will
// attempt to execute a triggered order before giving up and unwatching it.
const maxTriggerAttempts = 3

// watchEntry is a single order being monitored for a price threshold.
type watchEntry struct {
	order           *models.Order
	targetPrice     float64 // LTP must reach this level to trigger
	triggered       int32   // atomic: 1 = already triggered
	triggerAttempts int32   // atomic: number of times trigger+execute has been attempted
	stockKey        string  // cached "exchange:token" key for deduplication
}

// PriceMonitor watches Redis prices and triggers order placement when the
// live market price reaches the target_monitor_price for a pending order.
//
// Design goals:
//   - Ultra-low latency: polls Redis every checkInterval (default 500ms)
//   - Batch fetching: single MGET call for all unique stocks per tick
//   - Deduplication: same stock watched by N orders → 1 Redis lookup
//   - Parallel evaluation: sharded workers evaluate price conditions concurrently
//   - Single execution: atomic flag prevents duplicate triggers
//   - Restart-safe: reloads pending monitor orders from DB on Start()
type PriceMonitor struct {
	ltpProvider   LTPProvider
	orderRepo     repository.OrderRepository
	kafkaPub      *publisher.KafkaPublisher
	executeFn     OrderExecutorFunc
	checkInterval time.Duration
	numWorkers    int // number of parallel evaluation workers

	// onTickDone is called after each checkPrices tick completes.
	// Used by PaperWSServer to broadcast price watch snapshots via WebSocket.
	onTickDone func()

	mu      sync.RWMutex
	watches map[uuid.UUID]*watchEntry // keyed by OrderID

	stopChan chan struct{}
}

// NewPriceMonitor creates a new price monitor.
// checkInterval controls how often Redis is polled (default 500ms).
func NewPriceMonitor(
	ltpProvider LTPProvider,
	orderRepo repository.OrderRepository,
	kafkaPub *publisher.KafkaPublisher,
	executeFn OrderExecutorFunc,
	checkInterval time.Duration,
) *PriceMonitor {
	if checkInterval <= 0 {
		checkInterval = 500 * time.Millisecond
	}

	numWorkers := runtime.NumCPU()
	if numWorkers < 2 {
		numWorkers = 2
	}
	if numWorkers > 8 {
		numWorkers = 8
	}

	return &PriceMonitor{
		ltpProvider:   ltpProvider,
		orderRepo:     orderRepo,
		kafkaPub:      kafkaPub,
		executeFn:     executeFn,
		checkInterval: checkInterval,
		numWorkers:    numWorkers,
		watches:       make(map[uuid.UUID]*watchEntry),
		stopChan:      make(chan struct{}),
	}
}

// SetOnTickDone sets a callback invoked after each checkPrices tick.
// Used to broadcast price watch snapshots via WebSocket.
func (pm *PriceMonitor) SetOnTickDone(fn func()) {
	pm.onTickDone = fn
}

// Watch registers an order for price monitoring.
// targetPrice is the price level at which the order should be triggered.
// The order's current Price field should contain the target_monitor_price.
func (pm *PriceMonitor) Watch(order *models.Order, targetPrice float64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.watches[order.OrderID]; exists {
		log.Printf("[price-monitor] Order %s already being watched — skipping duplicate", order.OrderID)
		return
	}

	pm.watches[order.OrderID] = &watchEntry{
		order:       order,
		targetPrice: targetPrice,
		stockKey:    stockKey(string(order.Exchange), order.StockCode),
	}

	log.Printf("[price-monitor] ▶ Watching %s:%s (order=%s user=%s strategy=%s) target=%.2f",
		order.Exchange, order.Symbol, order.OrderID, order.UserID, order.StrategyID, targetPrice)
}

// Unwatch removes an order from monitoring.
func (pm *PriceMonitor) Unwatch(orderID uuid.UUID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.watches, orderID)
}

// UnwatchByStrategy removes all orders belonging to a given strategy from monitoring.
func (pm *PriceMonitor) UnwatchByStrategy(strategyID string) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	removed := 0
	for id, entry := range pm.watches {
		if entry.order.StrategyID == strategyID {
			delete(pm.watches, id)
			log.Printf("[price-monitor] ■ Unwatched order %s (strategy %s deactivated)", id, strategyID)
			removed++
		}
	}
	return removed
}

// WatchCount returns the number of orders currently being monitored.
func (pm *PriceMonitor) WatchCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.watches)
}

// Start begins the price monitoring loop. It first reloads any pending
// monitor orders from the DB (for restart recovery), then polls Redis
// at checkInterval.
func (pm *PriceMonitor) Start(ctx context.Context) error {
	log.Printf("[price-monitor] Starting Price Monitor (interval=%v, workers=%d)", pm.checkInterval, pm.numWorkers)

	// Reload pending monitor orders from DB for restart recovery
	if err := pm.reloadFromDB(ctx); err != nil {
		log.Printf("[price-monitor] Warning: failed to reload pending orders from DB: %v", err)
		// Non-fatal — continue; new signals will still be watched
	}

	ticker := time.NewTicker(pm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[price-monitor] Stopped (context cancelled)")
			return nil
		case <-pm.stopChan:
			log.Println("[price-monitor] Stopped (stop signal)")
			return nil
		case <-ticker.C:
			pm.checkPrices(ctx)
		}
	}
}

// Stop stops the price monitor.
func (pm *PriceMonitor) Stop() {
	close(pm.stopChan)
}

// stockKey builds a deduplicated cache key for a stock.
func stockKey(exchange string, token int64) string {
	return fmt.Sprintf("%s:%d", strings.ToLower(exchange), token)
}

// checkPrices fetches all unique stock LTPs in a single batch MGET,
// then fans out price evaluation to parallel workers.
func (pm *PriceMonitor) checkPrices(ctx context.Context) {
	// --- Step 1: Snapshot watches and group by stock key ---
	pm.mu.RLock()
	if len(pm.watches) == 0 {
		pm.mu.RUnlock()
		return
	}

	// Group entries by stock key for deduplication
	byStock := make(map[string][]*watchEntry, len(pm.watches)/2+1)
	for _, e := range pm.watches {
		if atomic.LoadInt32(&e.triggered) != 0 {
			continue
		}
		byStock[e.stockKey] = append(byStock[e.stockKey], e)
	}
	pm.mu.RUnlock()

	if len(byStock) == 0 {
		return
	}

	// --- Step 2: Batch fetch all unique stock LTPs in one MGET ---
	uniqueKeys := make([]string, 0, len(byStock))
	for k := range byStock {
		uniqueKeys = append(uniqueKeys, k)
	}

	batchCtx, batchCancel := context.WithTimeout(ctx, 3*time.Second)
	ltps, err := pm.ltpProvider.GetLTPs(batchCtx, uniqueKeys)
	batchCancel()

	if err != nil {
		log.Printf("[price-monitor] Batch LTP fetch failed: %v (unique_keys=%d)", err, len(uniqueKeys))
		return
	}

	// --- Step 3: Fan out evaluation to parallel workers ---
	type evalJob struct {
		entry *watchEntry
		ltp   float64
	}

	jobCh := make(chan evalJob, len(byStock)*2)

	// Enqueue jobs: for each stock with a valid LTP, check all watching entries
	for key, entries := range byStock {
		ltp, ok := ltps[key]
		if !ok || ltp <= 0 {
			continue
		}
		for _, entry := range entries {
			jobCh <- evalJob{entry: entry, ltp: ltp}
		}
	}
	close(jobCh)

	// Spawn workers to evaluate conditions in parallel
	var wg sync.WaitGroup
	workers := pm.numWorkers
	if len(jobCh) < workers {
		workers = len(jobCh)
	}
	if workers == 0 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				pm.evaluateEntry(ctx, job.entry, job.ltp)
			}
		}()
	}

	wg.Wait()

	// Notify listener (e.g. WS server) that a tick completed
	if pm.onTickDone != nil {
		pm.onTickDone()
	}
}

// evaluateEntry checks if a single watch entry's price condition is met and triggers execution.
func (pm *PriceMonitor) evaluateEntry(ctx context.Context, entry *watchEntry, ltp float64) {
	// Double-check atomic flag (another worker may have triggered it)
	if atomic.LoadInt32(&entry.triggered) != 0 {
		return
	}

	// Check if price condition is met
	isBuy := entry.order.OrderSide == models.OrderSideBuy
	conditionMet := false

	if isBuy {
		// BUY: trigger when LTP >= target (price rose to our level)
		conditionMet = ltp >= entry.targetPrice
	} else {
		// SELL: trigger when LTP <= target (price dropped to our level)
		conditionMet = ltp <= entry.targetPrice
	}

	if !conditionMet {
		return
	}

	// Atomically claim this trigger (prevents duplicate execution)
	if !atomic.CompareAndSwapInt32(&entry.triggered, 0, 1) {
		return
	}

	log.Printf("[price-monitor] ✓ TRIGGERED %s:%s (order=%s) LTP=%.2f target=%.2f — placing order",
		entry.order.Exchange, entry.order.Symbol, entry.order.OrderID, ltp, entry.targetPrice)

	// Execute in a goroutine to not block the evaluation workers
	go pm.triggerOrder(ctx, entry, ltp)
}

// isTerminalStatus returns true if the order status means no further execution should happen.
func isTerminalStatus(s models.OrderStatus) bool {
	switch s {
	case models.StatusFilled, models.StatusCancelled, models.StatusFailed, models.StatusRejected:
		return true
	}
	return false
}

// triggerOrder updates the order with the current LTP and calls the executor.
func (pm *PriceMonitor) triggerOrder(ctx context.Context, entry *watchEntry, ltp float64) {
	order := entry.order

	// Before executing, verify the order hasn't been cancelled/failed in the DB
	// (e.g. strategy was deactivated while this order was in the watch list).
	if pm.orderRepo != nil {
		dbOrder, err := pm.orderRepo.GetByID(ctx, order.OrderID)
		if err == nil && dbOrder != nil && isTerminalStatus(dbOrder.Status) {
			log.Printf("[price-monitor] ■ Order %s is %s in DB — removing from watch list (strategy likely deactivated)",
				order.OrderID, dbOrder.Status)
			pm.Unwatch(order.OrderID)
			return
		}
	}

	// Promote from STOP_LOSS → LIMIT so the executor places a Limit + BRACKET_ORDER.
	// The PriceMonitor already waited for the price to reach the target level.
	// Set limit price = LTP + 0.5% buffer to cross the spread and fill immediately.
	order.OrderType = models.OrderTypeLimit
	adjustedLimit := ltp * 1.005
	// Round to NSE tick size (0.05) using integer paise arithmetic
	paise := int64(adjustedLimit*100 + 0.5) // math.Round equivalent
	paise = ((paise + 2) / 5) * 5
	roundedLimit := float64(paise) / 100.0
	order.Price = &roundedLimit // limit price with 0.5% buffer for immediate fill

	log.Printf("[price-monitor] Limit price set: LTP=%.2f → limit=%.2f (+0.5%% buffer)",
		ltp, roundedLimit)

	// Record the trigger event
	if pm.orderRepo != nil {
		pm.orderRepo.RecordExecutionEvent(ctx, order.OrderID, "PRICE_MONITOR_TRIGGERED", map[string]interface{}{
			"target_price": entry.targetPrice,
			"trigger_ltp":  ltp,
			"timestamp":    time.Now(),
		})
	}

	// Publish monitoring triggered event to Kafka
	if pm.kafkaPub != nil {
		now := time.Now()
		update := &models.OrderUpdate{
			UpdateID:   uuid.New().String(),
			OrderID:    order.OrderID.String(),
			UserID:     order.UserID,
			UpdateType: "PRICE_MONITOR_TRIGGERED",
			Priority:   "MEDIUM",
			Title:      "Price Target Reached",
			Message:    fmt.Sprintf("%s reached %.2f — placing bracket order", order.Symbol, ltp),
			Status:     string(order.Status),
			CreatedAt:  now,
			ExpiresAt:  now.Add(1 * time.Hour),
			OrderSummary: models.OrderSummary{
				Stock:     order.Symbol,
				Action:    string(order.OrderSide),
				Quantity:  order.Quantity,
				Exchange:  string(order.Exchange),
				OrderType: string(order.OrderType),
				Price:     fmt.Sprintf("₹%.2f", ltp),
			},
			NotificationChannels: models.NotificationChannels{Push: true, InApp: true},
		}
		if err := pm.kafkaPub.PublishOrderUpdate(ctx, update); err != nil {
			log.Printf("[price-monitor] Failed to publish trigger event for order %s: %v", order.OrderID, err)
		}
	}

	// Call the executor to place the order at broker
	if err := pm.executeFn.ExecuteOrder(ctx, order); err != nil {
		attempts := atomic.AddInt32(&entry.triggerAttempts, 1)
		log.Printf("[price-monitor] ✗ Failed to execute triggered order %s (trigger attempt %d/%d): %v",
			order.OrderID, attempts, maxTriggerAttempts, err)

		if attempts >= maxTriggerAttempts {
			log.Printf("[price-monitor] ✗ Max trigger attempts exhausted for order %s — removing from watch list", order.OrderID)
			pm.Unwatch(order.OrderID)
			return
		}

		// Reset trigger flag so it can be retried on next tick
		atomic.StoreInt32(&entry.triggered, 0)
		return
	}

	// Remove from watch list on success
	pm.Unwatch(order.OrderID)
	log.Printf("[price-monitor] ✓ Order %s executed and removed from watch list (remaining: %d)",
		order.OrderID, pm.WatchCount())
}

// CancelWatch removes an order from monitoring and marks it as CANCELLED in the DB.
// Returns true if the order was found and cancelled, false if it wasn't being watched.
func (pm *PriceMonitor) CancelWatch(ctx context.Context, orderID uuid.UUID, userID string) bool {
	pm.mu.RLock()
	entry, exists := pm.watches[orderID]
	pm.mu.RUnlock()

	if !exists {
		return false
	}

	// Verify user owns this order
	if entry.order.UserID != userID {
		return false
	}

	pm.Unwatch(orderID)

	// Mark as CANCELLED in DB
	if pm.orderRepo != nil {
		if err := pm.orderRepo.UpdateStatus(ctx, orderID, models.StatusCancelled); err != nil {
			log.Printf("[price-monitor] ■ Failed to update cancelled status for order %s: %v", orderID, err)
		}
		pm.orderRepo.RecordExecutionEvent(ctx, orderID, "PRICE_WATCH_CANCELLED", map[string]interface{}{
			"cancelled_by": userID,
			"target_price": entry.targetPrice,
		})
	}

	// Publish cancellation event to Kafka
	if pm.kafkaPub != nil {
		now := time.Now()
		update := &models.OrderUpdate{
			UpdateID:   uuid.New().String(),
			OrderID:    orderID.String(),
			UserID:     userID,
			UpdateType: "PRICE_WATCH_CANCELLED",
			Priority:   "LOW",
			Title:      "Price Watch Cancelled",
			Message:    fmt.Sprintf("Cancelled price watch for %s (target ₹%.2f)", entry.order.Symbol, entry.targetPrice),
			Status:     string(models.StatusCancelled),
			CreatedAt:  now,
			ExpiresAt:  now.Add(1 * time.Hour),
			OrderSummary: models.OrderSummary{
				Stock:     entry.order.Symbol,
				Action:    string(entry.order.OrderSide),
				Quantity:  entry.order.Quantity,
				Exchange:  string(entry.order.Exchange),
				OrderType: string(entry.order.OrderType),
				Price:     fmt.Sprintf("₹%.2f", entry.targetPrice),
			},
			NotificationChannels: models.NotificationChannels{Push: false, InApp: true},
		}
		if err := pm.kafkaPub.PublishOrderUpdate(ctx, update); err != nil {
			log.Printf("[price-monitor] Failed to publish cancel event for order %s: %v", orderID, err)
		}
	}

	log.Printf("[price-monitor] ■ User %s cancelled watch for order %s (%s target=%.2f) (remaining: %d)",
		userID, orderID, entry.order.Symbol, entry.targetPrice, pm.WatchCount())
	return true
}

// CancelWatchBatch cancels multiple watches at once. Returns count of successfully cancelled watches.
func (pm *PriceMonitor) CancelWatchBatch(ctx context.Context, orderIDs []uuid.UUID, userID string) int {
	cancelled := 0
	for _, id := range orderIDs {
		if pm.CancelWatch(ctx, id, userID) {
			cancelled++
		}
	}
	return cancelled
}

// WatchSnapshot is a JSON-serializable snapshot of a single watched order.
type WatchSnapshot struct {
	OrderID     string  `json:"order_id"`
	UserID      string  `json:"user_id"`
	StrategyID  string  `json:"strategy_id"`
	Symbol      string  `json:"symbol"`
	Exchange    string  `json:"exchange"`
	StockCode   int64   `json:"stock_code"`
	OrderSide   string  `json:"order_side"`
	OrderType   string  `json:"order_type"`
	ProductType string  `json:"product_type"`
	TargetPrice float64 `json:"target_price"`
	StopLoss    float64 `json:"stop_loss,omitempty"`
	TakeProfit  float64 `json:"take_profit,omitempty"`
	Quantity    int32   `json:"quantity"`
	Triggered   bool    `json:"triggered"`
	Attempts    int32   `json:"attempts"`
	CreatedAt   string  `json:"created_at"`
}

// GetWatchSnapshot returns a snapshot of all currently watched orders for a given user.
// If userID is empty, returns watches for all users.
func (pm *PriceMonitor) GetWatchSnapshot(userID string) []WatchSnapshot {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make([]WatchSnapshot, 0, len(pm.watches))
	for _, e := range pm.watches {
		if userID != "" && e.order.UserID != userID {
			continue
		}
		snap := WatchSnapshot{
			OrderID:     e.order.OrderID.String(),
			UserID:      e.order.UserID,
			StrategyID:  e.order.StrategyID,
			Symbol:      e.order.Symbol,
			Exchange:    string(e.order.Exchange),
			StockCode:   e.order.StockCode,
			OrderSide:   string(e.order.OrderSide),
			OrderType:   string(e.order.OrderType),
			ProductType: e.order.ProductType,
			TargetPrice: e.targetPrice,
			Quantity:    e.order.Quantity,
			Triggered:   atomic.LoadInt32(&e.triggered) != 0,
			Attempts:    atomic.LoadInt32(&e.triggerAttempts),
			CreatedAt:   e.order.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
		if e.order.StopLoss != nil {
			snap.StopLoss = *e.order.StopLoss
		}
		if e.order.TakeProfit != nil {
			snap.TakeProfit = *e.order.TakeProfit
		}
		result = append(result, snap)
	}
	return result
}

// reloadFromDB loads pending STOP_LOSS+BRACKET orders from the DB and re-registers
// them for monitoring. This handles service restart recovery.
func (pm *PriceMonitor) reloadFromDB(ctx context.Context) error {
	if pm.orderRepo == nil {
		return nil
	}

	orders, err := pm.orderRepo.GetPendingMonitorOrders(ctx)
	if err != nil {
		return fmt.Errorf("failed to query pending monitor orders: %w", err)
	}

	for _, order := range orders {
		if order.Price == nil {
			log.Printf("[price-monitor] Skipping order %s — no target price set", order.OrderID)
			continue
		}
		pm.Watch(order, *order.Price)
	}

	if len(orders) > 0 {
		log.Printf("[price-monitor] Reloaded %d pending monitor orders from DB", len(orders))
	}

	return nil
}
