package scheduler

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/pkg/tradecap"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

// schedRecover recovers a panic in a detached scheduler goroutine and logs it
// with a stack trace, matching the package's existing stdlib-log style. Shared
// by the price monitor and auto-square-off scheduler. Logs and returns only —
// it never changes order/square-off logic.
func schedRecover(where string) {
	if r := recover(); r != nil {
		log.Printf("[scheduler] PANIC RECOVERED in %s: %v\n%s", where, r, debug.Stack())
	}
}

// OCOAdopter adopts a freshly-filled entry order into an OCO group, placing
// SL and TP legs based on the percentages derived from the persisted order.
// Satisfied by *oco.OCOManager.AdoptOrder. Declared here as an interface to
// avoid a scheduler→oco import cycle.
type OCOAdopter interface {
	AdoptOrder(order *models.Order, slPercent, tpPercent float64, trailingSL bool, trailingSLPct float64, auth *indiraClient.AuthContext)
}

// LTPProvider fetches the latest traded price from Redis (fallback).
// *paper.RedisPriceClient satisfies this interface.
type LTPProvider interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
	// GetLTPs fetches multiple LTPs in a single Redis MGET round-trip.
	// keys are in the format "exchange:token" (e.g. "nse:2475").
	// Returns a map of key → LTP. Missing/invalid keys are omitted from the result.
	GetLTPs(ctx context.Context, keys []string) (map[string]float64, error)
	// GetTickSize returns the instrument tick size stored in Redis market data.
	GetTickSize(ctx context.Context, exchange string, token int64) (float64, error)
}

// MarketWSClient provides real-time LTP from WebSocket (primary price source).
// Satisfied by *marketws.Client.
type MarketWSClient interface {
	// GetLTPs returns cached LTPs for the given keys ("exchange:token").
	GetLTPs(keys []string) map[string]float64
	// IsHealthy returns true if the WSS connection is active.
	IsHealthy() bool
	// Subscribe registers tokens for live price updates (ref-counted).
	Subscribe(tokens []string)
	// Unsubscribe decrements ref count; actually unsubscribes when count reaches 0.
	Unsubscribe(tokens []string)
}

// OrderExecutorFunc is defined in auto_square_off.go (shared interface in this package).
// It is the callback the monitor invokes when a price condition is met.

// maxTriggerAttempts is the maximum number of times the price monitor will
// attempt to execute a triggered order before giving up and unwatching it.
const maxTriggerAttempts = 3

// shardCount is the number of shards for the stock index.
// Distributes lock contention across shards for concurrent Watch/Unwatch/evaluate.
const shardCount = 32

// watchEntry is a single order being monitored for a price threshold.
type watchEntry struct {
	order           *models.Order
	targetPrice     float64 // LTP must reach this level to trigger
	maxPrice        float64 // LTP must NOT exceed this level (0 = no upper bound)
	triggered       int32   // atomic: 1 = already triggered
	triggerAttempts int32   // atomic: number of times trigger+execute has been attempted
	stockKey        string  // cached "exchange:token" key for deduplication
	// triggeredAt is stamped the instant the price condition is first met (the CAS
	// in evaluateEntry). Used to order/audit the strategy trade-cap reservation so
	// that when several stocks cross at once the earliest crosser is favoured (FCFS).
	triggeredAt time.Time
	// onAfterFill is called after a successful ExecuteOrder. Used by the signal
	// processor to wire multi-level exit setup for below_min + MULTI_LEVEL orders,
	// where routeToMultiLevel cannot run at signal-arrival time (order must wait
	// for price target first). Nil for all non-multi-level orders.
	onAfterFill func(order *models.Order)
}

// stockShard holds the watches for a subset of stocks, behind its own lock.
type stockShard struct {
	mu sync.RWMutex
	// byOrder: orderID → watchEntry (for fast lookup/delete)
	byOrder map[uuid.UUID]*watchEntry
	// byStock: stockKey → []*watchEntry (for fanout on price update)
	byStock map[string][]*watchEntry
}

// PriceMonitor watches market prices and triggers order placement when the
// live market price reaches the target_monitor_price for a pending order.
//
// Architecture (optimized for 1000 users × 700 stocks):
//   - Sharded index: watches partitioned into 32 shards by stock key
//   - Event-driven: WSS pushes trigger immediate per-stock evaluation via OnPriceUpdate
//   - Persistent worker pool: evalCh feeds N workers (no per-tick allocation)
//   - Fallback polling: Redis polled only for stocks not covered by WSS
//   - Deduplication: same stock watched by N orders → 1 price lookup
//   - Single execution: atomic flag prevents duplicate triggers
//   - Dynamic subscriptions: auto subscribe/unsubscribe on Watch/Unwatch
type PriceMonitor struct {
	ltpProvider   LTPProvider    // Redis fallback
	wsClient      MarketWSClient // WebSocket primary (nil-safe)
	orderRepo     repository.OrderRepository
	kafkaPub      *publisher.KafkaPublisher
	executeFn     OrderExecutorFunc
	checkInterval time.Duration
	numWorkers    int

	// onTickDone is called after each checkPrices tick completes.
	// Used by PaperWSServer to broadcast price watch snapshots via WebSocket.
	onTickDone func()

	// ocoAdopter adopts triggered live entry orders into an OCO group on restart
	// recovery. Set via SetOCOAdopter. When nil, reloaded orders that have SL/TP
	// configuration on them will execute without OCO protection — log a warning.
	ocoAdopter OCOAdopter

	// Sharded index for O(1) lookup by stock and by order.
	shards [shardCount]*stockShard

	// evalCh is the persistent evaluation channel. Workers drain this continuously.
	evalCh chan evalJob

	// capReserver enforces MaxTradesPerStrategy when a below_min watch triggers —
	// the moment a monitored watch becomes a real trade. Shares the same Redis
	// counter the rules-engine uses for immediate trades. nil-safe: when unset or
	// Redis is unavailable the cap is not enforced here (fail-open).
	capReserver *tradecap.Reserver
	// strategyReserveLocks serializes trade-cap reservations per strategy so
	// concurrent same-strategy triggers don't interleave. Keyed by strategy_id.
	strategyReserveLocks sync.Map

	stopChan chan struct{}
}

// evalJob is a unit of work for the evaluation worker pool.
type evalJob struct {
	entry *watchEntry
	ltp   float64
}

// NewPriceMonitor creates a new price monitor.
// checkInterval controls how often Redis is polled for fallback (default 100ms).
func NewPriceMonitor(
	ltpProvider LTPProvider,
	orderRepo repository.OrderRepository,
	kafkaPub *publisher.KafkaPublisher,
	executeFn OrderExecutorFunc,
	checkInterval time.Duration,
) *PriceMonitor {
	if checkInterval <= 0 {
		checkInterval = 100 * time.Millisecond
	}

	numWorkers := runtime.NumCPU() * 4
	if numWorkers < 16 {
		numWorkers = 16
	}
	if numWorkers > 64 {
		numWorkers = 64
	}

	pm := &PriceMonitor{
		ltpProvider:   ltpProvider,
		orderRepo:     orderRepo,
		kafkaPub:      kafkaPub,
		executeFn:     executeFn,
		checkInterval: checkInterval,
		numWorkers:    numWorkers,
		evalCh:        make(chan evalJob, 4096), // buffered for burst handling
		stopChan:      make(chan struct{}),
	}

	for i := range pm.shards {
		pm.shards[i] = &stockShard{
			byOrder: make(map[uuid.UUID]*watchEntry),
			byStock: make(map[string][]*watchEntry),
		}
	}

	return pm
}

// shardFor returns the shard index for a stock key using FNV-1a hash.
func shardFor(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % shardCount)
}

// SetExecuteFn replaces the executor used to place orders when a price condition is met.
// Use this to swap in a RoutingExecutor that handles both live and paper orders.
func (pm *PriceMonitor) SetExecuteFn(fn OrderExecutorFunc) {
	pm.executeFn = fn
}

// SetOnTickDone sets a callback invoked after each checkPrices tick.
// Used to broadcast price watch snapshots via WebSocket.
func (pm *PriceMonitor) SetOnTickDone(fn func()) {
	pm.onTickDone = fn
}

// SetWSClient sets the WebSocket market data client as primary price source.
// When set, WSS prices are used first; Redis is only queried for keys missing from WSS.
func (pm *PriceMonitor) SetWSClient(ws MarketWSClient) {
	pm.wsClient = ws
}

// SetOCOAdopter wires the OCO manager so the price monitor can reconstruct
// SL/TP protection for orders reloaded from DB after a service restart. The
// original signal_processor closures (which carried SL%, TP%, trailing flag)
// are lost on restart — without this, reloaded orders that hit their price
// target would execute with NO stop-loss or take-profit protection.
func (pm *PriceMonitor) SetOCOAdopter(a OCOAdopter) {
	pm.ocoAdopter = a
}

// SetCapReserver wires the shared per-strategy daily trade counter so the
// monitor enforces MaxTradesPerStrategy when a watch triggers. Pass nil (or an
// unavailable reserver) to leave cap enforcement disabled at trigger time.
func (pm *PriceMonitor) SetCapReserver(r *tradecap.Reserver) {
	pm.capReserver = r
}

// strategyReserveLock returns the per-strategy reservation mutex.
func (pm *PriceMonitor) strategyReserveLock(strategyID string) *sync.Mutex {
	v, _ := pm.strategyReserveLocks.LoadOrStore(strategyID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// reserveStrategySlot atomically claims one daily trade slot for the order's
// strategy at trigger time. Returns reserved=true when a slot was taken (the
// caller must Release it if the placement then fails), capReached=true when the
// strategy is already at its limit (the caller must cancel the watch), or both
// false when enforcement is disabled/unlimited (allow the trade, nothing to
// release). A non-nil error is a transient Redis failure the caller should retry.
func (pm *PriceMonitor) reserveStrategySlot(ctx context.Context, order *models.Order) (reserved, capReached bool, err error) {
	if pm.capReserver == nil || !pm.capReserver.IsEnabled() || order.MaxTradesPerStrategy <= 0 {
		return false, false, nil
	}
	mu := pm.strategyReserveLock(order.StrategyID)
	mu.Lock()
	defer mu.Unlock()

	n, err := pm.capReserver.Reserve(ctx, order.StrategyID, order.MaxTradesPerStrategy, 0, time.Now())
	if err != nil {
		return false, false, err
	}
	if n == tradecap.CapReached {
		return false, true, nil
	}
	return true, false, nil
}

// releaseStrategySlot hands a reserved slot back when a triggered order will not
// become a trade (placement failed). No-op when nothing was reserved.
func (pm *PriceMonitor) releaseStrategySlot(ctx context.Context, order *models.Order, reserved bool) {
	if !reserved || pm.capReserver == nil {
		return
	}
	if err := pm.capReserver.Release(ctx, order.StrategyID, time.Now()); err != nil {
		log.Printf("[price-monitor] failed to release trade slot for strategy %s (order %s): %v",
			order.StrategyID, order.OrderID, err)
	}
}

// cancelForCapReached cancels a watch whose target triggered after the strategy
// had already used its daily trade budget. Mirrors the max-exceeded cancel path:
// unwatch, mark CANCELLED, record an audit event, and notify the user.
func (pm *PriceMonitor) cancelForCapReached(ctx context.Context, entry *watchEntry, ltp float64) {
	order := entry.order
	log.Printf("[price-monitor] ■ STRATEGY_TRADE_CAP_REACHED order=%s strategy=%s symbol=%s cap=%d triggeredAt=%s — cancelling watch (not placed)",
		order.OrderID, order.StrategyID, order.Symbol, order.MaxTradesPerStrategy, entry.triggeredAt.Format(time.RFC3339Nano))

	pm.Unwatch(order.OrderID)

	if pm.orderRepo != nil {
		if err := pm.orderRepo.UpdateStatus(ctx, order.OrderID, models.StatusCancelled); err != nil {
			log.Printf("[price-monitor] failed to mark order %s cancelled after cap reached: %v", order.OrderID, err)
		}
		pm.orderRepo.RecordExecutionEvent(ctx, order.OrderID, "STRATEGY_TRADE_CAP_REACHED", map[string]interface{}{
			"strategy_id":             order.StrategyID,
			"max_trades_per_strategy": order.MaxTradesPerStrategy,
			"trigger_ltp":             ltp,
			"target_price":            entry.targetPrice,
			"triggered_at":            entry.triggeredAt,
		})
	}

	if pm.kafkaPub != nil {
		now := time.Now()
		update := &models.OrderUpdate{
			UpdateID:   uuid.New().String(),
			OrderID:    order.OrderID.String(),
			UserID:     order.UserID,
			UpdateType: "STRATEGY_TRADE_CAP_REACHED",
			Priority:   "LOW",
			Title:      "Daily Trade Limit Reached",
			Message:    fmt.Sprintf("%s reached its target but the strategy's daily trade limit (%d) was already used — order not placed", order.Symbol, order.MaxTradesPerStrategy),
			Status:     string(models.StatusCancelled),
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
			NotificationChannels: models.NotificationChannels{Push: false, InApp: true},
		}
		if err := pm.kafkaPub.PublishOrderUpdate(ctx, update); err != nil {
			log.Printf("[price-monitor] failed to publish cap-reached event for order %s: %v", order.OrderID, err)
		}
	}
}

// Watch registers an order for price monitoring.
// targetPrice is the price level at which the order should be triggered.
// Automatically subscribes the token on the WSS client for real-time prices.
// Watch registers an order for price monitoring. onAfterFill is an optional
// callback invoked after a successful ExecuteOrder (used for multi-level setup
// on below_min orders). Pass nil when not needed.
func (pm *PriceMonitor) Watch(order *models.Order, targetPrice float64, onAfterFill func(*models.Order)) {
	key := stockKey(string(order.Exchange), order.StockCode)
	shard := pm.shards[shardFor(key)]

	shard.mu.Lock()
	if _, exists := shard.byOrder[order.OrderID]; exists {
		shard.mu.Unlock()
		log.Printf("[price-monitor] Order %s already being watched — skipping duplicate", order.OrderID)
		return
	}

	var maxPrice float64
	if order.MaxMonitorPrice != nil {
		maxPrice = *order.MaxMonitorPrice
	}
	entry := &watchEntry{
		order:       order,
		targetPrice: targetPrice,
		maxPrice:    maxPrice,
		stockKey:    key,
		onAfterFill: onAfterFill,
	}
	shard.byOrder[order.OrderID] = entry
	shard.byStock[key] = append(shard.byStock[key], entry)
	shard.mu.Unlock()

	// Subscribe to WSS for this token's live prices
	if pm.wsClient != nil {
		token := fmt.Sprintf("%d", order.StockCode)
		pm.wsClient.Subscribe([]string{token})
	}

	if maxPrice > 0 {
		log.Printf("[price-monitor] ▶ Watching %s:%s (order=%s user=%s strategy=%s) target=%.2f max=%.2f",
			order.Exchange, order.Symbol, order.OrderID, order.UserID, order.StrategyID, targetPrice, maxPrice)
	} else {
		log.Printf("[price-monitor] ▶ Watching %s:%s (order=%s user=%s strategy=%s) target=%.2f (no max)",
			order.Exchange, order.Symbol, order.OrderID, order.UserID, order.StrategyID, targetPrice)
	}
}

// Unwatch removes an order from monitoring.
// Automatically unsubscribes the token from WSS if no other orders watch it.
func (pm *PriceMonitor) Unwatch(orderID uuid.UUID) {
	// We need to find which shard this order is in.
	// Check all shards (fast — 32 shards with small maps).
	for _, shard := range pm.shards {
		shard.mu.Lock()
		entry, exists := shard.byOrder[orderID]
		if !exists {
			shard.mu.Unlock()
			continue
		}

		delete(shard.byOrder, orderID)

		// Remove from byStock slice
		key := entry.stockKey
		entries := shard.byStock[key]
		for i, e := range entries {
			if e.order.OrderID == orderID {
				shard.byStock[key] = append(entries[:i], entries[i+1:]...)
				break
			}
		}
		if len(shard.byStock[key]) == 0 {
			delete(shard.byStock, key)
		}
		shard.mu.Unlock()

		// Unsubscribe from WSS
		if pm.wsClient != nil {
			token := fmt.Sprintf("%d", entry.order.StockCode)
			pm.wsClient.Unsubscribe([]string{token})
		}
		return
	}
}

// UnwatchByStrategy removes all orders belonging to a given strategy from monitoring.
// Automatically unsubscribes tokens from WSS.
func (pm *PriceMonitor) UnwatchByStrategy(strategyID string) int {
	var unsubTokens []string
	removed := 0

	for _, shard := range pm.shards {
		shard.mu.Lock()
		for id, entry := range shard.byOrder {
			if entry.order.StrategyID != strategyID {
				continue
			}
			if pm.wsClient != nil {
				unsubTokens = append(unsubTokens, fmt.Sprintf("%d", entry.order.StockCode))
			}

			// Remove from byStock
			key := entry.stockKey
			entries := shard.byStock[key]
			for i, e := range entries {
				if e.order.OrderID == id {
					shard.byStock[key] = append(entries[:i], entries[i+1:]...)
					break
				}
			}
			if len(shard.byStock[key]) == 0 {
				delete(shard.byStock, key)
			}

			delete(shard.byOrder, id)
			log.Printf("[price-monitor] ■ Unwatched order %s (strategy %s deactivated)", id, strategyID)
			removed++
		}
		shard.mu.Unlock()
	}

	if pm.wsClient != nil && len(unsubTokens) > 0 {
		pm.wsClient.Unsubscribe(unsubTokens)
	}
	return removed
}

// WatchCount returns the number of orders currently being monitored.
func (pm *PriceMonitor) WatchCount() int {
	total := 0
	for _, shard := range pm.shards {
		shard.mu.RLock()
		total += len(shard.byOrder)
		shard.mu.RUnlock()
	}
	return total
}

// Start begins the price monitoring loop. It:
//  1. Reloads pending monitor orders from DB (restart recovery)
//  2. Starts persistent evaluation workers
//  3. Runs Redis fallback polling at checkInterval
func (pm *PriceMonitor) Start(ctx context.Context) error {
	log.Printf("[price-monitor] Starting Price Monitor (workers=%d, fallback_interval=%v, shards=%d)",
		pm.numWorkers, pm.checkInterval, shardCount)

	// Reload pending monitor orders from DB for restart recovery
	if err := pm.reloadFromDB(ctx); err != nil {
		log.Printf("[price-monitor] Warning: failed to reload pending orders from DB: %v", err)
	}

	// Start persistent evaluation workers
	var workerWg sync.WaitGroup
	for i := 0; i < pm.numWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-pm.stopChan:
					return
				case job, ok := <-pm.evalCh:
					if !ok {
						return
					}
					// Per-job recovery: a panic evaluating one watched order must
					// not kill this worker (which would reduce monitoring capacity
					// for every other order routed to it).
					func() {
						defer schedRecover("priceMonitor.evaluateEntry")
						pm.evaluateEntry(ctx, job.entry, job.ltp)
					}()
				}
			}
		}()
	}

	// Redis fallback polling ticker
	ticker := time.NewTicker(pm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[price-monitor] Stopped (context cancelled)")
			workerWg.Wait()
			return nil
		case <-pm.stopChan:
			log.Println("[price-monitor] Stopped (stop signal)")
			workerWg.Wait()
			return nil
		case <-ticker.C:
			pm.checkPricesFallback(ctx)
		}
	}
}

// Stop stops the price monitor.
func (pm *PriceMonitor) Stop() {
	close(pm.stopChan)
}

// OnPriceUpdate is the event-driven entry point called by the WSS client
// whenever a new price tick arrives for a subscribed stock.
// It immediately evaluates all orders watching that stock — zero polling delay.
//
// key format: "exchange:token" (e.g. "nse:2475")
func (pm *PriceMonitor) OnPriceUpdate(key string, ltp float64) {
	if ltp <= 0 {
		return
	}

	shard := pm.shards[shardFor(key)]

	shard.mu.RLock()
	entries := shard.byStock[key]
	if len(entries) == 0 {
		shard.mu.RUnlock()
		return
	}

	// Copy slice ref under RLock — entries are pointer-stable
	snapshot := make([]*watchEntry, len(entries))
	copy(snapshot, entries)
	shard.mu.RUnlock()

	// Fan out to worker pool
	for _, entry := range snapshot {
		if atomic.LoadInt32(&entry.triggered) != 0 {
			continue
		}
		// Non-blocking send: if evalCh is full, skip this tick (next tick will retry)
		select {
		case pm.evalCh <- evalJob{entry: entry, ltp: ltp}:
		default:
			// Channel full — workers are busy, will catch up on next price update
		}
	}
}

// stockKey builds a deduplicated cache key for a stock.
func stockKey(exchange string, token int64) string {
	return fmt.Sprintf("%s:%d", strings.ToLower(exchange), token)
}

// checkPricesFallback runs the Redis polling path for stocks not covered by WSS.
// This is the fallback — WSS-driven evaluation via OnPriceUpdate is the primary path.
func (pm *PriceMonitor) checkPricesFallback(ctx context.Context) {
	defer schedRecover("priceMonitor.checkPricesFallback")
	// Collect all unique stock keys across all shards
	allStockKeys := make(map[string]struct{}, 128)
	for _, shard := range pm.shards {
		shard.mu.RLock()
		for key := range shard.byStock {
			allStockKeys[key] = struct{}{}
		}
		shard.mu.RUnlock()
	}

	if len(allStockKeys) == 0 {
		return
	}

	// Determine which keys need Redis fallback
	uniqueKeys := make([]string, 0, len(allStockKeys))
	for k := range allStockKeys {
		uniqueKeys = append(uniqueKeys, k)
	}

	var keysToFetch []string

	// If WSS is healthy, only fetch keys that WSS doesn't have
	if pm.wsClient != nil && pm.wsClient.IsHealthy() {
		wssLTPs := pm.wsClient.GetLTPs(uniqueKeys)
		// Evaluate WSS-cached prices immediately
		for key, ltp := range wssLTPs {
			if ltp > 0 {
				pm.evaluateStock(key, ltp)
			}
		}
		// Collect keys missing from WSS for Redis fallback
		for _, k := range uniqueKeys {
			if _, ok := wssLTPs[k]; !ok {
				keysToFetch = append(keysToFetch, k)
			}
		}
	} else {
		// WSS unavailable — all keys go to Redis
		keysToFetch = uniqueKeys
	}

	// Fetch missing keys from Redis
	if len(keysToFetch) > 0 && pm.ltpProvider != nil {
		batchCtx, batchCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		redisLTPs, err := pm.ltpProvider.GetLTPs(batchCtx, keysToFetch)
		batchCancel()

		if err != nil {
			log.Printf("[price-monitor] Redis fallback LTP fetch failed: %v (keys=%d)", err, len(keysToFetch))
		} else {
			for key, ltp := range redisLTPs {
				if ltp > 0 {
					pm.evaluateStock(key, ltp)
				}
			}
		}
	}

	// Notify listener (e.g. WS server) that a tick completed
	if pm.onTickDone != nil {
		pm.onTickDone()
	}
}

// evaluateStock fans out a price update for a single stock to the worker pool.
func (pm *PriceMonitor) evaluateStock(key string, ltp float64) {
	shard := pm.shards[shardFor(key)]

	shard.mu.RLock()
	entries := shard.byStock[key]
	if len(entries) == 0 {
		shard.mu.RUnlock()
		return
	}
	snapshot := make([]*watchEntry, len(entries))
	copy(snapshot, entries)
	shard.mu.RUnlock()

	for _, entry := range snapshot {
		if atomic.LoadInt32(&entry.triggered) != 0 {
			continue
		}
		select {
		case pm.evalCh <- evalJob{entry: entry, ltp: ltp}:
		default:
		}
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

	// Check upper bound: if LTP has overshot past maxPrice, skip the order.
	// This prevents triggering when the stock has moved beyond the user's
	// max_pct_change threshold (e.g., min=0.5%, max=2% but stock jumped to 3%).
	if entry.maxPrice > 0 {
		exceeded := false
		if isBuy {
			exceeded = ltp > entry.maxPrice
		} else {
			exceeded = ltp < entry.maxPrice
		}
		if exceeded {
			log.Printf("[price-monitor] ✗ SKIPPED %s:%s (order=%s) LTP=%.2f exceeded max=%.2f — removing from watch",
				entry.order.Exchange, entry.order.Symbol, entry.order.OrderID, ltp, entry.maxPrice)
			// Mark as triggered to prevent re-evaluation, then unwatch
			atomic.StoreInt32(&entry.triggered, 1)
			go func() {
				defer schedRecover("priceMonitor.unwatchMaxExceeded")
				pm.Unwatch(entry.order.OrderID)
				// Mark as CANCELLED in DB
				if pm.orderRepo != nil {
					pm.orderRepo.UpdateStatus(ctx, entry.order.OrderID, models.StatusCancelled)
					pm.orderRepo.RecordExecutionEvent(ctx, entry.order.OrderID, "PRICE_EXCEEDED_MAX", map[string]interface{}{
						"ltp":       ltp,
						"max_price": entry.maxPrice,
						"target":    entry.targetPrice,
					})
				}
			}()
			return
		}
	}

	// Atomically claim this trigger (prevents duplicate execution)
	if !atomic.CompareAndSwapInt32(&entry.triggered, 0, 1) {
		return
	}
	// Stamp the exact instant the price condition was met — used to order the
	// strategy trade-cap reservation (earliest crosser wins the slot) and audit.
	entry.triggeredAt = time.Now()

	log.Printf("[price-monitor] ✓ TRIGGERED %s:%s (order=%s) LTP=%.2f target=%.2f — placing order",
		entry.order.Exchange, entry.order.Symbol, entry.order.OrderID, ltp, entry.targetPrice)

	// Execute in a goroutine to not block the evaluation workers
	go pm.triggerOrder(ctx, entry, ltp)
}

// isTerminalStatus returns true if the order status means no further execution should happen.
func isTerminalStatus(s models.OrderStatus) bool {
	return models.IsTerminalStatus(s)
}

// triggerOrder updates the order with the current LTP and calls the executor.
func (pm *PriceMonitor) triggerOrder(ctx context.Context, entry *watchEntry, ltp float64) {
	defer schedRecover("priceMonitor.triggerOrder")
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

	// ── Enforce the strategy's daily trade cap (hard ceiling) ────────────────
	// This watch only becomes a real trade now, at trigger time, so this is where
	// it consumes a slot. Reserve atomically on the shared counter before we
	// announce or place anything. If the strategy has already used its budget
	// (earlier immediate trades or earlier-triggering watches won the slots),
	// cancel this watch instead of placing — FCFS. A transient Redis error is
	// treated like an execute failure so it retries rather than bypassing the cap.
	reserved, capReached, capErr := pm.reserveStrategySlot(ctx, order)
	if capErr != nil {
		attempts := atomic.AddInt32(&entry.triggerAttempts, 1)
		log.Printf("[price-monitor] ✗ trade-cap reserve error for order %s (attempt %d/%d): %v",
			order.OrderID, attempts, maxTriggerAttempts, capErr)
		if attempts >= maxTriggerAttempts {
			pm.Unwatch(order.OrderID)
			return
		}
		atomic.StoreInt32(&entry.triggered, 0) // allow retry on the next tick
		return
	}
	if capReached {
		pm.cancelForCapReached(ctx, entry, ltp)
		return
	}

	// Promote from STOP_LOSS → LIMIT so the executor places the order at broker.
	// The PriceMonitor already waited for the price to reach the target level.
	// Set limit price = LTP + 0.5% buffer to cross the spread and fill immediately.
	order.OrderType = models.OrderTypeLimit

	// If onAfterFill is set, an ML manager or OCO manager will place its own SL/TP
	// legs after the entry fills. Keep ProductType as INTRADAY so the broker does NOT
	// place bracket legs that would conflict (or be rejected when SL/TP=0 in ML mode).
	slVal := 0.0
	tpVal := 0.0
	if order.StopLoss != nil {
		slVal = *order.StopLoss
	}
	if order.TakeProfit != nil {
		tpVal = *order.TakeProfit
	}
	if entry.onAfterFill != nil || (slVal == 0 && tpVal == 0) {
		order.ProductType = "INTRADAY"
	}
	adjustedLimit := ltp * 1.005

	// Fetch per-instrument tick size from Redis; fall back to NSE default (0.05).
	tickSize := 0.05
	if pm.ltpProvider != nil {
		if ts, err := pm.ltpProvider.GetTickSize(ctx, string(order.Exchange), order.StockCode); err == nil && ts > 0 {
			tickSize = ts
		}
	}
	roundedLimit := indiraClient.RoundToTickSize(adjustedLimit, tickSize)
	order.Price = &roundedLimit

	if !order.IsPaperTrade && entry.onAfterFill != nil {
		// LIVE only: an OCO/ML manager will place the SL/TP legs after this entry
		// fills, from the actual fill price. The entry itself must be a clean
		// marketable LIMIT — leaving order.StopLoss set would make
		// convertToIndiraRequest emit triggerPrice = StopLoss and turn the entry
		// into a broker SL/stop order, which the broker rejects when the SL sits
		// far from LTP (wide fixed SL%). Paper never reaches the broker, so its
		// SL/TP are preserved via the recompute below (paper executor reads them).
		order.StopLoss = nil
		order.TakeProfit = nil
	} else {
		// Paper trades, or a live entry that carries its own broker bracket legs:
		// recompute SL/TP from the actual fill price (roundedLimit) rather than
		// targetMonitorPrice. SL/TP were set as targetMonitorPrice*(1±slPct/100),
		// so the ratio order.StopLoss/targetPrice preserves (1-slPct/100) exactly.
		targetPrice := entry.targetPrice
		if targetPrice > 0 {
			if order.StopLoss != nil && *order.StopLoss > 0 {
				ratio := *order.StopLoss / targetPrice
				newSL := indiraClient.RoundToTickSize(roundedLimit*ratio, tickSize)
				order.StopLoss = &newSL
			}
			if order.TakeProfit != nil && *order.TakeProfit > 0 {
				ratio := *order.TakeProfit / targetPrice
				newTP := indiraClient.RoundToTickSize(roundedLimit*ratio, tickSize)
				order.TakeProfit = &newTP
			}
		}
	}

	log.Printf("[price-monitor] Limit price set: LTP=%.2f → limit=%.2f (+0.5%% buffer, tick=%.2f)",
		ltp, roundedLimit, tickSize)

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

	// Call the executor to place the order at broker.
	// For paper trades: fills synchronously and sets order.FilledPrice/FilledQuantity.
	// onAfterFill (if set) is called on success to wire ML exit setup for
	// below_min + MULTI_LEVEL orders.
	if err := pm.executeFn.ExecuteOrder(ctx, order); err != nil {
		// This attempt's reservation will not become a trade — hand the slot back
		// so a failed placement never permanently consumes the strategy's budget.
		// (A retry re-reserves; giving up leaves the slot free for another stock.)
		pm.releaseStrategySlot(ctx, order, reserved)

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

	if entry.onAfterFill != nil {
		entry.onAfterFill(order)
	}
}

// CancelWatch removes an order from monitoring and marks it as CANCELLED in the DB.
// Returns true if the order was found and cancelled, false if it wasn't being watched.
func (pm *PriceMonitor) CancelWatch(ctx context.Context, orderID uuid.UUID, userID string) bool {
	// Find the order across shards
	var foundEntry *watchEntry
	for _, shard := range pm.shards {
		shard.mu.RLock()
		entry, exists := shard.byOrder[orderID]
		shard.mu.RUnlock()
		if exists {
			foundEntry = entry
			break
		}
	}

	if foundEntry == nil {
		return false
	}

	// Verify user owns this order
	if foundEntry.order.UserID != userID {
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
			"target_price": foundEntry.targetPrice,
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
			Message:    fmt.Sprintf("Cancelled price watch for %s (target ₹%.2f)", foundEntry.order.Symbol, foundEntry.targetPrice),
			Status:     string(models.StatusCancelled),
			CreatedAt:  now,
			ExpiresAt:  now.Add(1 * time.Hour),
			OrderSummary: models.OrderSummary{
				Stock:     foundEntry.order.Symbol,
				Action:    string(foundEntry.order.OrderSide),
				Quantity:  foundEntry.order.Quantity,
				Exchange:  string(foundEntry.order.Exchange),
				OrderType: string(foundEntry.order.OrderType),
				Price:     fmt.Sprintf("₹%.2f", foundEntry.targetPrice),
			},
			NotificationChannels: models.NotificationChannels{Push: false, InApp: true},
		}
		if err := pm.kafkaPub.PublishOrderUpdate(ctx, update); err != nil {
			log.Printf("[price-monitor] Failed to publish cancel event for order %s: %v", orderID, err)
		}
	}

	log.Printf("[price-monitor] ■ User %s cancelled watch for order %s (%s target=%.2f) (remaining: %d)",
		userID, orderID, foundEntry.order.Symbol, foundEntry.targetPrice, pm.WatchCount())
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
	MaxPrice    float64 `json:"max_price,omitempty"`
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
	result := make([]WatchSnapshot, 0, 64)

	for _, shard := range pm.shards {
		shard.mu.RLock()
		for _, e := range shard.byOrder {
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
				MaxPrice:    e.maxPrice,
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
		shard.mu.RUnlock()
	}
	return result
}

// reloadFromDB loads pending STOP_LOSS+BRACKET orders from the DB and re-registers
// them for monitoring. Also subscribes all reloaded tokens on the WSS client.
//
// Restart recovery: rebuilds the onAfterFill closure that the original signal
// processor attached in-memory. Without this, a below_min order persisted to DB
// would, on restart, trigger an entry placement but skip OCO adoption — the
// position would fill with no SL/TP protection. The SL/TP percentages and
// trailing flag are derived from the persisted order fields, so the closure
// can be reconstructed without the original kafka signal context.
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
		pm.Watch(order, *order.Price, pm.buildReloadOnAfterFill(order))
	}

	if len(orders) > 0 {
		log.Printf("[price-monitor] Reloaded %d pending monitor orders from DB", len(orders))
	}

	return nil
}

// buildReloadOnAfterFill reconstructs the onAfterFill closure for a price-monitor
// order loaded from DB. Returns nil if no OCO/SL/TP protection is configured or
// if the adopter isn't wired (paper trades, or no SL/TP intent).
//
// SL/TP percentages are recovered from the absolute prices stored on the order:
// order.StopLoss / order.TakeProfit hold target_price × (1 ± pct/100), so the
// percentage is the deviation of that ratio from 1. This matches the rescaling
// the live triggerOrder path does at fill time, so the percentages we hand to
// the adopter are the same ones the original signal carried.
func (pm *PriceMonitor) buildReloadOnAfterFill(order *models.Order) func(*models.Order) {
	if order == nil || order.IsPaperTrade {
		return nil // paper trades use a different exit path (ML manager simulates)
	}
	if pm.ocoAdopter == nil {
		// Adopter not wired — log once so missing protection is observable.
		log.Printf("[price-monitor] Reloaded order %s has no OCO adopter wired — SL/TP will be unprotected if it triggers",
			order.OrderID)
		return nil
	}
	if order.Price == nil || *order.Price <= 0 {
		return nil
	}
	target := *order.Price

	// Derive SL% from the stored absolute SL price. For BUY: SL = target × (1 - p/100)
	// → p = (1 - SL/target) × 100. For SELL the sign is flipped. The ratio path
	// is robust to either side since we apply Abs at the end.
	var slPct, tpPct float64
	if order.StopLoss != nil && *order.StopLoss > 0 {
		ratio := *order.StopLoss / target
		slPct = (1 - ratio) * 100
		if slPct < 0 {
			slPct = -slPct
		}
	}
	if order.TakeProfit != nil && *order.TakeProfit > 0 {
		ratio := *order.TakeProfit / target
		tpPct = (ratio - 1) * 100
		if tpPct < 0 {
			tpPct = -tpPct
		}
	}

	trailing := order.StopLossType != nil && *order.StopLossType == "TRAILING"
	trailPct := 0.0
	if order.TrailingSLPct != nil {
		trailPct = *order.TrailingSLPct
	}

	// If nothing to protect, skip — caller would have routed plain instead.
	if slPct <= 0 && tpPct <= 0 {
		return nil
	}

	adopter := pm.ocoAdopter // capture so the field can be swapped later without affecting in-flight closures
	return func(filled *models.Order) {
		if filled.IndiraOrderID == nil || *filled.IndiraOrderID == "" {
			return
		}
		var auth *indiraClient.AuthContext
		if filled.BearerToken != nil && filled.AppId != nil && filled.Source != nil {
			auth = &indiraClient.AuthContext{
				UserId:      filled.UserID,
				BearerToken: *filled.BearerToken,
				AppId:       *filled.AppId,
				Source:      *filled.Source,
			}
		}
		log.Printf("[price-monitor] Reload-adopt: order=%s sl=%.2f%% tp=%.2f%% trailing=%v trailPct=%.2f",
			filled.OrderID, slPct, tpPct, trailing, trailPct)
		adopter.AdoptOrder(filled, slPct, tpPct, trailing, trailPct, auth)
	}
}
