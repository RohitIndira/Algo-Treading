package multilevel

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// BrokerOrderPlacer is the broker client subset used by the ML manager.
// *indira.ExecutionClient satisfies this interface.
type BrokerOrderPlacer interface {
	PlaceOrder(ctx context.Context, order *models.Order, auth *indiraClient.AuthContext) (string, error)
	ModifyOrder(ctx context.Context, order *models.Order, auth *indiraClient.AuthContext) error
	CancelOrder(ctx context.Context, exchange, orderID, symbol string, auth *indiraClient.AuthContext) error
}

// WSClient is the market WSS client subset used for subscription management.
// *marketws.Client satisfies this interface.
type WSClient interface {
	Subscribe(tokens []string)
	Unsubscribe(tokens []string)
}

// PriceLookup fetches LTP from Redis (optional — used for paper fill prices).
// *paper.RedisPriceClient satisfies this interface.
type PriceLookup interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

const (
	numShards  = 16
	evalBufLen = 8192
	numWorkers = 32
)

type evalJob struct {
	group *Group
	ltp   float64
}

type mlStockShard struct {
	mu     sync.RWMutex
	groups map[int64][]*Group // stockCode → groups watching this stock
}

// Manager is the shared, event-driven multi-level SL/TP manager.
// Replaces per-group Redis polling goroutines with a shared worker pool
// fed by WSS price events, supporting many concurrent users.
type Manager struct {
	// shards holds active (post-fill) groups indexed by stockCode for price evaluation.
	shards [numShards]mlStockShard

	// groupsByEntry holds ALL groups (pre- and post-fill) keyed by entry order UUID.
	groupsByEntry sync.Map // uuid.UUID → *Group

	// singleSLIndex maps a broker SL order ID to its group, for EXECUTED callbacks.
	singleSLIndex sync.Map // brokerOrderID string → *Group

	// mlLevelIndex maps a per-level broker order ID to its group and level metadata.
	// Used to route EXECUTED callbacks for multi-level SL/TP broker orders.
	mlLevelIndex sync.Map // brokerOrderID string → *mlLevelRef

	evalCh chan evalJob

	ws          WSClient
	broker      BrokerOrderPlacer
	repo        repository.OrderRepository
	priceLookup PriceLookup // optional Redis LTP for paper trades
	logger      *zap.Logger

	started int32

	// Paper event hooks — set from main.go after construction.
	OnPaperGroupCompleted func(userID, orderID string, finalPnL, avgExitPrice float64)
	OnPaperLevelTriggered func(userID, orderID, exitType string, levelNum int, exitPrice float64, remainingQty int32, cancelledExitType string, cancelledLevelNum int)
	OnPaperQtyUpdated     func(entryOrderID uuid.UUID, remainingQty int32)
	OnPaperSLMoved        func(entryOrderID uuid.UUID, newSL float64)
}

// NewManager creates a new Manager.
// Call SetWSClient then Start before processing price events.
func NewManager(
	repo repository.OrderRepository,
	priceLookup PriceLookup,
	broker BrokerOrderPlacer,
	logger *zap.Logger,
) *Manager {
	m := &Manager{
		repo:        repo,
		priceLookup: priceLookup,
		broker:      broker,
		logger:      logger,
		evalCh:      make(chan evalJob, evalBufLen),
	}
	for i := range m.shards {
		m.shards[i].groups = make(map[int64][]*Group)
	}
	return m
}

// SetWSClient wires the market WSS client for stock subscription management.
func (m *Manager) SetWSClient(ws WSClient) {
	m.ws = ws
}

// Start launches the shared worker pool. Call once after SetWSClient.
func (m *Manager) Start(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&m.started, 0, 1) {
		return
	}
	for i := 0; i < numWorkers; i++ {
		go m.worker(ctx)
	}
}

func (m *Manager) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-m.evalCh:
			m.runEvalJob(ctx, job)
		}
	}
}

// OnPriceUpdate is called by the WSS price feed on every tick.
// key format: "exchange:token" (e.g. "nse:2475"); ltp is the last traded price.
func (m *Manager) OnPriceUpdate(key string, ltp float64) {
	stockCode := stockCodeFromKey(key)
	if stockCode <= 0 {
		return
	}

	shard := m.shardForStock(stockCode)
	shard.mu.RLock()
	list := shard.groups[stockCode]
	if len(list) == 0 {
		shard.mu.RUnlock()
		return
	}
	snapshot := make([]*Group, len(list))
	copy(snapshot, list)
	shard.mu.RUnlock()

	for _, g := range snapshot {
		g.mu.Lock()
		active := g.State == GroupStateActive && g.entryFilled
		g.mu.Unlock()
		if !active {
			continue
		}
		select {
		case m.evalCh <- evalJob{group: g, ltp: ltp}:
		default:
			// channel full — drop; next tick re-evaluates
		}
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// MultiLevelManager interface (executor.MultiLevelManager)
// ══════════════════════════════════════════════════════════════════════════════

// RegisterEntry stores the entry order configuration before the entry order is placed.
// Subscription to the worker pool happens in OnEntryFill once fill price and qty are known.
func (m *Manager) RegisterEntry(
	entryOrder *models.Order,
	slMode, tpMode string,
	slLevels, tpLevels []models.MultiLevelExitLevel,
	fixedSLPct, trailingSLPct float64,
	auth *indiraClient.AuthContext,
) {
	if slMode == "" {
		slMode = SLModeFixed
	}
	if tpMode == "" {
		tpMode = TPModeFixed
	}

	var slConfigs, tpConfigs []SLTPLevelConfig
	for _, l := range slLevels {
		slConfigs = append(slConfigs, SLTPLevelConfig{LevelNum: l.LevelNum, PricePct: l.PricePct, QtyPct: l.QtyPct})
	}
	for _, l := range tpLevels {
		tpConfigs = append(tpConfigs, SLTPLevelConfig{LevelNum: l.LevelNum, PricePct: l.PricePct, QtyPct: l.QtyPct})
	}

	var bearerToken, appID, source string
	if auth != nil {
		bearerToken = auth.BearerToken
		appID = auth.AppId
		source = auth.Source
	}

	g := &Group{
		GroupID:        uuid.New(),
		EntryOrderID:   entryOrder.OrderID,
		UserID:         entryOrder.UserID,
		Symbol:         entryOrder.Symbol,
		Exchange:       string(entryOrder.Exchange),
		StockCode:      entryOrder.StockCode,
		OrderSide:      string(entryOrder.OrderSide),
		ProductType:    entryOrder.ProductType,
		Validity:       entryOrder.Validity,
		SLMode:         slMode,
		FixedSLPct:     fixedSLPct,
		TrailingSLPct:  trailingSLPct,
		TPMode:         tpMode,
		SLLevelConfigs: slConfigs,
		TPLevelConfigs: tpConfigs,
		State:          GroupStateActive,
		TradingMode:    entryOrder.TradingMode,
		Auth:           auth,
		broker:         m.broker,
		StrategyID:     entryOrder.StrategyID,
		StrategyName:   entryOrder.StrategyName,
		EventID:        entryOrder.EventID,
		AuthBearer:     bearerToken,
		AuthAppID:      appID,
		AuthSource:     source,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	m.groupsByEntry.Store(entryOrder.OrderID, g)
}

// OnEntryFill is called once the entry order confirms filled (paper: immediately; live: via HandleBrokerUpdate).
// Computes trigger prices for all exit levels, persists them, places broker SL orders (live),
// then subscribes the group to the shared price evaluation pool.
func (m *Manager) OnEntryFill(
	ctx context.Context,
	entryOrderID uuid.UUID,
	fillPrice float64,
	filledQty int32,
	slLevels, tpLevels []models.MultiLevelExitLevel,
) {
	val, ok := m.groupsByEntry.Load(entryOrderID)
	if !ok {
		m.logger.Warn("OnEntryFill: no group registered for entry order",
			zap.String("entry_order_id", entryOrderID.String()))
		return
	}
	g := val.(*Group)

	g.mu.Lock()
	if g.entryFilled {
		g.mu.Unlock()
		return // idempotent
	}
	g.FillPrice = fillPrice
	g.TotalQty = filledQty
	g.RemainingQty = filledQty
	g.entryFilled = true
	g.HighestPrice = fillPrice

	// Use passed slLevels/tpLevels if available, otherwise fall back to stored configs.
	if len(slLevels) > 0 {
		g.SLLevelConfigs = make([]SLTPLevelConfig, len(slLevels))
		for i, l := range slLevels {
			g.SLLevelConfigs[i] = SLTPLevelConfig{LevelNum: l.LevelNum, PricePct: l.PricePct, QtyPct: l.QtyPct}
		}
	}
	if len(tpLevels) > 0 {
		g.TPLevelConfigs = make([]SLTPLevelConfig, len(tpLevels))
		for i, l := range tpLevels {
			g.TPLevelConfigs[i] = SLTPLevelConfig{LevelNum: l.LevelNum, PricePct: l.PricePct, QtyPct: l.QtyPct}
		}
	}

	// Sort configs by LevelNum so the "last level absorbs the remainder" rule in
	// computeInitialExitQtys lands on the highest-numbered level deterministically.
	sort.Slice(g.SLLevelConfigs, func(i, j int) bool { return g.SLLevelConfigs[i].LevelNum < g.SLLevelConfigs[j].LevelNum })
	sort.Slice(g.TPLevelConfigs, func(i, j int) bool { return g.TPLevelConfigs[i].LevelNum < g.TPLevelConfigs[j].LevelNum })

	// Build SL exit level states. Quantities are computed via computeInitialExitQtys
	// so the rounding remainder lands on the last level and Σ ExitQty == filledQty.
	slQtys := computeInitialExitQtys(g.SLLevelConfigs, filledQty, m.logger, "SL", entryOrderID)
	for i, cfg := range g.SLLevelConfigs {
		qty := slQtys[i]
		g.SLLevels = append(g.SLLevels, &ExitLevelState{
			LevelNum:        cfg.LevelNum,
			TriggerPrice:    g.CalcSLTriggerPrice(cfg.PricePct),
			ExitQty:         qty,
			OriginalExitQty: qty,
			Status:          LevelActive,
			ExitOrderID:     uuid.New(),
		})
	}

	// Build TP exit level states (same remainder-on-last-level rule as SL).
	tpQtys := computeInitialExitQtys(g.TPLevelConfigs, filledQty, m.logger, "TP", entryOrderID)
	for i, cfg := range g.TPLevelConfigs {
		qty := tpQtys[i]
		g.TPLevels = append(g.TPLevels, &ExitLevelState{
			LevelNum:        cfg.LevelNum,
			TriggerPrice:    g.CalcTPLimitPrice(cfg.PricePct),
			ExitQty:         qty,
			OriginalExitQty: qty,
			Status:          LevelActive,
			ExitOrderID:     uuid.New(),
		})
	}
	g.mu.Unlock()

	// Persist exit levels to DB
	go m.persistExitLevels(ctx, g)

	// For live trades: place broker-side SL and TP orders.
	if g.TradingMode == "LIVE" {
		switch g.SLMode {
		case SLModeFixed:
			go m.placeFixedSLOrder(ctx, g, filledQty)
		case SLModeTrailing:
			go m.placeLiveTrailingSL(ctx, g)
		case SLModeMultiLevel:
			// Each SL level becomes a real broker SL-M order.
			// Broker WS EXECUTED callbacks route through mlLevelIndex.
			go m.placeMultiSLOrders(ctx, g)
		}
		if g.TPMode == TPModeMultiLevel {
			// Each TP level becomes a real broker LIMIT order.
			// Broker WS EXECUTED callbacks route through mlLevelIndex.
			go m.placeMultiTPOrders(ctx, g)
		}
	}

	m.subscribeGroup(g)
}

// ══════════════════════════════════════════════════════════════════════════════
// MLHandler interface (statusservice.MLHandler)
// ══════════════════════════════════════════════════════════════════════════════

// HandleBrokerUpdate processes a broker WS order status event.
// Detects live entry fills and single SL fills.
func (m *Manager) HandleBrokerUpdate(ctx context.Context, order *models.Order, brokerStatus string) {
	if order == nil {
		return
	}

	// Check if this is a live entry fill for a registered group.
	val, ok := m.groupsByEntry.Load(order.OrderID)
	if ok {
		g := val.(*Group)
		g.mu.Lock()
		alreadyFilled := g.entryFilled
		g.mu.Unlock()

		if !alreadyFilled && isFilledStatus(brokerStatus) && order.FilledQuantity > 0 && order.FilledPrice != nil {
			fillPrice := *order.FilledPrice
			go m.OnEntryFill(ctx, order.OrderID, fillPrice, order.FilledQuantity, nil, nil)
			return
		}
	}

	// Check per-level multi-level SL/TP fills (broker orders placed by placeMultiSLOrders / placeMultiTPOrders).
	if isFilledStatus(brokerStatus) && order.IndiraOrderID != nil {
		if val, ok := m.mlLevelIndex.Load(*order.IndiraOrderID); ok {
			ref := val.(*mlLevelRef)
			go m.onMultiLevelFilled(ctx, ref.group, ref.exitType, ref.levelNum, order.FilledPrice, order.FilledQuantity)
			return
		}
	}

	// Check single SL fill (FIXED or TRAILING).
	if isFilledStatus(brokerStatus) && order.IndiraOrderID != nil {
		if val, ok := m.singleSLIndex.Load(*order.IndiraOrderID); ok {
			g := val.(*Group)
			go m.onSingleSLFilled(ctx, g, order.FilledPrice, order.FilledQuantity)
		}
	}
}

// CancelGroupsBySymbol cancels all active groups for the given user and symbol.
// Used during force-exit (auto square-off or user-initiated close).
func (m *Manager) CancelGroupsBySymbol(ctx context.Context, userID string, symbol string) {
	symUpper := strings.ToUpper(symbol)
	m.groupsByEntry.Range(func(key, val interface{}) bool {
		g := val.(*Group)
		g.mu.Lock()
		matches := g.UserID == userID && strings.ToUpper(g.Symbol) == symUpper && g.State == GroupStateActive
		g.mu.Unlock()
		if matches {
			m.cancelGroupInternal(ctx, g)
		}
		return true
	})
}

// IsKnownBrokerID returns true if the broker order ID belongs to a registered ML group.
// Used by the status service to avoid double-handling by OCO and ML managers.
func (m *Manager) IsKnownBrokerID(brokerID string) bool {
	if _, ok := m.singleSLIndex.Load(brokerID); ok {
		return true
	}
	_, ok := m.mlLevelIndex.Load(brokerID)
	return ok
}

// ══════════════════════════════════════════════════════════════════════════════
// MLGroupCanceller interface (paper.MLGroupCanceller)
// ══════════════════════════════════════════════════════════════════════════════

// CancelGroup cancels the ML group associated with the given entry order.
func (m *Manager) CancelGroup(ctx context.Context, entryOrderID uuid.UUID) {
	val, ok := m.groupsByEntry.Load(entryOrderID)
	if !ok {
		return
	}
	m.cancelGroupInternal(ctx, val.(*Group))
}

func (m *Manager) cancelGroupInternal(ctx context.Context, g *Group) {
	g.mu.Lock()
	if g.State != GroupStateActive {
		g.mu.Unlock()
		return
	}
	g.State = GroupStateCancelled
	oldSLBrokerID := g.SingleSLBrokerID
	exchange := g.Exchange
	symbol := g.Symbol
	auth := g.Auth
	remaining := g.RemainingQty
	g.mu.Unlock()

	m.groupsByEntry.Delete(g.EntryOrderID)
	m.unsubscribeGroup(g)
	if oldSLBrokerID != "" {
		m.singleSLIndex.Delete(oldSLBrokerID)
	}

	// Cancel single SL broker order (FIXED/TRAILING live only).
	if g.TradingMode == "LIVE" && oldSLBrokerID != "" && auth != nil {
		go func() {
			if err := m.broker.CancelOrder(ctx, exchange, oldSLBrokerID, symbol, auth); err != nil {
				m.logger.Warn("CancelGroup: broker SL cancel failed",
					zap.String("group", g.GroupID.String()),
					zap.Error(err))
			}
		}()
	}

	// Cancel any active multi-level SL and TP broker orders (live only).
	if g.TradingMode == "LIVE" && auth != nil {
		m.cancelRemainingMLOrders(ctx, g, g.SLLevels, auth)
		m.cancelRemainingMLOrders(ctx, g, g.TPLevels, auth)
	}

	_ = remaining // persist if needed
}

// cancelSLOrder cancels the single SL broker order for the group (FIXED/TRAILING mode).
func (m *Manager) cancelSLOrder(ctx context.Context, group *Group) {
	group.mu.Lock()
	brokerID := group.SingleSLBrokerID
	group.SingleSLBrokerID = ""
	group.mu.Unlock()

	if brokerID == "" || group.broker == nil || group.Auth == nil {
		return
	}
	if err := group.broker.CancelOrder(ctx, group.Exchange, brokerID, group.Symbol, group.Auth); err != nil {
		m.logger.Warn("ml_cancel_sl_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.String("broker_id", brokerID),
			zap.Error(err))
	}
}

// cancelTPOrder cancels a single active TP limit order and marks the level cancelled.
func (m *Manager) cancelTPOrder(ctx context.Context, group *Group, level *ExitLevelState, brokerID string) {
	if group.broker == nil || group.Auth == nil {
		return
	}
	if err := group.broker.CancelOrder(ctx, group.Exchange, brokerID, group.Symbol, group.Auth); err != nil {
		m.logger.Warn("ml_cancel_tp_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("level", level.LevelNum),
			zap.Error(err))
	} else {
		level.markCancelled()
		m.mlLevelIndex.Delete(brokerID)
		_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeTP, level.LevelNum,
			models.MLStatusCancelled, 0)
	}
}

// recordPaperPartialExit writes a paper closed-position row for one partial ML exit
// (SL/TP level or strategy deactivation). The row appears in the Closed Positions tab.
func (m *Manager) recordPaperPartialExit(ctx context.Context, group *Group, qty int32, exitPrice float64, reason string, levelNum int) {
	if qty <= 0 {
		m.logger.Warn("ml_paper_exit_skipped_zero_qty",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("level", levelNum),
			zap.String("reason", reason))
		return
	}
	log.Printf("[ml] Paper partial exit: group=%s level=%d qty=%d price=%.2f reason=%s",
		group.GroupID, levelNum, qty, exitPrice, reason)

	reverseSide := "SELL"
	if group.OrderSide == "SELL" {
		reverseSide = "BUY"
	}

	paperID := fmt.Sprintf("PAPER-ML-%s-L%d-%s", group.EntryOrderID.String()[:8], levelNum, reason)
	now := time.Now()
	entryPrice := group.FillPrice

	var partialPnL float64
	if group.OrderSide == "BUY" {
		partialPnL = (exitPrice - entryPrice) * float64(qty)
	} else {
		partialPnL = (entryPrice - exitPrice) * float64(qty)
	}

	exitOrder := &models.Order{
		OrderID:          uuid.New(),
		UserID:           group.UserID,
		StrategyID:       group.StrategyID,
		StrategyName:     group.StrategyName,
		StockCode:        group.StockCode,
		Exchange:         models.Exchange(group.Exchange),
		Symbol:           group.Symbol,
		OrderType:        models.OrderTypeMarket,
		OrderSide:        models.OrderSide(reverseSide),
		Quantity:         qty,
		Price:            &entryPrice,
		Validity:         "IOC",
		ProductType:      group.ProductType,
		Status:           models.StatusFilled,
		IsPaperTrade:     true,
		TradingMode:      "PAPER",
		FilledQuantity:   qty,
		FilledPrice:      &exitPrice,
		PaperExitPrice:   &exitPrice,
		PaperPnL:         &partialPnL,
		IsSquareOffOrder: true,
		RiskApproved:     true,
		IndiraOrderID:    &paperID,
		SubmittedAt:      &now,
		ExecutedAt:       &now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := m.repo.CreatePaperPartialExit(ctx, exitOrder); err != nil {
		m.logger.Error("ml_paper_exit_order_create_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Error(err))
	}
}

// CancelGroupForExit cancels an active ML group and, for paper positions with remaining
// qty, records a partial exit at exitPrice. Used during square-off and force-exit.
func (m *Manager) CancelGroupForExit(ctx context.Context, entryOrderID uuid.UUID, exitPrice float64, reason string) {
	v, ok := m.groupsByEntry.Load(entryOrderID)
	if !ok {
		return
	}
	group := v.(*Group)

	group.mu.Lock()
	if group.State != GroupStateActive {
		group.mu.Unlock()
		return
	}
	remaining := group.RemainingQty
	tradingMode := group.TradingMode
	group.State = GroupStateCancelled
	cancel := group.cancelMonitor
	group.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	for _, level := range group.TPLevels {
		level.mu.Lock()
		if level.Status == LevelActive && level.BrokerOrderID != "" {
			bid := level.BrokerOrderID
			level.mu.Unlock()
			m.cancelTPOrder(ctx, group, level, bid)
		} else {
			level.mu.Unlock()
		}
	}
	m.cancelSLOrder(ctx, group)

	if tradingMode == "PAPER" && remaining > 0 && exitPrice > 0 {
		m.recordPaperPartialExit(ctx, group, remaining, exitPrice, reason, 0)
		if err := m.repo.UpdatePaperPositionFilledQty(ctx, entryOrderID, 0); err != nil {
			m.logger.Warn("ml_cancel_for_exit_qty_update_failed",
				zap.String("group_id", group.GroupID.String()),
				zap.Error(err))
		}
	}

	m.groupsByEntry.Delete(entryOrderID)
	m.unsubscribeGroup(group)
	m.singleSLIndex.Delete(group.SingleSLBrokerID)
	m.logger.Info("ml_group_cancelled_for_exit",
		zap.String("group_id", group.GroupID.String()),
		zap.Float64("exit_price", exitPrice),
		zap.String("reason", reason),
		zap.Int32("remaining_qty", remaining))
}

// CancelGroupsByStrategy cancels all active groups for a user+strategy.
// For paper positions with remaining qty, a partial exit is recorded at the current
// LTP so the closed-positions tab shows a row for the deactivation exit.
// Called by StrategyEventsConsumer when a strategy is paused or deleted.
func (m *Manager) CancelGroupsByStrategy(ctx context.Context, userID, strategyID string) {
	m.groupsByEntry.Range(func(k, v interface{}) bool {
		g := v.(*Group)
		if g.UserID != userID || g.StrategyID != strategyID {
			return true
		}

		g.mu.Lock()
		if g.State != GroupStateActive {
			g.mu.Unlock()
			return true
		}
		remaining := g.RemainingQty
		tradingMode := g.TradingMode
		g.State = GroupStateCancelled
		cancel := g.cancelMonitor
		g.mu.Unlock()

		if cancel != nil {
			cancel()
		}

		if tradingMode == "PAPER" && remaining > 0 && m.priceLookup != nil {
			ltp, err := m.priceLookup.GetLTP(ctx, g.Exchange, g.StockCode)
			if err == nil && ltp > 0 {
				m.recordPaperPartialExit(ctx, g, remaining, ltp, "STRATEGY_DEACTIVATED", 0)
				if err2 := m.repo.UpdatePaperPositionFilledQty(ctx, g.EntryOrderID, 0); err2 != nil {
					m.logger.Warn("ml_cancel_strategy_qty_update_failed",
						zap.String("group_id", g.GroupID.String()),
						zap.Error(err2))
				}
			} else {
				m.logger.Warn("ml_cancel_strategy_ltp_unavailable",
					zap.String("group_id", g.GroupID.String()),
					zap.String("symbol", g.Symbol),
					zap.Error(err))
			}
		}

		m.groupsByEntry.Delete(g.EntryOrderID)
		m.unsubscribeGroup(g)
		m.logger.Info("ml_group_cancelled_by_strategy",
			zap.String("group_id", g.GroupID.String()),
			zap.String("strategy_id", strategyID),
			zap.Int32("remaining_qty", remaining))

		return true
	})
}

// ══════════════════════════════════════════════════════════════════════════════
// Internal: price evaluation worker
// ══════════════════════════════════════════════════════════════════════════════

func (m *Manager) runEvalJob(ctx context.Context, job evalJob) {
	g := job.group
	ltp := job.ltp

	if !g.tryClaimEvaluation() {
		return // another worker is already evaluating this group
	}
	defer g.releaseEvaluation()

	g.mu.Lock()
	if g.State != GroupStateActive {
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()

	// SL evaluated first; if a multi-level SL fires, skip TP for this tick.
	slFired := m.evaluateSLLevels(ctx, g, ltp)
	if !slFired {
		m.evaluateTPLevels(ctx, g, ltp)
	}
}

// evaluateSLLevels checks multi-level SL triggers or updates the trailing SL peak.
// Returns true if a multi-level SL fired (single broker SL is handled by broker, not here).
func (m *Manager) evaluateSLLevels(ctx context.Context, g *Group, ltp float64) bool {
	if g.SLMode == SLModeTrailing && g.SingleSLBrokerID != "" {
		m.evaluateTrailingSL(ctx, g, ltp)
		return false
	}
	if g.SLMode != SLModeMultiLevel {
		return false
	}
	// Live multi-level SL: each level is a broker SL-M order — broker handles triggering.
	if g.TradingMode == "LIVE" {
		return false
	}

	for _, level := range g.SLLevels {
		level.mu.Lock()
		status := level.Status
		triggerPrice := level.TriggerPrice
		level.mu.Unlock()

		if status != LevelActive || !g.SLBreached(triggerPrice, ltp) {
			continue
		}
		if !level.tryMarkTriggered(ltp) {
			continue
		}

		qty := atomic.LoadInt32(&level.ExitQty)
		m.logger.Info("ML SL level triggered",
			zap.String("group", g.GroupID.String()),
			zap.Int("level", level.LevelNum),
			zap.Float64("ltp", ltp),
			zap.Float64("trigger", triggerPrice),
			zap.Int32("qty", qty))

		if g.TradingMode == "LIVE" {
			go m.placeExitOrderMarket(ctx, g, level, qty, ltp, models.MLExitTypeSL)
		} else {
			m.recordPaperExit(ctx, g, level, qty, ltp, models.MLExitTypeSL)
		}
		return true
	}
	return false
}

// evaluateTPLevels checks all pending TP levels for both paper and live.
// Live: dispatches MARKET IOC after app confirms LTP crossed the TP trigger.
// Exception: when TPMode is MULTI_LEVEL and trading is LIVE, broker LIMIT orders
// handle triggering — app-level monitoring is skipped entirely.
func (m *Manager) evaluateTPLevels(ctx context.Context, g *Group, ltp float64) {
	// Live multi-level TP: each level is a broker LIMIT order — broker handles triggering.
	if g.TPMode == TPModeMultiLevel && g.TradingMode == "LIVE" {
		return
	}

	for _, level := range g.TPLevels {
		level.mu.Lock()
		status := level.Status
		triggerPrice := level.TriggerPrice
		level.mu.Unlock()

		if status != LevelActive || !g.TPReached(triggerPrice, ltp) {
			continue
		}
		if !level.tryMarkTriggered(ltp) {
			continue
		}

		qty := atomic.LoadInt32(&level.ExitQty)
		m.logger.Info("ML TP level triggered",
			zap.String("group", g.GroupID.String()),
			zap.Int("level", level.LevelNum),
			zap.Float64("ltp", ltp),
			zap.Float64("trigger", triggerPrice),
			zap.Int32("qty", qty))

		if g.TradingMode == "LIVE" {
			// For live: decrement here — broker EXECUTED for these market IOC orders
			// is not routed back through onMultiLevelFilled (only mlLevelIndex orders
			// are), so this is the single point of truth for the qty update.
			g.mu.Lock()
			g.RemainingQty -= qty
			remaining := g.RemainingQty
			if remaining <= 0 {
				g.State = GroupStateCompleted
			}
			g.mu.Unlock()

			go m.placeExitOrderMarket(ctx, g, level, qty, ltp, models.MLExitTypeTP)
			// Re-size the single SL order to match the reduced position.
			if g.SingleSLBrokerID != "" {
				switch g.SLMode {
				case SLModeFixed:
					go m.replaceSLWithReducedQty(ctx, g, remaining)
				case SLModeTrailing:
					go m.replaceSLTrailing(ctx, g, remaining)
				}
			}
			if remaining <= 0 {
				m.finishGroup(ctx, g)
			}
		} else {
			// For paper: recordPaperExit owns the qty decrement, DB records,
			// callbacks, and finishGroupPaper when all levels have exited.
			// Do NOT decrement here — recordPaperExit is the single decrement point.
			m.recordPaperExit(ctx, g, level, qty, ltp, models.MLExitTypeTP)
		}

		return // only one TP fires per tick for strict sequential ordering
	}
}

// evaluateTrailingSL updates the trailing SL high-water mark and issues a
// broker ModifyOrder when price moves favorably past the trailing threshold.
//
// Key ordering: CalcTrailingSL is called BEFORE updating HighestPrice so it
// can compare currentLTP against the previous peak. HighestPrice is only
// advanced after CalcTrailingSL confirms an update is warranted. CurrentSLTrigger
// is updated only after the broker ModifyOrder call succeeds, keeping the
// in-memory state consistent with what is actually live at the broker.
func (m *Manager) evaluateTrailingSL(ctx context.Context, g *Group, ltp float64) {
	g.mu.Lock()

	// Compute before updating HighestPrice — CalcTrailingSL compares ltp to
	// the previous peak, which is only valid while HighestPrice is unchanged.
	newTrigger, shouldUpdate := g.CalcTrailingSL(ltp)

	if !shouldUpdate {
		g.mu.Unlock()
		return
	}

	// Now safe to advance the high-water mark.
	g.HighestPrice = ltp

	brokerOrderID := g.SingleSLBrokerID
	orderID := g.SingleSLOrderID
	stockCode := g.StockCode
	exchange := g.Exchange
	symbol := g.Symbol
	exitSide := g.ExitSide()
	validity := g.Validity
	productType := g.ProductType
	qty := g.RemainingQty
	auth := g.Auth
	g.mu.Unlock()

	go func() {
		modOrder := &models.Order{
			OrderID:       orderID,
			IndiraOrderID: &brokerOrderID,
			StockCode:     stockCode,
			Exchange:      models.Exchange(exchange),
			Symbol:        symbol,
			OrderType:     models.OrderTypeStopLossMarket,
			OrderSide:     models.OrderSide(exitSide),
			Quantity:      qty,
			StopLoss:      &newTrigger,
			Validity:      validity,
			ProductType:   productType,
			UserID:        g.UserID,
			StrategyID:    g.StrategyID,
			StrategyName:  g.StrategyName,
			EventID:       g.EventID,
			BearerToken:   &g.AuthBearer,
			AppId:         &g.AuthAppID,
			Source:        &g.AuthSource,
		}
		if err := m.broker.ModifyOrder(ctx, modOrder, auth); err != nil {
			m.logger.Warn("trailing SL modify failed",
				zap.String("group", g.GroupID.String()),
				zap.Error(err))
			return
		}
		// Update CurrentSLTrigger only after the broker confirms the modify.
		// Advancing it before success would break the next CalcTrailingSL call:
		// a stale CurrentSLTrigger against an advanced HighestPrice produces tiny
		// changePct values that never clear the threshold, stalling the trailing SL.
		g.mu.Lock()
		g.CurrentSLTrigger = newTrigger
		g.mu.Unlock()
		m.logger.Info("trailing SL moved",
			zap.String("group", g.GroupID.String()),
			zap.Float64("new_trigger", newTrigger),
			zap.Float64("highest_price", ltp))
	}()
}

// ══════════════════════════════════════════════════════════════════════════════
// Broker order placement helpers (live only)
// ══════════════════════════════════════════════════════════════════════════════

// placeExitOrderMarket places a MARKET IOC exit order for a triggered level.
func (m *Manager) placeExitOrderMarket(ctx context.Context, g *Group, level *ExitLevelState, qty int32, exitPrice float64, exitType string) {
	order := m.buildExitOrder(g, qty, models.OrderTypeMarket, nil, nil)
	brokerID, err := m.broker.PlaceOrder(ctx, order, g.Auth)
	if err != nil {
		m.logger.Error("ML exit order placement failed",
			zap.String("group", g.GroupID.String()),
			zap.String("exit_type", exitType),
			zap.Int("level", level.LevelNum),
			zap.Error(err))
		return
	}

	_ = m.repo.UpdateMultiLevelLevelStatus(ctx, g.EntryOrderID, exitType, level.LevelNum,
		models.MLStatusTriggered, exitPrice)
	m.logger.Info("ML exit order placed",
		zap.String("group", g.GroupID.String()),
		zap.String("type", exitType),
		zap.Int("level", level.LevelNum),
		zap.String("broker", brokerID))
}

// placeFixedSLOrder places a single SL-M broker order covering the full position qty.
func (m *Manager) placeFixedSLOrder(ctx context.Context, g *Group, qty int32) {
	g.mu.Lock()
	triggerPrice := g.CalcSLTriggerPrice(g.FixedSLPct)
	g.mu.Unlock()

	order := m.buildExitOrder(g, qty, models.OrderTypeStopLossMarket, nil, &triggerPrice)
	brokerID, err := m.broker.PlaceOrder(ctx, order, g.Auth)
	if err != nil {
		m.logger.Error("fixed SL placement failed", zap.String("group", g.GroupID.String()), zap.Error(err))
		return
	}

	exitOrderID := uuid.New()
	g.mu.Lock()
	g.SingleSLBrokerID = brokerID
	g.SingleSLOrderID = exitOrderID
	g.mu.Unlock()

	m.singleSLIndex.Store(brokerID, g)
	m.logger.Info("fixed SL placed",
		zap.String("group", g.GroupID.String()),
		zap.Float64("trigger", triggerPrice),
		zap.String("broker", brokerID))
}

// placeLiveTrailingSL places the initial SL-M broker order at FixedSLPct% from fill.
// FixedSLPct (e.g. 1%) is the initial SL distance; TrailingSLPct (e.g. 0.2%) is the
// trailing increment used later by evaluateTrailingSL — they are separate concerns.
func (m *Manager) placeLiveTrailingSL(ctx context.Context, g *Group) {
	g.mu.Lock()
	triggerPrice := g.CalcSLTriggerPrice(g.FixedSLPct) // initial SL = 1% from fill
	qty := g.RemainingQty
	g.mu.Unlock()

	order := m.buildExitOrder(g, qty, models.OrderTypeStopLossMarket, nil, &triggerPrice)
	brokerID, err := m.broker.PlaceOrder(ctx, order, g.Auth)
	if err != nil {
		m.logger.Error("trailing SL placement failed", zap.String("group", g.GroupID.String()), zap.Error(err))
		return
	}

	exitOrderID := uuid.New()
	g.mu.Lock()
	g.SingleSLBrokerID = brokerID
	g.SingleSLOrderID = exitOrderID
	g.CurrentSLTrigger = triggerPrice // track for subsequent trailing adjustments
	g.mu.Unlock()

	m.singleSLIndex.Store(brokerID, g)
	m.logger.Info("trailing SL placed",
		zap.String("group", g.GroupID.String()),
		zap.Float64("trigger", triggerPrice),
		zap.Float64("initial_sl_pct", g.FixedSLPct),
		zap.String("broker", brokerID))
}

// replaceSLWithReducedQty cancels the current single SL order and places a new
// smaller one after a TP level fires, so the broker SL covers only remaining qty.
func (m *Manager) replaceSLWithReducedQty(ctx context.Context, g *Group, remainingQty int32) {
	if remainingQty <= 0 {
		return
	}

	g.mu.Lock()
	oldBrokerID := g.SingleSLBrokerID
	if oldBrokerID == "" {
		g.mu.Unlock()
		return
	}
	triggerPrice := g.CalcSLTriggerPrice(g.FixedSLPct)
	exchange := g.Exchange
	symbol := g.Symbol
	auth := g.Auth
	g.mu.Unlock()

	m.singleSLIndex.Delete(oldBrokerID)
	if err := m.broker.CancelOrder(ctx, exchange, oldBrokerID, symbol, auth); err != nil {
		m.logger.Warn("replaceSL: cancel failed (continuing)", zap.String("group", g.GroupID.String()), zap.Error(err))
	}

	order := m.buildExitOrder(g, remainingQty, models.OrderTypeStopLossMarket, nil, &triggerPrice)
	newBrokerID, err := m.broker.PlaceOrder(ctx, order, auth)
	if err != nil {
		m.logger.Error("replaceSL: place failed", zap.String("group", g.GroupID.String()), zap.Error(err))
		return
	}

	exitOrderID := uuid.New()
	g.mu.Lock()
	g.SingleSLBrokerID = newBrokerID
	g.SingleSLOrderID = exitOrderID
	g.mu.Unlock()

	m.singleSLIndex.Store(newBrokerID, g)
	m.logger.Info("SL replaced",
		zap.String("group", g.GroupID.String()),
		zap.Int32("qty", remainingQty),
		zap.Float64("trigger", triggerPrice),
		zap.String("broker", newBrokerID))
}

// replaceSLTrailing cancels the current trailing SL broker order and places a new
// SL-M for remainingQty at the current trailing SL trigger price (CurrentSLTrigger).
// Called when a TP level fills and the trailing SL must be re-sized to match the
// reduced position. Falls back to FixedSLPct if CurrentSLTrigger is not yet set.
func (m *Manager) replaceSLTrailing(ctx context.Context, g *Group, remainingQty int32) {
	if remainingQty <= 0 {
		return
	}

	g.mu.Lock()
	oldBrokerID := g.SingleSLBrokerID
	if oldBrokerID == "" {
		g.mu.Unlock()
		return
	}
	triggerPrice := g.CurrentSLTrigger
	if triggerPrice <= 0 {
		triggerPrice = g.CalcSLTriggerPrice(g.FixedSLPct)
	}
	exchange := g.Exchange
	symbol := g.Symbol
	auth := g.Auth
	g.mu.Unlock()

	m.singleSLIndex.Delete(oldBrokerID)
	if err := m.broker.CancelOrder(ctx, exchange, oldBrokerID, symbol, auth); err != nil {
		m.logger.Warn("replaceSLTrailing: cancel old SL failed (continuing with new placement)",
			zap.String("group", g.GroupID.String()),
			zap.String("old_broker_id", oldBrokerID),
			zap.Error(err))
	}

	order := m.buildExitOrder(g, remainingQty, models.OrderTypeStopLossMarket, nil, &triggerPrice)
	newBrokerID, err := m.broker.PlaceOrder(ctx, order, auth)
	if err != nil {
		m.logger.Error("replaceSLTrailing: place new SL failed — position partially unprotected",
			zap.String("group", g.GroupID.String()),
			zap.Int32("remaining_qty", remainingQty),
			zap.Float64("trigger", triggerPrice),
			zap.Error(err))
		return
	}

	exitOrderID := uuid.New()
	g.mu.Lock()
	g.SingleSLBrokerID = newBrokerID
	g.SingleSLOrderID = exitOrderID
	g.CurrentSLTrigger = triggerPrice
	g.mu.Unlock()

	m.singleSLIndex.Store(newBrokerID, g)
	m.logger.Info("trailing SL replaced with reduced qty",
		zap.String("group", g.GroupID.String()),
		zap.Int32("qty", remainingQty),
		zap.Float64("trigger", triggerPrice),
		zap.String("broker", newBrokerID))
}

// onSingleSLFilled handles a broker EXECUTED event for the fixed/trailing SL order.
func (m *Manager) onSingleSLFilled(ctx context.Context, g *Group, fillPrice *float64, filledQty int32) {
	price := 0.0
	if fillPrice != nil {
		price = *fillPrice
	}

	g.mu.Lock()
	g.RemainingQty -= filledQty
	remaining := g.RemainingQty
	if remaining <= 0 {
		g.State = GroupStateCompleted
	}
	// Cancel all pending TP levels.
	// For MULTI_LEVEL TP mode, each level has a live broker LIMIT order that must
	// also be cancelled at the exchange (the earlier comment "no broker cancel needed"
	// only applied to FIXED TP which has no broker order).
	var tpBrokerIDsToCancel []string
	for _, lvl := range g.TPLevels {
		lvl.mu.Lock()
		if lvl.Status == LevelActive || lvl.Status == LevelPending {
			lvl.Status = LevelCancelled
			if g.TPMode == TPModeMultiLevel && lvl.BrokerOrderID != "" {
				tpBrokerIDsToCancel = append(tpBrokerIDsToCancel, lvl.BrokerOrderID)
			}
		}
		lvl.mu.Unlock()
	}
	// Remove from broker index and cancel at exchange outside the level lock.
	for _, bid := range tpBrokerIDsToCancel {
		m.mlLevelIndex.Delete(bid)
		bid := bid
		go func() {
			if err := m.broker.CancelOrder(context.Background(), g.Exchange, bid, g.Symbol, g.Auth); err != nil {
				m.logger.Warn("onSingleSLFilled: cancel TP broker order failed",
					zap.String("group", g.GroupID.String()),
					zap.String("broker_id", bid),
					zap.Error(err))
			}
		}()
	}
	tpLevels := make([]*ExitLevelState, len(g.TPLevels))
	copy(tpLevels, g.TPLevels)
	entryOrderID := g.EntryOrderID
	g.mu.Unlock()

	// Persist TP cancellations to DB
	for _, lvl := range tpLevels {
		lvl.mu.Lock()
		s := lvl.Status
		ln := lvl.LevelNum
		lvl.mu.Unlock()
		if s == LevelCancelled {
			_ = m.repo.UpdateMultiLevelLevelStatus(ctx, entryOrderID, models.MLExitTypeTP, ln, models.MLStatusCancelled, 0)
		}
	}

	// Update SL level status in DB
	_ = m.repo.UpdateMultiLevelLevelStatus(ctx, entryOrderID, models.MLExitTypeSL, 0, models.MLStatusTriggered, price)

	if remaining <= 0 {
		m.finishGroup(ctx, g)
	}

	// Fire paper callbacks for paper groups
	if g.TradingMode == "PAPER" && m.OnPaperLevelTriggered != nil {
		m.OnPaperLevelTriggered(g.UserID, g.EntryOrderID.String(), models.MLExitTypeSL, 0, price, remaining, models.MLExitTypeTP, 0)
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Paper trading helpers
// ══════════════════════════════════════════════════════════════════════════════

// recordPaperExit records a simulated exit for paper trading.
// It is the single point of truth for the qty decrement on paper SL/TP level triggers.
func (m *Manager) recordPaperExit(ctx context.Context, g *Group, level *ExitLevelState, qty int32, exitPrice float64, exitType string) {
	g.mu.Lock()
	g.RemainingQty -= qty
	remaining := g.RemainingQty
	if remaining <= 0 {
		g.State = GroupStateCompleted
	}
	entryOrderID := g.EntryOrderID
	userID := g.UserID
	g.mu.Unlock()

	_ = m.repo.UpdateMultiLevelLevelStatus(ctx, entryOrderID, exitType, level.LevelNum, models.MLStatusTriggered, exitPrice)

	// Create a closed-position row so the partial exit appears in the Closed tab.
	// recordPaperPartialExit writes a new order record (is_square_off_order=true)
	// that the closed-positions query returns alongside full-position exits.
	m.recordPaperPartialExit(ctx, g, qty, exitPrice, exitType, level.LevelNum)

	// Update the entry order's filled_quantity in the DB so the open-positions
	// REST endpoint and on-restart recovery both reflect the remaining qty.
	_ = m.repo.UpdatePaperPositionFilledQty(ctx, entryOrderID, remaining)

	if m.OnPaperQtyUpdated != nil {
		m.OnPaperQtyUpdated(entryOrderID, remaining)
	}

	// Rebalance the opposite-side levels' ExitQty to reflect the reduced position,
	// mirroring what onMultiLevelFilled does for live trading. BrokerOrderID is always
	// empty on paper levels so rebalanceAndModifyLevels skips all broker calls.
	if remaining > 0 {
		switch exitType {
		case models.MLExitTypeTP:
			m.rebalanceAndModifyLevels(ctx, g, g.SLLevels, g.SLLevelConfigs, remaining, nil, models.MLExitTypeSL)
		case models.MLExitTypeSL:
			m.rebalanceAndModifyLevels(ctx, g, g.TPLevels, g.TPLevelConfigs, remaining, nil, models.MLExitTypeTP)
		}
	}

	if m.OnPaperLevelTriggered != nil {
		m.OnPaperLevelTriggered(userID, entryOrderID.String(), exitType, level.LevelNum, exitPrice, remaining, "", 0)
	}

	// Move SL to breakeven/lock-in after TP fires (trailing protection)
	if exitType == models.MLExitTypeTP && m.OnPaperSLMoved != nil {
		newSL := m.computeNewSLAfterTP(g, level, exitPrice)
		if newSL > 0 {
			m.OnPaperSLMoved(entryOrderID, newSL)
		}
	}

	if remaining <= 0 {
		m.finishGroupPaper(ctx, g, exitPrice)
	}
}

// computeNewSLAfterTP computes the new SL stop price after a TP level fires.
// TP L1 → SL moves to entry (breakeven); TP L2+ → SL moves to prev TP trigger price.
func (m *Manager) computeNewSLAfterTP(g *Group, triggeredLevel *ExitLevelState, exitPrice float64) float64 {
	if triggeredLevel.LevelNum == 1 {
		return g.FillPrice // breakeven
	}
	// Find the previous TP level's trigger price as the new SL
	for _, lvl := range g.TPLevels {
		if lvl.LevelNum == triggeredLevel.LevelNum-1 {
			lvl.mu.Lock()
			tp := lvl.TriggerPrice
			lvl.mu.Unlock()
			return tp
		}
	}
	return exitPrice
}

func (m *Manager) finishGroup(ctx context.Context, g *Group) {
	m.groupsByEntry.Delete(g.EntryOrderID)
	m.unsubscribeGroup(g)
	if g.SingleSLBrokerID != "" {
		m.singleSLIndex.Delete(g.SingleSLBrokerID)
	}
}

func (m *Manager) finishGroupPaper(ctx context.Context, g *Group, lastExitPrice float64) {
	m.finishGroup(ctx, g)

	if m.OnPaperGroupCompleted == nil {
		return
	}

	// Compute average exit price and simple PnL
	var totalExitValue float64
	var totalExitQty int32
	for _, lvl := range g.SLLevels {
		lvl.mu.Lock()
		if lvl.Status == LevelTriggered {
			totalExitValue += lvl.ExitPrice * float64(lvl.OriginalExitQty)
			totalExitQty += lvl.OriginalExitQty
		}
		lvl.mu.Unlock()
	}
	for _, lvl := range g.TPLevels {
		lvl.mu.Lock()
		if lvl.Status == LevelTriggered {
			totalExitValue += lvl.ExitPrice * float64(lvl.OriginalExitQty)
			totalExitQty += lvl.OriginalExitQty
		}
		lvl.mu.Unlock()
	}

	avgExitPrice := lastExitPrice
	if totalExitQty > 0 {
		avgExitPrice = totalExitValue / float64(totalExitQty)
	}

	var pnl float64
	if g.OrderSide == "BUY" {
		pnl = (avgExitPrice - g.FillPrice) * float64(g.TotalQty)
	} else {
		pnl = (g.FillPrice - avgExitPrice) * float64(g.TotalQty)
	}

	m.OnPaperGroupCompleted(g.UserID, g.EntryOrderID.String(), pnl, avgExitPrice)
}

// ══════════════════════════════════════════════════════════════════════════════
// DB helpers
// ══════════════════════════════════════════════════════════════════════════════

func (m *Manager) persistExitLevels(ctx context.Context, g *Group) {
	for _, lvl := range g.SLLevels {
		lvl.mu.Lock()
		triggerPrice := lvl.TriggerPrice
		exitQty := lvl.ExitQty
		lvl.mu.Unlock()

		rec := &models.MultiLevelExitRecord{
			EntryOrderID: g.EntryOrderID,
			ExitType:     models.MLExitTypeSL,
			LevelNum:     lvl.LevelNum,
			PricePct:     g.FixedSLPct, // may be 0 for multi-level; individual pct from config
			QtyPct:       0,            // already computed to qty
			TriggerPrice: &triggerPrice,
			ExitQty:      &exitQty,
			Status:       models.MLStatusActive,
		}
		// Recover per-level price_pct from stored configs
		for _, cfg := range g.SLLevelConfigs {
			if cfg.LevelNum == lvl.LevelNum {
				rec.PricePct = cfg.PricePct
				rec.QtyPct = cfg.QtyPct
				break
			}
		}
		if err := m.repo.UpsertMultiLevelExitLevel(ctx, rec); err != nil {
			m.logger.Warn("persist SL level failed", zap.Int("level", lvl.LevelNum), zap.Error(err))
		}
	}

	for _, lvl := range g.TPLevels {
		lvl.mu.Lock()
		triggerPrice := lvl.TriggerPrice
		exitQty := lvl.ExitQty
		lvl.mu.Unlock()

		rec := &models.MultiLevelExitRecord{
			EntryOrderID: g.EntryOrderID,
			ExitType:     models.MLExitTypeTP,
			LevelNum:     lvl.LevelNum,
			TriggerPrice: &triggerPrice,
			ExitQty:      &exitQty,
			Status:       models.MLStatusActive,
		}
		for _, cfg := range g.TPLevelConfigs {
			if cfg.LevelNum == lvl.LevelNum {
				rec.PricePct = cfg.PricePct
				rec.QtyPct = cfg.QtyPct
				break
			}
		}
		if err := m.repo.UpsertMultiLevelExitLevel(ctx, rec); err != nil {
			m.logger.Warn("persist TP level failed", zap.Int("level", lvl.LevelNum), zap.Error(err))
		}
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Order builder
// ══════════════════════════════════════════════════════════════════════════════

func (m *Manager) buildExitOrder(g *Group, qty int32, orderType models.OrderType, limitPrice, stopLoss *float64) *models.Order {
	isPaper := g.TradingMode == "PAPER"
	return &models.Order{
		OrderID:      uuid.New(),
		UserID:       g.UserID,
		StrategyID:   g.StrategyID,
		StrategyName: g.StrategyName,
		EventID:      g.EventID,
		StockCode:    g.StockCode,
		Exchange:     models.Exchange(g.Exchange),
		Symbol:       g.Symbol,
		OrderType:    orderType,
		OrderSide:    models.OrderSide(g.ExitSide()),
		Quantity:     qty,
		Price:        limitPrice,
		StopLoss:     stopLoss,
		Validity:     "IOC",
		ProductType:  "INTRADAY",
		Status:       models.StatusReceived,
		IsPaperTrade: isPaper,
		TradingMode:  g.TradingMode,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		BearerToken:  &g.AuthBearer,
		AppId:        &g.AuthAppID,
		Source:       &g.AuthSource,
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// WSS subscription management
// ══════════════════════════════════════════════════════════════════════════════

func (m *Manager) subscribeGroup(g *Group) {
	shard := m.shardForStock(g.StockCode)
	shard.mu.Lock()
	shard.groups[g.StockCode] = append(shard.groups[g.StockCode], g)
	isFirst := len(shard.groups[g.StockCode]) == 1
	shard.mu.Unlock()

	if isFirst && m.ws != nil {
		m.ws.Subscribe([]string{fmt.Sprintf("%d", g.StockCode)})
	}
}

func (m *Manager) unsubscribeGroup(g *Group) {
	shard := m.shardForStock(g.StockCode)
	shard.mu.Lock()
	list := shard.groups[g.StockCode]
	for i, existing := range list {
		if existing.GroupID == g.GroupID {
			shard.groups[g.StockCode] = append(list[:i], list[i+1:]...)
			break
		}
	}
	isEmpty := len(shard.groups[g.StockCode]) == 0
	shard.mu.Unlock()

	if isEmpty && m.ws != nil {
		m.ws.Unsubscribe([]string{fmt.Sprintf("%d", g.StockCode)})
	}
}

func (m *Manager) shardForStock(stockCode int64) *mlStockShard {
	idx := stockCode % numShards
	if idx < 0 {
		idx = -idx
	}
	return &m.shards[idx]
}

// stockCodeFromKey parses the numeric stock code from a WSS tick key ("exchange:token").
// e.g. "nse:2475" → 2475
func stockCodeFromKey(key string) int64 {
	idx := strings.LastIndex(key, ":")
	if idx < 0 || idx == len(key)-1 {
		return 0
	}
	var code int64
	for _, c := range key[idx+1:] {
		if c < '0' || c > '9' {
			return 0
		}
		code = code*10 + int64(c-'0')
	}
	return code
}

// ══════════════════════════════════════════════════════════════════════════════
// Monitoring / diagnostics
// ══════════════════════════════════════════════════════════════════════════════

// GroupCount returns the total number of subscribed (post-fill) groups.
func (m *Manager) GroupCount() int {
	total := 0
	for i := range m.shards {
		m.shards[i].mu.RLock()
		for _, groups := range m.shards[i].groups {
			total += len(groups)
		}
		m.shards[i].mu.RUnlock()
	}
	return total
}

// ══════════════════════════════════════════════════════════════════════════════
// Utilities
// ══════════════════════════════════════════════════════════════════════════════

func isFilledStatus(s string) bool {
	switch strings.ToUpper(s) {
	case "FILLED", "EXECUTED", "TRADED":
		return true
	}
	return false
}

// ══════════════════════════════════════════════════════════════════════════════
// Live multi-level broker order management
// ══════════════════════════════════════════════════════════════════════════════

// placeMultiSLOrders places one broker SL-M order per SL level for live groups.
// All placements run concurrently; broker IDs are stored in mlLevelIndex for WS routing.
func (m *Manager) placeMultiSLOrders(ctx context.Context, g *Group) {
	g.mu.Lock()
	levels := make([]*ExitLevelState, len(g.SLLevels))
	copy(levels, g.SLLevels)
	auth := g.Auth
	g.mu.Unlock()

	var wg sync.WaitGroup
	for _, lvl := range levels {
		wg.Add(1)
		go func(level *ExitLevelState) {
			defer wg.Done()
			level.mu.Lock()
			trigger := level.TriggerPrice
			qty := level.ExitQty
			levelNum := level.LevelNum
			orderID := level.ExitOrderID
			level.mu.Unlock()

			order := m.buildMultiSLOrder(g, qty, trigger, orderID)
			brokerID, err := m.broker.PlaceOrder(ctx, order, auth)
			if err != nil {
				m.logger.Error("multi SL level placement failed",
					zap.String("group", g.GroupID.String()),
					zap.Int("level", levelNum),
					zap.Error(err))
				return
			}

			level.mu.Lock()
			level.BrokerOrderID = brokerID
			level.mu.Unlock()

			m.mlLevelIndex.Store(brokerID, &mlLevelRef{
				group: g, exitType: models.MLExitTypeSL, levelNum: levelNum,
			})
			m.logger.Info("multi SL level placed",
				zap.String("group", g.GroupID.String()),
				zap.Int("level", levelNum),
				zap.Float64("trigger", trigger),
				zap.Int32("qty", qty),
				zap.String("broker", brokerID))
		}(lvl)
	}
	wg.Wait()
}

// placeMultiTPOrders places one broker LIMIT order per TP level for live groups.
// TriggerPrice on each ExitLevelState holds the absolute limit price (set by CalcTPLimitPrice).
func (m *Manager) placeMultiTPOrders(ctx context.Context, g *Group) {
	g.mu.Lock()
	levels := make([]*ExitLevelState, len(g.TPLevels))
	copy(levels, g.TPLevels)
	auth := g.Auth
	g.mu.Unlock()

	var wg sync.WaitGroup
	for _, lvl := range levels {
		wg.Add(1)
		go func(level *ExitLevelState) {
			defer wg.Done()
			level.mu.Lock()
			limitPrice := level.TriggerPrice // CalcTPLimitPrice stored here
			qty := level.ExitQty
			levelNum := level.LevelNum
			orderID := level.ExitOrderID
			level.mu.Unlock()

			order := m.buildMultiTPOrder(g, qty, limitPrice, orderID)
			brokerID, err := m.broker.PlaceOrder(ctx, order, auth)
			if err != nil {
				m.logger.Error("multi TP level placement failed",
					zap.String("group", g.GroupID.String()),
					zap.Int("level", levelNum),
					zap.Error(err))
				return
			}

			level.mu.Lock()
			level.BrokerOrderID = brokerID
			level.mu.Unlock()

			m.mlLevelIndex.Store(brokerID, &mlLevelRef{
				group: g, exitType: models.MLExitTypeTP, levelNum: levelNum,
			})
			m.logger.Info("multi TP level placed",
				zap.String("group", g.GroupID.String()),
				zap.Int("level", levelNum),
				zap.Float64("limit", limitPrice),
				zap.Int32("qty", qty),
				zap.String("broker", brokerID))
		}(lvl)
	}
	wg.Wait()
}

// onMultiLevelFilled handles a broker EXECUTED event for one per-level SL or TP order.
// Marks the level triggered, decrements RemainingQty, then proportionally rebalances
// all remaining active levels on BOTH sides and issues ModifyOrder for each.
func (m *Manager) onMultiLevelFilled(ctx context.Context, g *Group, exitType string, levelNum int, fillPrice *float64, filledQty int32) {
	price := 0.0
	if fillPrice != nil {
		price = *fillPrice
	}

	g.mu.Lock()

	// Resolve same-side and other-side slices and configs.
	var sameSideLevels, otherSideLevels []*ExitLevelState
	var sameSideConfigs, otherSideConfigs []SLTPLevelConfig
	var sameSideExitType, otherSideExitType string
	if exitType == models.MLExitTypeSL {
		sameSideLevels = g.SLLevels
		otherSideLevels = g.TPLevels
		sameSideConfigs = g.SLLevelConfigs
		otherSideConfigs = g.TPLevelConfigs
		sameSideExitType = models.MLExitTypeSL
		otherSideExitType = models.MLExitTypeTP
	} else {
		sameSideLevels = g.TPLevels
		otherSideLevels = g.SLLevels
		sameSideConfigs = g.TPLevelConfigs
		otherSideConfigs = g.SLLevelConfigs
		sameSideExitType = models.MLExitTypeTP
		otherSideExitType = models.MLExitTypeSL
	}

	// Locate the triggered level.
	var triggeredLevel *ExitLevelState
	for _, lvl := range sameSideLevels {
		if lvl.LevelNum == levelNum {
			triggeredLevel = lvl
			break
		}
	}
	if triggeredLevel == nil {
		g.mu.Unlock()
		m.logger.Warn("onMultiLevelFilled: level not found",
			zap.String("group", g.GroupID.String()),
			zap.String("exit_type", exitType),
			zap.Int("level_num", levelNum))
		return
	}

	// Idempotency: skip if already processed.
	triggeredLevel.mu.Lock()
	if triggeredLevel.Status != LevelActive {
		triggeredLevel.mu.Unlock()
		g.mu.Unlock()
		return
	}
	exitedQty := triggeredLevel.ExitQty
	now := time.Now()
	triggeredLevel.Status = LevelTriggered
	triggeredLevel.TriggeredAt = &now
	triggeredLevel.ExitPrice = price
	triggeredBrokerID := triggeredLevel.BrokerOrderID
	triggeredLevel.mu.Unlock()

	g.RemainingQty -= exitedQty
	remaining := g.RemainingQty
	if remaining <= 0 {
		g.State = GroupStateCompleted
	}

	// Snapshot configs for rebalancing while holding g.mu.
	sameConfs := make([]SLTPLevelConfig, len(sameSideConfigs))
	copy(sameConfs, sameSideConfigs)
	otherConfs := make([]SLTPLevelConfig, len(otherSideConfigs))
	copy(otherConfs, otherSideConfigs)
	auth := g.Auth
	entryOrderID := g.EntryOrderID
	g.mu.Unlock()

	// Remove triggered level from broker index.
	m.mlLevelIndex.Delete(triggeredBrokerID)

	// Persist triggered status.
	_ = m.repo.UpdateMultiLevelLevelStatus(ctx, entryOrderID, exitType, levelNum, models.MLStatusTriggered, price)

	if remaining <= 0 {
		// All qty exited — cancel all remaining broker orders on both sides.
		m.cancelRemainingMLOrders(ctx, g, sameSideLevels, auth)
		m.cancelRemainingMLOrders(ctx, g, otherSideLevels, auth)
		// For FIXED/TRAILING SL mode the SL is a single broker order tracked separately
		// from the SL levels slice. Cancel it so no orphaned SL sits at the exchange.
		g.mu.Lock()
		slBrokerID := g.SingleSLBrokerID
		g.SingleSLBrokerID = ""
		g.mu.Unlock()
		if slBrokerID != "" {
			m.singleSLIndex.Delete(slBrokerID)
			go func() {
				if err := m.broker.CancelOrder(ctx, g.Exchange, slBrokerID, g.Symbol, g.Auth); err != nil {
					m.logger.Warn("onMultiLevelFilled: cancel single SL failed",
						zap.String("group", g.GroupID.String()),
						zap.String("broker_id", slBrokerID),
						zap.Error(err))
				}
			}()
		}
		m.finishGroup(ctx, g)
		return
	}

	// Rebalance both sides from new remaining qty and modify broker orders.
	m.rebalanceAndModifyLevels(ctx, g, sameSideLevels, sameConfs, remaining, auth, sameSideExitType)
	m.rebalanceAndModifyLevels(ctx, g, otherSideLevels, otherConfs, remaining, auth, otherSideExitType)
}

// rebalanceAndModifyLevels redistributes remainingQty across all active levels
// in proportion to their configured QtyPct, then calls ModifyOrder for each level
// whose quantity changed.
func (m *Manager) rebalanceAndModifyLevels(ctx context.Context, g *Group, levels []*ExitLevelState, configs []SLTPLevelConfig, remainingQty int32, auth *indiraClient.AuthContext, exitType string) {
	type item struct {
		level  *ExitLevelState
		qtyPct float64
	}
	var active []item
	totalPct := 0.0

	for _, lvl := range levels {
		lvl.mu.Lock()
		status := lvl.Status
		levelNum := lvl.LevelNum
		lvl.mu.Unlock()
		if status != LevelActive {
			continue
		}
		qtyPct := 0.0
		for _, cfg := range configs {
			if cfg.LevelNum == levelNum {
				qtyPct = cfg.QtyPct
				break
			}
		}
		if qtyPct <= 0 {
			continue
		}
		active = append(active, item{level: lvl, qtyPct: qtyPct})
		totalPct += qtyPct
	}

	if len(active) == 0 || totalPct <= 0 {
		return
	}

	pcts := make([]float64, len(active))
	for i, a := range active {
		pcts[i] = a.qtyPct
	}
	newQtys := computeRebalancedQtys(pcts, totalPct, remainingQty)

	for i, a := range active {
		newQty := newQtys[i]
		a.level.mu.Lock()
		oldQty := a.level.ExitQty
		a.level.ExitQty = newQty
		brokerID := a.level.BrokerOrderID
		orderID := a.level.ExitOrderID
		triggerPrice := a.level.TriggerPrice
		levelNum := a.level.LevelNum
		a.level.mu.Unlock()

		if brokerID == "" || newQty == oldQty {
			continue
		}

		go m.modifyLevelOrder(ctx, g, orderID, brokerID, newQty, triggerPrice, auth, exitType, levelNum)
	}
}

// computeRebalancedQtys distributes remainingQty across activePcts proportionally.
// Rounding: frac ≥ 0.5 → ceil, else floor. Corrects drift so the sum always
// equals remainingQty exactly (adjusts the levels with fractions closest to 0.5).
func computeRebalancedQtys(activePcts []float64, totalPct float64, remainingQty int32) []int32 {
	n := len(activePcts)
	if n == 0 {
		return nil
	}

	rounded := make([]int32, n)
	fracs := make([]float64, n)
	roundedUp := make([]bool, n)

	for i, pct := range activePcts {
		raw := float64(remainingQty) * pct / totalPct
		fl := math.Floor(raw)
		frac := raw - fl
		fracs[i] = frac
		if frac >= 0.5 {
			rounded[i] = int32(fl) + 1
			roundedUp[i] = true
		} else {
			rounded[i] = int32(fl)
		}
	}

	// Correct integer drift so sum == remainingQty.
	var sum int32
	for _, q := range rounded {
		sum += q
	}
	diff := remainingQty - sum

	type cand struct {
		idx  int
		frac float64
	}
	if diff > 0 {
		// Give extra shares to floored levels with the largest fractional parts.
		var cands []cand
		for i := range rounded {
			if !roundedUp[i] {
				cands = append(cands, cand{i, fracs[i]})
			}
		}
		sort.Slice(cands, func(a, b int) bool { return cands[a].frac > cands[b].frac })
		for i := 0; i < int(diff) && i < len(cands); i++ {
			rounded[cands[i].idx]++
		}
	} else if diff < 0 {
		// Remove shares from rounded-up levels with the smallest fractional parts.
		var cands []cand
		for i := range rounded {
			if roundedUp[i] {
				cands = append(cands, cand{i, fracs[i]})
			}
		}
		sort.Slice(cands, func(a, b int) bool { return cands[a].frac < cands[b].frac })
		for i := 0; i < -int(diff) && i < len(cands); i++ {
			rounded[cands[i].idx]--
		}
	}

	return rounded
}

// computeInitialExitQtys distributes filledQty across configs in slice order.
// Each non-last level gets floor(filledQty * QtyPct / 100); the last level
// absorbs whatever remainder is left so Σ result == filledQty exactly, even
// when individual percentages don't divide filledQty cleanly. Caller must pass
// configs sorted by LevelNum ascending so the remainder lands on the highest
// level deterministically.
//
// Misconfiguration handling:
//   - If cumulative assignment would push the last level below zero (Σ QtyPct
//     for non-last levels > 100), early levels are clamped so the last level
//     receives 0 and a warn log is emitted.
//   - If Σ QtyPct < 100, the last level still gets the full remainder (caller's
//     contract is "all of filledQty must exit across these levels").
//
// logger/exitType/entryOrderID are only used for the misconfig warn log and may
// be nil/empty in tests.
func computeInitialExitQtys(configs []SLTPLevelConfig, filledQty int32, logger *zap.Logger, exitType string, entryOrderID uuid.UUID) []int32 {
	n := len(configs)
	if n == 0 {
		return nil
	}
	qtys := make([]int32, n)
	if filledQty <= 0 {
		return qtys
	}
	var assigned int32
	clamped := false
	for i := 0; i < n-1; i++ {
		q := int32(float64(filledQty) * configs[i].QtyPct / 100)
		// Clamp so we never overshoot filledQty (would otherwise make last level negative).
		if assigned+q > filledQty {
			q = filledQty - assigned
			if q < 0 {
				q = 0
			}
			clamped = true
		}
		qtys[i] = q
		assigned += q
	}
	qtys[n-1] = filledQty - assigned // last level absorbs the remainder
	if clamped && logger != nil {
		logger.Warn("ml_qty_pct_overflow",
			zap.String("entry_order_id", entryOrderID.String()),
			zap.String("exit_type", exitType),
			zap.Int32("filled_qty", filledQty),
			zap.Any("configs", configs),
		)
	}
	return qtys
}

// modifyLevelOrder issues a broker ModifyOrder for a single exit level with updated qty.
// SL levels use SL-M (trigger only); TP levels use LIMIT (price only).
func (m *Manager) modifyLevelOrder(ctx context.Context, g *Group, orderID uuid.UUID, brokerID string, qty int32, triggerPrice float64, auth *indiraClient.AuthContext, exitType string, levelNum int) {
	var orderType models.OrderType
	var limitPrice *float64
	var stopLoss *float64

	if exitType == models.MLExitTypeSL {
		orderType = models.OrderTypeStopLossMarket
		stopLoss = &triggerPrice
	} else {
		orderType = models.OrderTypeLimit
		limitPrice = &triggerPrice
	}

	modOrder := &models.Order{
		OrderID:       orderID,
		IndiraOrderID: &brokerID,
		StockCode:     g.StockCode,
		Exchange:      models.Exchange(g.Exchange),
		Symbol:        g.Symbol,
		OrderType:     orderType,
		OrderSide:     models.OrderSide(g.ExitSide()),
		Quantity:      qty,
		Price:         limitPrice,
		StopLoss:      stopLoss,
		Validity:      g.Validity,
		ProductType:   g.ProductType,
		UserID:        g.UserID,
		StrategyID:    g.StrategyID,
		StrategyName:  g.StrategyName,
		EventID:       g.EventID,
		BearerToken:   &g.AuthBearer,
		AppId:         &g.AuthAppID,
		Source:        &g.AuthSource,
	}

	if err := m.broker.ModifyOrder(ctx, modOrder, auth); err != nil {
		m.logger.Warn("ML level modify failed",
			zap.String("group", g.GroupID.String()),
			zap.String("exit_type", exitType),
			zap.Int("level", levelNum),
			zap.Int32("new_qty", qty),
			zap.Error(err))
		return
	}

	m.logger.Info("ML level qty rebalanced",
		zap.String("group", g.GroupID.String()),
		zap.String("exit_type", exitType),
		zap.Int("level", levelNum),
		zap.Int32("new_qty", qty))
}

// cancelRemainingMLOrders cancels all active broker orders in the given level slice.
// Used when the position is fully closed or the group is force-cancelled.
func (m *Manager) cancelRemainingMLOrders(ctx context.Context, g *Group, levels []*ExitLevelState, auth *indiraClient.AuthContext) {
	for _, lvl := range levels {
		lvl.mu.Lock()
		if lvl.Status != LevelActive {
			lvl.mu.Unlock()
			continue
		}
		brokerID := lvl.BrokerOrderID
		levelNum := lvl.LevelNum
		lvl.Status = LevelCancelled
		lvl.mu.Unlock()

		if brokerID == "" {
			continue
		}
		m.mlLevelIndex.Delete(brokerID)

		go func(bid string, ln int) {
			if err := m.broker.CancelOrder(ctx, g.Exchange, bid, g.Symbol, auth); err != nil {
				m.logger.Warn("cancel ML level order failed",
					zap.String("group", g.GroupID.String()),
					zap.Int("level", ln),
					zap.Error(err))
			}
		}(brokerID, levelNum)
	}
}

// buildMultiSLOrder builds a stop-loss-market order for one multi-level SL placement.
func (m *Manager) buildMultiSLOrder(g *Group, qty int32, triggerPrice float64, orderID uuid.UUID) *models.Order {
	return &models.Order{
		OrderID:      orderID,
		UserID:       g.UserID,
		StrategyID:   g.StrategyID,
		StrategyName: g.StrategyName,
		EventID:      g.EventID,
		StockCode:    g.StockCode,
		Exchange:     models.Exchange(g.Exchange),
		Symbol:       g.Symbol,
		OrderType:    models.OrderTypeStopLossMarket,
		OrderSide:    models.OrderSide(g.ExitSide()),
		Quantity:     qty,
		StopLoss:     &triggerPrice,
		Validity:     g.Validity,
		ProductType:  g.ProductType,
		Status:       models.StatusReceived,
		TradingMode:  g.TradingMode,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		BearerToken:  &g.AuthBearer,
		AppId:        &g.AuthAppID,
		Source:       &g.AuthSource,
	}
}

// buildMultiTPOrder builds a limit exit order for one multi-level TP placement.
func (m *Manager) buildMultiTPOrder(g *Group, qty int32, limitPrice float64, orderID uuid.UUID) *models.Order {
	return &models.Order{
		OrderID:      orderID,
		UserID:       g.UserID,
		StrategyID:   g.StrategyID,
		StrategyName: g.StrategyName,
		EventID:      g.EventID,
		StockCode:    g.StockCode,
		Exchange:     models.Exchange(g.Exchange),
		Symbol:       g.Symbol,
		OrderType:    models.OrderTypeLimit,
		OrderSide:    models.OrderSide(g.ExitSide()),
		Quantity:     qty,
		Price:        &limitPrice,
		Validity:     g.Validity,
		ProductType:  g.ProductType,
		Status:       models.StatusReceived,
		TradingMode:  g.TradingMode,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		BearerToken:  &g.AuthBearer,
		AppId:        &g.AuthAppID,
		Source:       &g.AuthSource,
	}
}

// suppress unused import if logger is the only zap user
var _ = log.Printf
