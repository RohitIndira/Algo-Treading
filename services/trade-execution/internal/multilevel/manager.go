package multilevel

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// PriceLookup is satisfied by *paper.RedisPriceClient.
type PriceLookup interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

// BrokerOrderPlacer places, modifies, and cancels broker orders.
// Satisfied by *indira.ExecutionClient.
type BrokerOrderPlacer interface {
	PlaceOrder(ctx context.Context, order *models.Order, auth *indiraClient.AuthContext) (string, error)
	CancelOrder(ctx context.Context, exchange, orderID, symbol string, auth *indiraClient.AuthContext) error
	ModifyOrder(ctx context.Context, order *models.Order, auth *indiraClient.AuthContext) error
}

// Manager orchestrates multi-level SL and TP exits.
//
// Thread safety: Groups are stored in a sync.Map. Per-group mutations are
// serialised by the Group.mu lock. The broker-ID index (byBrokerID) uses a
// separate sync.Map for O(1) WS-event routing.
type Manager struct {
	groups     sync.Map // entryOrderID (uuid.UUID) → *Group
	byBrokerID sync.Map // brokerOrderID (string)   → *Group (for TP limit order fills)

	repo        repository.OrderRepository
	priceLookup PriceLookup
	broker      BrokerOrderPlacer
	logger      *zap.Logger

	pollInterval time.Duration // how often to check SL/paper-TP levels (default: 1s)
}

// NewManager creates a Manager.
//
//	broker may be nil for paper-only deployments.
//	priceLookup may be nil (levels will not trigger in paper mode without it).
func NewManager(
	repo repository.OrderRepository,
	priceLookup PriceLookup,
	broker BrokerOrderPlacer,
	logger *zap.Logger,
) *Manager {
	return &Manager{
		repo:         repo,
		priceLookup:  priceLookup,
		broker:       broker,
		logger:       logger,
		pollInterval: 1 * time.Second,
	}
}

// ── Registration ─────────────────────────────────────────────────────────────

// RegisterEntry registers an entry order with multi-level exit config.
// Called by the signal processor after the entry order is placed (before fill).
// For paper orders, OnEntryFill must be called immediately after this.
func (m *Manager) RegisterEntry(
	entryOrder *models.Order,
	slMode string,
	tpMode string,
	slLevels []models.MultiLevelExitLevel,
	tpLevels []models.MultiLevelExitLevel,
	fixedSLPct float64,
	trailingSLPct float64,
	auth *indiraClient.AuthContext,
) {
	group := &Group{
		GroupID:       uuid.New(),
		EntryOrderID:  entryOrder.OrderID,
		UserID:        entryOrder.UserID,
		Symbol:        entryOrder.Symbol,
		Exchange:      string(entryOrder.Exchange),
		StockCode:     entryOrder.StockCode,
		OrderSide:     string(entryOrder.OrderSide),
		ProductType:   entryOrder.ProductType,
		Validity:      entryOrder.Validity,
		TotalQty:      entryOrder.Quantity,
		RemainingQty:  entryOrder.Quantity,
		SLMode:        slMode,
		FixedSLPct:    fixedSLPct,
		TrailingSLPct: trailingSLPct,
		TPMode:        tpMode,
		TradingMode:   entryOrder.TradingMode,
		Auth:          auth,
		broker:        m.broker,
		StrategyID:    entryOrder.StrategyID,
		StrategyName:  entryOrder.StrategyName,
		EventID:       entryOrder.EventID,
		State:         GroupStateActive,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	// Store immutable configs for use when live entry fills (WS event)
	for _, l := range slLevels {
		group.SLLevelConfigs = append(group.SLLevelConfigs, SLTPLevelConfig{l.LevelNum, l.PricePct, l.QtyPct})
		group.SLLevels = append(group.SLLevels, &ExitLevelState{LevelNum: l.LevelNum, Status: LevelPending})
	}
	for _, l := range tpLevels {
		group.TPLevelConfigs = append(group.TPLevelConfigs, SLTPLevelConfig{l.LevelNum, l.PricePct, l.QtyPct})
		group.TPLevels = append(group.TPLevels, &ExitLevelState{LevelNum: l.LevelNum, Status: LevelPending})
	}

	m.groups.Store(entryOrder.OrderID, group)

	m.logger.Info("ml_group_registered",
		zap.String("group_id", group.GroupID.String()),
		zap.String("entry_order_id", entryOrder.OrderID.String()),
		zap.String("symbol", entryOrder.Symbol),
		zap.String("sl_mode", slMode),
		zap.String("tp_mode", tpMode),
		zap.Int("sl_levels", len(slLevels)),
		zap.Int("tp_levels", len(tpLevels)))
}

// IsRegistered returns true if the entry order is tracked by this manager.
func (m *Manager) IsRegistered(entryOrderID uuid.UUID) bool {
	_, ok := m.groups.Load(entryOrderID)
	return ok
}

// ── Entry Fill ───────────────────────────────────────────────────────────────

// OnEntryFill is called when the entry order fills (from paper executor or broker WS).
// It computes trigger prices, persists level rows, places live TP limit orders,
// and starts the SL/paper-TP price monitor goroutine.
func (m *Manager) OnEntryFill(
	ctx context.Context,
	entryOrderID uuid.UUID,
	fillPrice float64,
	filledQty int32,
	slLevels []models.MultiLevelExitLevel,
	tpLevels []models.MultiLevelExitLevel,
) {
	v, ok := m.groups.Load(entryOrderID)
	if !ok {
		m.logger.Warn("ml_entry_fill_no_group", zap.String("entry_order_id", entryOrderID.String()))
		return
	}
	group := v.(*Group)

	group.mu.Lock()
	if group.State != GroupStateActive {
		group.mu.Unlock()
		return
	}
	group.FillPrice = fillPrice
	group.TotalQty = filledQty
	group.RemainingQty = filledQty
	group.entryFilled = true
	if group.SLMode == SLModeTrailing {
		group.HighestPrice = fillPrice
	}
	group.UpdatedAt = time.Now()
	group.mu.Unlock()

	m.logger.Info("ml_entry_filled",
		zap.String("group_id", group.GroupID.String()),
		zap.Float64("fill_price", fillPrice),
		zap.Int32("qty", filledQty))

	// Compute and store level trigger prices
	m.computeAndPersistLevels(ctx, group, slLevels, tpLevels, filledQty, fillPrice)

	// Place live TP limit orders (for MULTI_LEVEL TP + live trading)
	if group.TPMode == TPModeMultiLevel && group.TradingMode == "LIVE" && group.broker != nil {
		m.placeLiveTPOrders(ctx, group)
	}

	// Place fixed SL order for full qty (for FIXED SL + multi-level TP)
	if group.SLMode == SLModeFixed && group.TradingMode == "LIVE" && group.broker != nil {
		m.placeLiveFixedSL(ctx, group)
	}

	// Start price monitor for:
	//   - SL levels (any mode) in live trading
	//   - All levels in paper trading
	monCtx, cancel := context.WithCancel(context.Background())
	group.mu.Lock()
	group.cancelMonitor = cancel
	group.mu.Unlock()

	go m.priceMonitorLoop(monCtx, group)
}

// ── Level Computation ─────────────────────────────────────────────────────────

func (m *Manager) computeAndPersistLevels(
	ctx context.Context,
	group *Group,
	slLevels, tpLevels []models.MultiLevelExitLevel,
	totalQty int32,
	fillPrice float64,
) {
	// SL levels
	for i, cfg := range slLevels {
		if i >= len(group.SLLevels) {
			break
		}
		trigger := group.CalcSLTriggerPrice(cfg.PricePct)
		exitQty := int32(float64(totalQty) * cfg.QtyPct / 100.0)
		if i == len(slLevels)-1 {
			// Last level gets all remaining qty to avoid rounding loss
			exitQty = totalQty - m.sumQty(group.SLLevels[:i])
		}

		state := group.SLLevels[i]
		state.mu.Lock()
		state.TriggerPrice = trigger
		state.ExitQty = exitQty
		state.Status = LevelActive
		state.mu.Unlock()

		m.persistLevel(ctx, group.EntryOrderID, models.MLExitTypeSL, cfg, trigger, exitQty)
	}

	// TP levels
	for i, cfg := range tpLevels {
		if i >= len(group.TPLevels) {
			break
		}
		limit := group.CalcTPLimitPrice(cfg.PricePct)
		exitQty := int32(float64(totalQty) * cfg.QtyPct / 100.0)
		if i == len(tpLevels)-1 {
			exitQty = totalQty - m.sumQty(group.TPLevels[:i])
		}

		state := group.TPLevels[i]
		state.mu.Lock()
		state.TriggerPrice = limit
		state.ExitQty = exitQty
		state.Status = LevelActive
		state.mu.Unlock()

		m.persistLevel(ctx, group.EntryOrderID, models.MLExitTypeTP, cfg, limit, exitQty)
	}
}

func (m *Manager) sumQty(levels []*ExitLevelState) int32 {
	var total int32
	for _, l := range levels {
		total += l.ExitQty
	}
	return total
}

func (m *Manager) persistLevel(
	ctx context.Context,
	entryOrderID uuid.UUID,
	exitType string,
	cfg models.MultiLevelExitLevel,
	triggerPrice float64,
	exitQty int32,
) {
	if err := m.repo.UpsertMultiLevelExitLevel(ctx, &models.MultiLevelExitRecord{
		EntryOrderID: entryOrderID,
		ExitType:     exitType,
		LevelNum:     cfg.LevelNum,
		PricePct:     cfg.PricePct,
		QtyPct:       cfg.QtyPct,
		TriggerPrice: &triggerPrice,
		ExitQty:      &exitQty,
		Status:       models.MLStatusActive,
	}); err != nil {
		m.logger.Error("ml_persist_level_failed",
			zap.String("entry_order_id", entryOrderID.String()),
			zap.String("exit_type", exitType),
			zap.Int("level", cfg.LevelNum),
			zap.Error(err))
	}
}

// ── Live TP Order Placement ───────────────────────────────────────────────────

func (m *Manager) placeLiveTPOrders(ctx context.Context, group *Group) {
	if group.Auth == nil {
		m.logger.Warn("ml_place_tp_no_auth", zap.String("group_id", group.GroupID.String()))
		return
	}

	for _, level := range group.TPLevels {
		level.mu.Lock()
		if level.Status != LevelActive {
			level.mu.Unlock()
			continue
		}
		limitPrice := level.TriggerPrice
		exitQty := level.ExitQty
		levelNum := level.LevelNum
		level.mu.Unlock()

		exitOrderID := uuid.New()
		exitOrder := m.buildExitOrder(exitOrderID, group, exitQty, limitPrice, "LIMIT", 0, "TP")

		brokerID, err := group.broker.PlaceOrder(ctx, exitOrder, group.Auth)
		if err != nil {
			m.logger.Error("ml_place_tp_order_failed",
				zap.String("group_id", group.GroupID.String()),
				zap.Int("level", levelNum),
				zap.Float64("price", limitPrice),
				zap.Error(err))
			continue
		}

		exitOrder.IndiraOrderID = &brokerID
		level.markActive(brokerID, exitOrderID)
		m.byBrokerID.Store(brokerID, group)

		m.logger.Info("ml_tp_order_placed",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("level", levelNum),
			zap.Float64("price", limitPrice),
			zap.Int32("qty", exitQty),
			zap.String("broker_id", brokerID))

		if err := m.repo.UpdateMultiLevelLevelBrokerID(ctx, group.EntryOrderID, models.MLExitTypeTP, levelNum, brokerID, exitOrderID); err != nil {
			m.logger.Error("ml_persist_broker_id_failed", zap.Error(err))
		}
	}
}

// ── Live Fixed SL Placement ───────────────────────────────────────────────────

func (m *Manager) placeLiveFixedSL(ctx context.Context, group *Group) {
	if group.Auth == nil || group.FixedSLPct <= 0 {
		return
	}

	slTrigger := group.CalcSLTriggerPrice(group.FixedSLPct)
	slLimit := slTrigger
	if group.OrderSide == "BUY" {
		slLimit = roundNSE(slTrigger * 0.995)
	} else {
		slLimit = roundNSE(slTrigger * 1.005)
	}

	exitOrderID := uuid.New()
	exitOrder := m.buildExitOrder(exitOrderID, group, group.TotalQty, slLimit, "SL-M", slTrigger, "SL")

	brokerID, err := group.broker.PlaceOrder(ctx, exitOrder, group.Auth)
	if err != nil {
		m.logger.Error("ml_place_fixed_sl_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Float64("trigger", slTrigger),
			zap.Error(err))
		return
	}

	group.mu.Lock()
	group.SingleSLBrokerID = brokerID
	group.SingleSLOrderID = exitOrderID
	group.mu.Unlock()

	m.logger.Info("ml_fixed_sl_placed",
		zap.String("group_id", group.GroupID.String()),
		zap.Float64("trigger", slTrigger),
		zap.Int32("qty", group.TotalQty),
		zap.String("broker_id", brokerID))
}

// ── Broker WS Event Handler ───────────────────────────────────────────────────

// HandleBrokerUpdate is called by the status service on every broker WS event.
// Implements statusservice.MLHandler.
//
// Three cases handled:
//  1. Entry order fills → call OnEntryFill (live path)
//  2. TP limit order fills → onTPLevelFilled
//  3. Single fixed/trailing SL order fills → onFixedSLFilled
func (m *Manager) HandleBrokerUpdate(ctx context.Context, order *models.Order, brokerStatus string) {
	if order == nil || order.IndiraOrderID == nil {
		return
	}
	brokerID := *order.IndiraOrderID

	// ── Case 1: Entry order fill (live orders only) ──────────────────────
	// Check if this order is a registered entry that hasn't been filled yet.
	if models.IsFilledStatus(models.OrderStatus(brokerStatus)) {
		if v, ok := m.groups.Load(order.OrderID); ok {
			group := v.(*Group)
			group.mu.Lock()
			alreadyFilled := group.entryFilled
			group.mu.Unlock()

			if !alreadyFilled {
				group.mu.Lock()
				group.entryFilled = true
				group.mu.Unlock()

				fillPrice := 0.0
				if order.FilledPrice != nil {
					fillPrice = *order.FilledPrice
				} else if order.Price != nil {
					fillPrice = *order.Price
				}
				filledQty := order.FilledQuantity
				if filledQty == 0 {
					filledQty = order.Quantity
				}

				// Convert stored configs back to models.MultiLevelExitLevel
				slLevels := configToMLLevels(group.SLLevelConfigs)
				tpLevels := configToMLLevels(group.TPLevelConfigs)

				m.OnEntryFill(ctx, order.OrderID, fillPrice, filledQty, slLevels, tpLevels)
				return
			}
		}
	}

	// ── Case 2: TP limit order fill ───────────────────────────────────────
	v, ok := m.byBrokerID.Load(brokerID)
	if ok {
		group := v.(*Group)
		if !models.IsFilledStatus(models.OrderStatus(brokerStatus)) {
			return
		}
		fillPrice := 0.0
		if order.FilledPrice != nil {
			fillPrice = *order.FilledPrice
		}
		for _, level := range group.TPLevels {
			level.mu.Lock()
			if level.BrokerOrderID == brokerID && level.Status == LevelActive {
				level.mu.Unlock()
				m.onTPLevelFilled(ctx, group, level, fillPrice)
				return
			}
			level.mu.Unlock()
		}
		return
	}

	// ── Case 3: Single SL order fill ─────────────────────────────────────
	m.checkFixedSLFill(ctx, brokerID, brokerStatus, order)
}

// configToMLLevels converts stored level configs back to models.MultiLevelExitLevel.
func configToMLLevels(configs []SLTPLevelConfig) []models.MultiLevelExitLevel {
	out := make([]models.MultiLevelExitLevel, len(configs))
	for i, c := range configs {
		out[i] = models.MultiLevelExitLevel{LevelNum: c.LevelNum, PricePct: c.PricePct, QtyPct: c.QtyPct}
	}
	return out
}

func (m *Manager) checkFixedSLFill(ctx context.Context, brokerID, brokerStatus string, order *models.Order) {
	if !models.IsFilledStatus(models.OrderStatus(brokerStatus)) {
		return
	}
	m.groups.Range(func(k, v interface{}) bool {
		group := v.(*Group)
		group.mu.Lock()
		isSLOrder := group.SingleSLBrokerID == brokerID && group.State == GroupStateActive
		group.mu.Unlock()

		if isSLOrder {
			fillPrice := 0.0
			if order.FilledPrice != nil {
				fillPrice = *order.FilledPrice
			}
			m.onFixedSLFilled(ctx, group, fillPrice)
			return false // stop ranging
		}
		return true
	})
}

// onTPLevelFilled handles a TP limit order fill for a specific level.
func (m *Manager) onTPLevelFilled(ctx context.Context, group *Group, level *ExitLevelState, fillPrice float64) {
	level.markTriggered(fillPrice)
	m.byBrokerID.Delete(level.BrokerOrderID)

	exitQty := level.ExitQty

	group.mu.Lock()
	group.RemainingQty -= exitQty
	remaining := group.RemainingQty
	group.mu.Unlock()

	m.logger.Info("ml_tp_level_triggered",
		zap.String("group_id", group.GroupID.String()),
		zap.Int("level", level.LevelNum),
		zap.Float64("fill_price", fillPrice),
		zap.Int32("exit_qty", exitQty),
		zap.Int32("remaining_qty", remaining))

	// Update DB
	_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeTP, level.LevelNum,
		models.MLStatusTriggered, fillPrice)

	if remaining <= 0 {
		// All TP levels filled — cancel SL order (if any) and complete
		m.cancelSLOrder(ctx, group)
		m.completeGroup(ctx, group)
		return
	}

	// For FIXED/TRAILING SL: replace SL order with reduced qty
	if (group.SLMode == SLModeFixed || group.SLMode == SLModeTrailing) &&
		group.SingleSLBrokerID != "" && group.TradingMode == "LIVE" {
		m.replaceSLWithReducedQty(ctx, group, remaining)
	}
}

// onFixedSLFilled handles a fixed/trailing SL order fill.
func (m *Manager) onFixedSLFilled(ctx context.Context, group *Group, fillPrice float64) {
	m.logger.Info("ml_fixed_sl_triggered",
		zap.String("group_id", group.GroupID.String()),
		zap.Float64("fill_price", fillPrice))

	// Cancel all remaining TP orders
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

	// Mark all pending SL levels as cancelled (not needed when fixed SL fires)
	for _, level := range group.SLLevels {
		level.mu.Lock()
		if level.Status == LevelActive || level.Status == LevelPending {
			level.Status = LevelCancelled
		}
		level.mu.Unlock()
	}

	group.mu.Lock()
	group.RemainingQty = 0
	group.mu.Unlock()

	m.completeGroup(ctx, group)
}

// ── Price Monitor ─────────────────────────────────────────────────────────────

// priceMonitorLoop polls Redis for LTP and checks SL/TP levels.
// Runs in a dedicated goroutine until the group is completed or cancelled.
func (m *Manager) priceMonitorLoop(ctx context.Context, group *Group) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			group.mu.Lock()
			if group.State != GroupStateActive {
				group.mu.Unlock()
				return
			}
			group.mu.Unlock()

			ltp, err := m.getLTP(ctx, group)
			if err != nil || ltp <= 0 {
				continue
			}

			m.evaluateSLLevels(ctx, group, ltp)

			// Paper TP monitoring (live TP is broker-managed)
			if group.TradingMode == "PAPER" {
				m.evaluateTPLevelsPaper(ctx, group, ltp)
			}

			// Trailing SL adjustment for live trading
			if group.SLMode == SLModeTrailing && group.TradingMode == "LIVE" {
				m.evaluateTrailingSL(ctx, group, ltp)
			}
		}
	}
}

func (m *Manager) getLTP(ctx context.Context, group *Group) (float64, error) {
	if m.priceLookup == nil {
		return 0, fmt.Errorf("no price lookup")
	}
	pCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return m.priceLookup.GetLTP(pCtx, group.Exchange, group.StockCode)
}

// evaluateSLLevels checks all pending SL levels against the current LTP.
// For live orders: places a market exit order. For paper: simulates exit.
func (m *Manager) evaluateSLLevels(ctx context.Context, group *Group, ltp float64) {
	for _, level := range group.SLLevels {
		level.mu.Lock()
		if level.Status != LevelActive {
			level.mu.Unlock()
			continue
		}
		trigger := level.TriggerPrice
		exitQty := level.ExitQty
		levelNum := level.LevelNum
		level.mu.Unlock()

		if !group.SLBreached(trigger, ltp) {
			continue
		}

		m.logger.Info("ml_sl_level_breached",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("level", levelNum),
			zap.Float64("trigger", trigger),
			zap.Float64("ltp", ltp))

		level.markTriggered(ltp)

		group.mu.Lock()
		group.RemainingQty -= exitQty
		remaining := group.RemainingQty
		group.mu.Unlock()

		_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeSL, levelNum,
			models.MLStatusTriggered, ltp)

		if group.TradingMode == "LIVE" && group.broker != nil {
			m.placeSLExitOrder(ctx, group, exitQty, ltp)
		} else {
			// Paper: record the partial exit
			m.recordPaperPartialExit(ctx, group, exitQty, ltp, "SL_HIT", levelNum)
		}

		// Cancel the corresponding TP order for this qty slice (MULTI_LEVEL TP case)
		if group.TPMode == TPModeMultiLevel {
			m.cancelTPLevelForSLFill(ctx, group, levelNum)
		}

		if remaining <= 0 {
			m.completeGroup(ctx, group)
			return
		}
	}
}

// evaluateTPLevelsPaper checks TP levels for paper trading.
func (m *Manager) evaluateTPLevelsPaper(ctx context.Context, group *Group, ltp float64) {
	for _, level := range group.TPLevels {
		level.mu.Lock()
		if level.Status != LevelActive {
			level.mu.Unlock()
			continue
		}
		limit := level.TriggerPrice
		exitQty := level.ExitQty
		levelNum := level.LevelNum
		level.mu.Unlock()

		if !group.TPReached(limit, ltp) {
			continue
		}

		level.markTriggered(ltp)

		group.mu.Lock()
		group.RemainingQty -= exitQty
		remaining := group.RemainingQty
		group.mu.Unlock()

		m.logger.Info("ml_tp_level_reached_paper",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("level", levelNum),
			zap.Float64("limit", limit),
			zap.Float64("ltp", ltp))

		_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeTP, levelNum,
			models.MLStatusTriggered, ltp)

		m.recordPaperPartialExit(ctx, group, exitQty, ltp, "TP_HIT", levelNum)

		if remaining <= 0 {
			m.completeGroup(ctx, group)
			return
		}
	}
}

// evaluateTrailingSL adjusts the single SL order for trailing mode.
func (m *Manager) evaluateTrailingSL(ctx context.Context, group *Group, ltp float64) {
	group.mu.Lock()
	newTrigger, shouldUpdate := group.CalcTrailingSL(ltp)
	if shouldUpdate {
		if group.OrderSide == "BUY" && ltp > group.HighestPrice {
			group.HighestPrice = ltp
		} else if group.OrderSide == "SELL" && ltp < group.HighestPrice {
			group.HighestPrice = ltp
		}
	}
	brokerID := group.SingleSLBrokerID
	group.mu.Unlock()

	if !shouldUpdate || brokerID == "" || group.broker == nil {
		return
	}

	slLimit := newTrigger
	if group.OrderSide == "BUY" {
		slLimit = roundNSE(newTrigger * 0.995)
	} else {
		slLimit = roundNSE(newTrigger * 1.005)
	}

	modOrder := m.buildExitOrder(group.SingleSLOrderID, group, group.RemainingQty, slLimit, "SL-M", newTrigger, "SL")
	modOrder.IndiraOrderID = &brokerID
	if err := group.broker.ModifyOrder(ctx, modOrder, group.Auth); err != nil {
		m.logger.Warn("ml_trailing_sl_modify_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Float64("new_trigger", newTrigger),
			zap.Error(err))
	} else {
		m.logger.Info("ml_trailing_sl_adjusted",
			zap.String("group_id", group.GroupID.String()),
			zap.Float64("new_trigger", newTrigger),
			zap.Float64("ltp", ltp))
	}
}

// ── Order Builder ─────────────────────────────────────────────────────────────

// buildExitOrder creates a *models.Order for an exit placement or modification.
// exitType is "SL" or "TP" (used only for logging / IndiraOrderID tag — not stored).
// triggerPrice is non-zero only for SL-M orders.
func (m *Manager) buildExitOrder(
	exitOrderID uuid.UUID,
	group *Group,
	qty int32,
	price float64,
	orderType string,
	triggerPrice float64,
	exitType string,
) *models.Order {
	_ = exitType // informational only
	now := time.Now()
	o := &models.Order{
		OrderID:      exitOrderID,
		UserID:       group.UserID,
		StrategyID:   group.StrategyID,
		StrategyName: group.StrategyName,
		StockCode:    group.StockCode,
		Exchange:     models.Exchange(group.Exchange),
		Symbol:       group.Symbol,
		OrderType:    models.OrderType(orderType),
		OrderSide:    models.OrderSide(group.ExitSide()),
		Quantity:     qty,
		Price:        &price,
		Validity:     group.Validity,
		ProductType:  group.ProductType,
		TradingMode:  group.TradingMode,
		EventID:      group.EventID,
		RiskApproved: true,
		CreatedAt:    now,
		UpdatedAt:    now,
		SubmittedAt:  &now,
	}
	if triggerPrice > 0 {
		o.StopLoss = &triggerPrice
	}
	return o
}

// ── SL order lifecycle ────────────────────────────────────────────────────────

func (m *Manager) placeSLExitOrder(ctx context.Context, group *Group, qty int32, ltp float64) {
	if group.Auth == nil {
		return
	}
	exitOrderID := uuid.New()
	exitOrder := m.buildExitOrder(exitOrderID, group, qty, ltp, "MARKET", 0, "SL")
	exitOrder.Validity = "IOC"

	brokerID, err := group.broker.PlaceOrder(ctx, exitOrder, group.Auth)
	if err != nil {
		m.logger.Error("ml_sl_exit_order_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Int32("qty", qty),
			zap.Error(err))
		return
	}
	m.logger.Info("ml_sl_exit_placed",
		zap.String("group_id", group.GroupID.String()),
		zap.Int32("qty", qty),
		zap.String("broker_id", brokerID))
}

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

func (m *Manager) replaceSLWithReducedQty(ctx context.Context, group *Group, remainingQty int32) {
	m.cancelSLOrder(ctx, group)

	if group.SLMode != SLModeFixed || group.Auth == nil || group.FixedSLPct <= 0 {
		// For trailing SL: the monitor recalculates on next tick with updated RemainingQty
		return
	}

	slTrigger := group.CalcSLTriggerPrice(group.FixedSLPct)
	slLimit := slTrigger
	if group.OrderSide == "BUY" {
		slLimit = roundNSE(slTrigger * 0.995)
	} else {
		slLimit = roundNSE(slTrigger * 1.005)
	}

	exitOrderID := uuid.New()
	exitOrder := m.buildExitOrder(exitOrderID, group, remainingQty, slLimit, "SL-M", slTrigger, "SL")

	brokerID, err := group.broker.PlaceOrder(ctx, exitOrder, group.Auth)
	if err != nil {
		m.logger.Error("ml_replace_sl_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Float64("trigger", slTrigger),
			zap.Int32("qty", remainingQty),
			zap.Error(err))
		return
	}

	group.mu.Lock()
	group.SingleSLBrokerID = brokerID
	group.SingleSLOrderID = exitOrderID
	group.mu.Unlock()

	m.logger.Info("ml_sl_replaced",
		zap.String("group_id", group.GroupID.String()),
		zap.Float64("trigger", slTrigger),
		zap.Int32("qty", remainingQty),
		zap.String("broker_id", brokerID))
}

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
		m.byBrokerID.Delete(brokerID)
		_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeTP, level.LevelNum,
			models.MLStatusCancelled, 0)
	}
}

// cancelTPLevelForSLFill cancels the TP level corresponding to the same SL level
// when multi-level SL fires. Strategy: cancel TP level N when SL level N fires
// (assuming symmetric level counts). If level counts differ, cancel TP levels
// proportionally: the TP level that covers the same slice of qty.
func (m *Manager) cancelTPLevelForSLFill(ctx context.Context, group *Group, slLevelNum int) {
	// Find the TP level with the same level_num, if it exists
	for _, tpLevel := range group.TPLevels {
		tpLevel.mu.Lock()
		matches := tpLevel.LevelNum == slLevelNum && tpLevel.Status == LevelActive
		bid := tpLevel.BrokerOrderID
		tpLevel.mu.Unlock()

		if matches && bid != "" && group.TradingMode == "LIVE" {
			m.cancelTPOrder(ctx, group, tpLevel, bid)
			return
		} else if matches && group.TradingMode == "PAPER" {
			tpLevel.markCancelled()
			_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeTP, tpLevel.LevelNum,
				models.MLStatusCancelled, 0)
		}
	}
}

// ── Paper Exit Recording ──────────────────────────────────────────────────────

func (m *Manager) recordPaperPartialExit(ctx context.Context, group *Group, qty int32, exitPrice float64, reason string, levelNum int) {
	log.Printf("[ml] Paper partial exit: group=%s level=%d qty=%d price=%.2f reason=%s",
		group.GroupID, levelNum, qty, exitPrice, reason)
	// Create a reverse paper order in DB for audit trail
	reverseSide := "SELL"
	if group.OrderSide == "SELL" {
		reverseSide = "BUY"
	}
	paperID := fmt.Sprintf("PAPER-ML-%s-L%d", group.EntryOrderID.String()[:8], levelNum)
	now := time.Now()
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
		Price:            &exitPrice,
		Validity:         "IOC",
		ProductType:      group.ProductType,
		Status:           models.StatusFilled,
		IsPaperTrade:     true,
		TradingMode:      "PAPER",
		FilledQuantity:   qty,
		FilledPrice:      &exitPrice,
		IsSquareOffOrder: true,
		RiskApproved:     true,
		IndiraOrderID:    &paperID,
		SubmittedAt:      &now,
		ExecutedAt:       &now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := m.repo.Create(ctx, exitOrder); err != nil {
		m.logger.Error("ml_paper_exit_order_create_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Error(err))
	}
}

// ── Group Completion ──────────────────────────────────────────────────────────

func (m *Manager) completeGroup(ctx context.Context, group *Group) {
	group.mu.Lock()
	if group.State != GroupStateActive {
		group.mu.Unlock()
		return
	}
	group.State = GroupStateCompleted
	group.UpdatedAt = time.Now()
	cancel := group.cancelMonitor
	group.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	m.groups.Delete(group.EntryOrderID)

	m.logger.Info("ml_group_completed",
		zap.String("group_id", group.GroupID.String()),
		zap.String("entry_order_id", group.EntryOrderID.String()))
}

// CancelGroup cancels all levels and stops monitoring (e.g., user force-exits).
func (m *Manager) CancelGroup(ctx context.Context, entryOrderID uuid.UUID) {
	v, ok := m.groups.Load(entryOrderID)
	if !ok {
		return
	}
	group := v.(*Group)

	group.mu.Lock()
	group.State = GroupStateCancelled
	cancel := group.cancelMonitor
	group.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	// Cancel live TP orders
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

	// Cancel single SL order if any
	m.cancelSLOrder(ctx, group)

	m.groups.Delete(entryOrderID)
	m.logger.Info("ml_group_cancelled",
		zap.String("group_id", group.GroupID.String()))
}

// CancelGroupsBySymbol cancels all active groups for a user+symbol.
// Implements statusservice.MLHandler interface (same as OCO).
func (m *Manager) CancelGroupsBySymbol(ctx context.Context, userID string, symbol string) {
	m.groups.Range(func(k, v interface{}) bool {
		group := v.(*Group)
		if group.UserID == userID && group.Symbol == symbol {
			m.CancelGroup(ctx, group.EntryOrderID)
		}
		return true
	})
}

// IsKnownBrokerID returns true if the broker order ID belongs to any tracked group.
func (m *Manager) IsKnownBrokerID(brokerID string) bool {
	_, ok := m.byBrokerID.Load(brokerID)
	if ok {
		return true
	}
	// Also check single SL orders
	found := false
	m.groups.Range(func(k, v interface{}) bool {
		g := v.(*Group)
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.SingleSLBrokerID == brokerID {
			found = true
			return false
		}
		return true
	})
	return found
}
