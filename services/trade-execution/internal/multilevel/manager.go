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

	// OnPaperGroupCompleted is called when all ML levels of a PAPER position have
	// triggered. Wired in main.go to broadcast a position_exit WS event.
	// Signature: (userID, orderID string, finalPnL, avgExitPrice float64)
	OnPaperGroupCompleted func(userID, orderID string, finalPnL, avgExitPrice float64)

	// OnPaperLevelTriggered is called each time a single ML level triggers on a
	// PAPER position. Wired in main.go to broadcast an ml_level_triggered WS event
	// so the frontend can refresh level chips in real time.
	//
	// cancelledExitType / cancelledLevelNum: the opposite-side level that was
	// automatically cancelled as a result of this trigger (e.g. TP L1 cancelled when
	// SL L1 fires). Both are empty / -1 when no cancellation occurred.
	//
	// Signature: (userID, orderID, exitType string, levelNum int, exitPrice float64,
	//             remainingQty int32, cancelledExitType string, cancelledLevelNum int)
	OnPaperLevelTriggered func(userID, orderID, exitType string, levelNum int, exitPrice float64, remainingQty int32, cancelledExitType string, cancelledLevelNum int)

	// OnPaperQtyUpdated is called after a partial ML exit so the in-memory monitor
	// cache can update FilledQuantity for the entry order. Wired in main.go to
	// monitor.UpdateCachedOrderQty.
	// Signature: (entryOrderID uuid.UUID, remainingQty int32)
	OnPaperQtyUpdated func(entryOrderID uuid.UUID, remainingQty int32)

	// OnPaperSLMoved is called after a TP level fires and the effective stop for
	// the remaining position is moved to breakeven (after L1) or to the previous
	// TP trigger price (after L2+). Wired in main.go to monitor.UpdateCachedOrderSL
	// so the regular SL price-check in the paper monitor uses the updated stop.
	// Signature: (entryOrderID uuid.UUID, newSL float64)
	OnPaperSLMoved func(entryOrderID uuid.UUID, newSL float64)
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
		state.TriggerPrice    = trigger
		state.ExitQty         = exitQty
		state.OriginalExitQty = exitQty // set once; never modified after this
		state.Status          = LevelActive
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
		state.TriggerPrice    = limit
		state.ExitQty         = exitQty
		state.OriginalExitQty = exitQty // set once; never modified after this
		state.Status          = LevelActive
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

	// For FIXED/TRAILING SL: cancel old SL order and replace with reduced qty
	// and a new trigger moved to breakeven (L1) or previous TP price (L2+).
	if (group.SLMode == SLModeFixed || group.SLMode == SLModeTrailing) &&
		group.TradingMode == "LIVE" {
		newSLTrigger := m.computeSLAfterTPFill(group, level.LevelNum)
		m.replaceSLWithReducedQty(ctx, group, remaining, newSLTrigger)
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
		levelNum := level.LevelNum
		level.mu.Unlock()

		if !group.SLBreached(trigger, ltp) {
			continue
		}

		// Safety cap: never exit more than what actually remains.
		group.mu.Lock()
		exitQty := level.ExitQty
		if exitQty > group.RemainingQty {
			exitQty = group.RemainingQty
		}
		group.mu.Unlock()

		// For paper trading, exit at the trigger price (the level's configured stop price)
		// rather than the current LTP. This ensures each level shows its own distinct exit
		// price, even when multiple levels breach in the same poll tick.
		exitPrice := ltp
		if group.TradingMode == "PAPER" && trigger > 0 {
			exitPrice = trigger
		}

		// exitQty=0 happens when totalQty is too small for the configured qty_pct
		// (e.g. qty=1 with level1=50% → int32(0.5)=0). Skip placing an order but
		// mark the level triggered so it doesn't fire again; remaining qty unchanged.
		if exitQty <= 0 {
			m.logger.Warn("ml_sl_level_qty_zero_skipped",
				zap.String("group_id", group.GroupID.String()),
				zap.Int("level", levelNum),
				zap.Float64("trigger", trigger),
				zap.Int32("total_qty", group.TotalQty))
			level.markTriggered(exitPrice)
			_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeSL, levelNum,
				models.MLStatusTriggered, exitPrice)
			continue
		}

		m.logger.Info("ml_sl_level_breached",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("level", levelNum),
			zap.Float64("trigger", trigger),
			zap.Float64("ltp", ltp),
			zap.Float64("exit_price", exitPrice))

		level.markTriggered(exitPrice)

		group.mu.Lock()
		group.RemainingQty -= exitQty
		remaining := group.RemainingQty
		group.mu.Unlock()

		_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeSL, levelNum,
			models.MLStatusTriggered, exitPrice)

		if group.TradingMode == "LIVE" && group.broker != nil {
			m.placeSLExitOrder(ctx, group, exitQty, ltp)
		} else {
			// Paper: record the partial exit and reduce entry order's filled_quantity.
			m.recordPaperPartialExit(ctx, group, exitQty, exitPrice, "SL_HIT", levelNum)
			if err := m.repo.UpdatePaperPositionFilledQty(ctx, group.EntryOrderID, remaining); err != nil {
				m.logger.Error("ml_paper_sl_qty_update_failed",
					zap.String("group_id", group.GroupID.String()),
					zap.Int("level", levelNum),
					zap.Error(err))
			}
			if m.OnPaperQtyUpdated != nil {
				m.OnPaperQtyUpdated(group.EntryOrderID, remaining)
			}
		}

		// Rebalance remaining TP levels so their qty sum equals the new RemainingQty.
		cancelledTPLevelNum := -1
		if group.TPMode == TPModeMultiLevel {
			cancelledTPLevelNum = m.rebalanceTPAfterSLFill(ctx, group, levelNum, remaining)
		}

		if group.TradingMode != "LIVE" && m.OnPaperLevelTriggered != nil {
			cancelledExitType := ""
			if cancelledTPLevelNum >= 0 {
				cancelledExitType = models.MLExitTypeTP
			}
			m.OnPaperLevelTriggered(group.UserID, group.EntryOrderID.String(), models.MLExitTypeSL, levelNum, exitPrice, remaining, cancelledExitType, cancelledTPLevelNum)
		}

		if remaining <= 0 {
			m.completeGroup(ctx, group)
			return
		}
		// Process one level per tick so each fires at its own price on the next poll.
		return
	}
}

// computeSLAfterTPFill returns the new SL trigger after a TP level fires.
//
// Risk management rule:
//   - After TP level 1 fires → move SL to entry price (breakeven, zero loss)
//   - After TP level N > 1 fires → move SL to level N-1's trigger price (lock in profit)
//
// Applied to both paper (via OnPaperSLMoved callback) and live (via replaceSLWithReducedQty).
func (m *Manager) computeSLAfterTPFill(group *Group, tpLevelNum int) float64 {
	if tpLevelNum <= 1 {
		// First profit level hit → protect capital at entry (breakeven)
		return group.FillPrice
	}
	// Subsequent levels → lock in the previous TP profit
	for _, l := range group.TPLevels {
		l.mu.Lock()
		num := l.LevelNum
		price := l.TriggerPrice
		l.mu.Unlock()
		if num == tpLevelNum-1 {
			return price
		}
	}
	// Fallback: use entry price if previous level not found
	return group.FillPrice
}

// rebalanceMLSLAfterTP moves all remaining active SL levels to the new floor price
// after a TP level fires in MULTI_LEVEL SL mode. This ensures the remaining position
// is protected at breakeven (after TP L1) or at the previous TP price (after TP L2+).
// It only tightens stops — it never widens a level that is already better than newSL.
func (m *Manager) rebalanceMLSLAfterTP(ctx context.Context, group *Group, tpLevelNum int) {
	newSL := m.computeSLAfterTPFill(group, tpLevelNum)
	if newSL <= 0 {
		return
	}
	for i, slLevel := range group.SLLevels {
		slLevel.mu.Lock()
		if slLevel.Status != LevelActive {
			slLevel.mu.Unlock()
			continue
		}
		oldTrigger := slLevel.TriggerPrice
		// Only tighten: for BUY the new SL must be higher (better), for SELL lower.
		shouldMove := (group.OrderSide == "BUY" && newSL > oldTrigger) ||
			(group.OrderSide == "SELL" && newSL < oldTrigger)
		if !shouldMove {
			slLevel.mu.Unlock()
			continue
		}
		slLevel.TriggerPrice = newSL
		levelNum := slLevel.LevelNum
		exitQty := slLevel.ExitQty
		slLevel.mu.Unlock()

		var cfg SLTPLevelConfig
		if i < len(group.SLLevelConfigs) {
			cfg = group.SLLevelConfigs[i]
		}
		triggerCopy := newSL
		_ = m.repo.UpsertMultiLevelExitLevel(ctx, &models.MultiLevelExitRecord{
			EntryOrderID: group.EntryOrderID,
			ExitType:     models.MLExitTypeSL,
			LevelNum:     levelNum,
			PricePct:     cfg.PricePct,
			QtyPct:       cfg.QtyPct,
			TriggerPrice: &triggerCopy,
			ExitQty:      &exitQty,
			Status:       models.MLStatusActive,
		})
		m.logger.Info("ml_sl_rebalanced_after_tp",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("sl_level", levelNum),
			zap.Float64("old_trigger", oldTrigger),
			zap.Float64("new_trigger", newSL),
			zap.Int("tp_level", tpLevelNum))
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
		levelNum := level.LevelNum
		level.mu.Unlock()

		if !group.TPReached(limit, ltp) {
			continue
		}

		// Safety cap: never exit more than what actually remains.
		group.mu.Lock()
		exitQty := level.ExitQty
		if exitQty > group.RemainingQty {
			exitQty = group.RemainingQty
		}
		group.mu.Unlock()

		// For paper trading, exit at the level's configured trigger price (not LTP).
		// This ensures each TP level shows its own distinct exit price even when the
		// price jumps past multiple levels in a single poll tick.
		exitPrice := ltp
		if limit > 0 {
			exitPrice = limit
		}

		// exitQty=0 when totalQty is too small for the configured qty_pct — skip.
		if exitQty <= 0 {
			m.logger.Warn("ml_tp_level_qty_zero_skipped",
				zap.String("group_id", group.GroupID.String()),
				zap.Int("level", levelNum),
				zap.Float64("limit", limit),
				zap.Int32("total_qty", group.TotalQty))
			level.markTriggered(exitPrice)
			_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeTP, levelNum,
				models.MLStatusTriggered, exitPrice)
			continue
		}

		level.markTriggered(exitPrice)

		group.mu.Lock()
		group.RemainingQty -= exitQty
		remaining := group.RemainingQty
		group.mu.Unlock()

		m.logger.Info("ml_tp_level_reached_paper",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("level", levelNum),
			zap.Float64("limit", limit),
			zap.Float64("ltp", ltp),
			zap.Float64("exit_price", exitPrice))

		_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID, models.MLExitTypeTP, levelNum,
			models.MLStatusTriggered, exitPrice)

		m.recordPaperPartialExit(ctx, group, exitQty, exitPrice, "TP_HIT", levelNum)
		// Reduce entry order's filled_quantity so open positions shows remaining qty.
		if err := m.repo.UpdatePaperPositionFilledQty(ctx, group.EntryOrderID, remaining); err != nil {
			m.logger.Error("ml_paper_tp_qty_update_failed",
				zap.String("group_id", group.GroupID.String()),
				zap.Int("level", levelNum),
				zap.Error(err))
		}
		if m.OnPaperQtyUpdated != nil {
			m.OnPaperQtyUpdated(group.EntryOrderID, remaining)
		}


		// Rebalance remaining SL levels so their qty sum equals the new RemainingQty.
		cancelledSLLevelNum := -1
		if group.SLMode == SLModeMultiLevel {
			cancelledSLLevelNum = m.rebalanceMLSLAfterTPFill(ctx, group, levelNum, remaining)
		}

		if m.OnPaperLevelTriggered != nil {
			cancelledExitType := ""
			if cancelledSLLevelNum >= 0 {
				cancelledExitType = models.MLExitTypeSL
			}
			m.OnPaperLevelTriggered(group.UserID, group.EntryOrderID.String(), models.MLExitTypeTP, levelNum, exitPrice, remaining, cancelledExitType, cancelledSLLevelNum)
		}

		if remaining <= 0 {
			m.completeGroup(ctx, group)
			return
		}
		// Process one level per tick for clean sequential paper simulation.
		return
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

// replaceSLWithReducedQty cancels the current single SL order and places a new
// one for the remaining qty using newSLTrigger as the trigger price.
//
// newSLTrigger is computed by computeSLAfterTPFill:
//   - After TP L1 → entry price (breakeven, zero downside risk on remaining qty)
//   - After TP L2+ → previous TP trigger price (lock in prior level's profit)
//
// Applies to both FIXED and TRAILING SL modes; for TRAILING the new order is a
// plain SL-M at the computed trigger — trailing continues from there via the
// evaluateTrailingSL loop on the next price tick.
func (m *Manager) replaceSLWithReducedQty(ctx context.Context, group *Group, remainingQty int32, newSLTrigger float64) {
	m.cancelSLOrder(ctx, group)

	if group.Auth == nil || group.broker == nil || newSLTrigger <= 0 {
		return
	}

	slLimit := newSLTrigger
	if group.OrderSide == "BUY" {
		slLimit = roundNSE(newSLTrigger * 0.995)
	} else {
		slLimit = roundNSE(newSLTrigger * 1.005)
	}

	exitOrderID := uuid.New()
	exitOrder := m.buildExitOrder(exitOrderID, group, remainingQty, slLimit, "SL-M", newSLTrigger, "SL")

	brokerID, err := group.broker.PlaceOrder(ctx, exitOrder, group.Auth)
	if err != nil {
		m.logger.Error("ml_replace_sl_failed",
			zap.String("group_id", group.GroupID.String()),
			zap.Float64("trigger", newSLTrigger),
			zap.Int32("qty", remainingQty),
			zap.Error(err))
		return
	}

	group.mu.Lock()
	group.SingleSLBrokerID = brokerID
	group.SingleSLOrderID = exitOrderID
	// For trailing SL, reset HighestPrice to current fill so trailing advances
	// from the new (breakeven / locked-in) level, not from the original entry.
	if group.SLMode == SLModeTrailing {
		group.HighestPrice = newSLTrigger
	}
	group.mu.Unlock()

	m.logger.Info("ml_sl_replaced",
		zap.String("group_id", group.GroupID.String()),
		zap.Float64("new_trigger", newSLTrigger),
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

// ── Paper Exit Recording ──────────────────────────────────────────────────────

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

	// Encode entry price, level, and reason in IndiraOrderID so the UI can display them.
	paperID := fmt.Sprintf("PAPER-ML-%s-L%d-%s", group.EntryOrderID.String()[:8], levelNum, reason)
	now := time.Now()

	// Store entry price in Price field; FilledPrice holds the exit price.
	// This lets the Closed tab show both entry and exit prices for the partial exit row.
	entryPrice := group.FillPrice

	// Compute partial P&L so it can be displayed directly without a join.
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
		Price:            &entryPrice, // entry price of the original position
		Validity:         "IOC",
		ProductType:      group.ProductType,
		Status:           models.StatusFilled,
		IsPaperTrade:     true,
		TradingMode:      "PAPER",
		FilledQuantity:   qty,
		FilledPrice:      &exitPrice,  // price at which this partial exit was executed
		PaperExitPrice:   &exitPrice,  // marks this as a closed record for the Closed tab query
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

// ── Rebalancing ───────────────────────────────────────────────────────────────

// rebalanceTPAfterSLFill is called when SL level slLevelNum fires.
// It proportionally scales ALL remaining active TP levels (including the
// same-numbered one) so their qty sum equals the new remainingQty.
// TP L1 is kept alive so the remaining position targets the nearest profit
// level first rather than jumping straight to TP L2.
//
// Always returns -1 (no TP level explicitly cancelled; levels may be cancelled
// only if their scaled qty rounds to zero).
//
// Example:
//
//	SL L1 fires (50 qty). RemainingQty = 50.
//	Active TPs: L1 (50 qty), L2 (50 qty) → pendingTotal = 100
//	Scale = 50/100
//	New L1 = 25  (stays active — nearest profit target)
//	New L2 = 25  (last level absorbs rounding)
func (m *Manager) rebalanceTPAfterSLFill(
	ctx context.Context,
	group *Group,
	slLevelNum int,
	remainingQty int32,
) int {
	reason := fmt.Sprintf("SL_L%d_TRIGGERED", slLevelNum)

	// Scale ALL active TP levels (including the same-numbered one) proportionally
	// to the new remainingQty. TP L1 stays alive as the nearest profit target for
	// the remaining position — cancelling it caused TP L2 to be the first trigger
	// even when price only reached TP L1's level.
	type tpEntry struct {
		state *ExitLevelState
		qty   int32
		orig  int32
	}
	var active []tpEntry
	var pendingTotal int32
	for _, tp := range group.TPLevels {
		tp.mu.Lock()
		if tp.Status == LevelActive {
			active = append(active, tpEntry{tp, tp.ExitQty, tp.OriginalExitQty})
			pendingTotal += tp.ExitQty
		}
		tp.mu.Unlock()
	}

	if len(active) == 0 || pendingTotal == 0 || remainingQty <= 0 {
		return -1
	}

	// Scale each TP level proportionally.
	var allocated int32
	for i, entry := range active {
		var newQty int32
		if i == len(active)-1 {
			// Last level absorbs rounding so all qtys sum exactly to remainingQty.
			newQty = remainingQty - allocated
		} else {
			newQty = int32(float64(entry.qty) * float64(remainingQty) / float64(pendingTotal))
		}
		if newQty < 0 {
			newQty = 0
		}
		allocated += newQty

		entry.state.mu.Lock()
		entry.state.ExitQty = newQty
		entry.state.mu.Unlock()

		if newQty == 0 {
			// Level quantity rounded to zero — cancel it entirely.
			if entry.state.BrokerOrderID != "" && group.TradingMode == "LIVE" {
				m.cancelTPOrder(ctx, group, entry.state, entry.state.BrokerOrderID)
			} else {
				entry.state.markCancelled()
				_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID,
					models.MLExitTypeTP, entry.state.LevelNum, models.MLStatusCancelled, 0)
			}
			_ = m.repo.UpdateMLLevelRebalancedQty(ctx, group.EntryOrderID,
				models.MLExitTypeTP, entry.state.LevelNum, entry.orig, 0, reason)
			m.logger.Info("ml_tp_level_cancelled_zero_qty_after_rebalance",
				zap.String("group_id", group.GroupID.String()),
				zap.Int("tp_level", entry.state.LevelNum),
				zap.String("reason", reason))
			continue
		}

		// Persist rebalanced qty with full audit (original vs new).
		_ = m.repo.UpdateMLLevelRebalancedQty(ctx, group.EntryOrderID,
			models.MLExitTypeTP, entry.state.LevelNum, entry.orig, newQty, reason)

		// For live trading: modify the broker LIMIT order to the new qty.
		if group.TradingMode == "LIVE" && entry.state.BrokerOrderID != "" && group.broker != nil {
			modOrder := m.buildExitOrder(
				entry.state.ExitOrderID, group, newQty,
				entry.state.TriggerPrice, "LIMIT", 0, "TP",
			)
			modOrder.IndiraOrderID = &entry.state.BrokerOrderID
			if err := group.broker.ModifyOrder(ctx, modOrder, group.Auth); err != nil {
				m.logger.Warn("ml_tp_rebalance_modify_failed",
					zap.String("group_id", group.GroupID.String()),
					zap.Int("tp_level", entry.state.LevelNum),
					zap.Int32("new_qty", newQty),
					zap.Error(err))
				// ModifyOrder failed — cancel to avoid over-exit.
				m.cancelTPOrder(ctx, group, entry.state, entry.state.BrokerOrderID)
			} else {
				m.logger.Info("ml_tp_rebalance_modified",
					zap.String("group_id", group.GroupID.String()),
					zap.Int("tp_level", entry.state.LevelNum),
					zap.Int32("original_qty", entry.orig),
					zap.Int32("new_qty", newQty),
					zap.String("reason", reason))
			}
		} else {
			m.logger.Info("ml_tp_rebalanced_paper",
				zap.String("group_id", group.GroupID.String()),
				zap.Int("tp_level", entry.state.LevelNum),
				zap.Int32("original_qty", entry.orig),
				zap.Int32("new_qty", newQty),
				zap.String("reason", reason))
		}
	}

	return -1
}

// rebalanceMLSLAfterTPFill replaces cancelSLLevelForTPFill for MULTI_LEVEL SL mode.
// When TP level tpLevelNum fires and reduces RemainingQty:
//  1. Cancel the same-numbered SL level (its qty slice is now fully taken profit on).
//  2. Proportionally scale remaining active SL ExitQty values to sum to remainingQty.
//
// For MULTI_LEVEL SL, exits are application-managed (no broker orders), so this
// is in-memory + DB only. Returns the cancelled same-numbered SL level (or -1).
func (m *Manager) rebalanceMLSLAfterTPFill(
	ctx context.Context,
	group *Group,
	tpLevelNum int,
	remainingQty int32,
) int {
	reason := fmt.Sprintf("TP_L%d_TRIGGERED", tpLevelNum)

	// Step 1: cancel the same-numbered SL level.
	cancelledNum := -1
	for _, sl := range group.SLLevels {
		sl.mu.Lock()
		matches := sl.LevelNum == tpLevelNum && sl.Status == LevelActive
		levelNum := sl.LevelNum
		origQty := sl.OriginalExitQty
		sl.mu.Unlock()

		if !matches {
			continue
		}
		cancelledNum = levelNum
		sl.markCancelled()
		_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID,
			models.MLExitTypeSL, levelNum, models.MLStatusCancelled, 0)
		_ = m.repo.UpdateMLLevelRebalancedQty(ctx, group.EntryOrderID,
			models.MLExitTypeSL, levelNum, origQty, 0, reason)
		break
	}

	// Step 2: collect remaining active SL levels.
	type slEntry struct {
		state *ExitLevelState
		qty   int32
		orig  int32
	}
	var active []slEntry
	var pendingTotal int32
	for _, sl := range group.SLLevels {
		sl.mu.Lock()
		if sl.Status == LevelActive {
			active = append(active, slEntry{sl, sl.ExitQty, sl.OriginalExitQty})
			pendingTotal += sl.ExitQty
		}
		sl.mu.Unlock()
	}

	if len(active) == 0 || pendingTotal == 0 || remainingQty <= 0 {
		return cancelledNum
	}

	// Step 3: scale each remaining SL level proportionally.
	var allocated int32
	for i, entry := range active {
		var newQty int32
		if i == len(active)-1 {
			newQty = remainingQty - allocated
		} else {
			newQty = int32(float64(entry.qty) * float64(remainingQty) / float64(pendingTotal))
		}
		if newQty < 0 {
			newQty = 0
		}
		allocated += newQty

		entry.state.mu.Lock()
		entry.state.ExitQty = newQty
		entry.state.mu.Unlock()

		if newQty == 0 {
			entry.state.markCancelled()
			_ = m.repo.UpdateMultiLevelLevelStatus(ctx, group.EntryOrderID,
				models.MLExitTypeSL, entry.state.LevelNum, models.MLStatusCancelled, 0)
			_ = m.repo.UpdateMLLevelRebalancedQty(ctx, group.EntryOrderID,
				models.MLExitTypeSL, entry.state.LevelNum, entry.orig, 0, reason)
			m.logger.Info("ml_sl_level_cancelled_zero_qty_after_rebalance",
				zap.String("group_id", group.GroupID.String()),
				zap.Int("sl_level", entry.state.LevelNum),
				zap.String("reason", reason))
			continue
		}

		_ = m.repo.UpdateMLLevelRebalancedQty(ctx, group.EntryOrderID,
			models.MLExitTypeSL, entry.state.LevelNum, entry.orig, newQty, reason)

		m.logger.Info("ml_sl_rebalanced_after_tp",
			zap.String("group_id", group.GroupID.String()),
			zap.Int("sl_level", entry.state.LevelNum),
			zap.Int32("original_qty", entry.orig),
			zap.Int32("new_qty", newQty),
			zap.String("reason", reason))
	}

	return cancelledNum
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

	// For paper trades: persist the final exit price and PnL on the entry order,
	// and notify the WS server so the frontend removes it from open positions.
	//
	// IMPORTANT: cancel() above has already cancelled the monitor goroutine's ctx.
	// We must use a fresh context here, otherwise UpdatePaperTradeExit will fail
	// with "context canceled" and paper_exit_price will never be written to the DB,
	// leaving the order as a zombie that reloads on every service restart.
	if group.TradingMode == "PAPER" {
		avgExitPrice, totalPnL := m.computeFinalPaperPnL(group)
		exitCtx, exitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer exitCancel()
		if err := m.repo.UpdatePaperTradeExit(exitCtx, group.EntryOrderID, avgExitPrice, totalPnL); err != nil {
			m.logger.Error("ml_complete_paper_exit_update_failed",
				zap.String("group_id", group.GroupID.String()),
				zap.String("entry_order_id", group.EntryOrderID.String()),
				zap.Error(err))
		}
		if m.OnPaperGroupCompleted != nil {
			m.OnPaperGroupCompleted(group.UserID, group.EntryOrderID.String(), totalPnL, avgExitPrice)
		}
	}
}

// computeFinalPaperPnL computes the weighted-average exit price and total PnL
// for a completed paper ML group by summing all triggered level exits.
func (m *Manager) computeFinalPaperPnL(group *Group) (avgExitPrice float64, totalPnL float64) {
	var totalExitQty int32
	var weightedSum float64

	collectLevels := func(levels []*ExitLevelState) {
		for _, l := range levels {
			l.mu.Lock()
			if l.Status == LevelTriggered && l.ExitQty > 0 && l.ExitPrice > 0 {
				weightedSum += l.ExitPrice * float64(l.ExitQty)
				totalExitQty += l.ExitQty
			}
			l.mu.Unlock()
		}
	}
	collectLevels(group.SLLevels)
	collectLevels(group.TPLevels)

	if totalExitQty > 0 {
		avgExitPrice = weightedSum / float64(totalExitQty)
	} else {
		// Fallback: use fill price (no exit recorded — shouldn't happen)
		avgExitPrice = group.FillPrice
	}

	qty := float64(group.TotalQty)
	if group.OrderSide == "BUY" {
		totalPnL = (avgExitPrice - group.FillPrice) * qty
	} else {
		totalPnL = (group.FillPrice - avgExitPrice) * qty
	}
	return
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

// CancelGroupForExit cancels an active ML group and, for paper positions with
// remaining qty, records a partial exit at exitPrice so the closed-positions tab
// shows a distinct row for the unexited qty (e.g. during square-off or force-exit).
// reason is the exit tag written into IndiraOrderID (e.g. "SQUARE_OFF", "FORCE_EXIT").
// Safe to call even if no group is registered (no-op).
func (m *Manager) CancelGroupForExit(ctx context.Context, entryOrderID uuid.UUID, exitPrice float64, reason string) {
	v, ok := m.groups.Load(entryOrderID)
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

	// Cancel live TP / SL broker orders.
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

	// For paper positions with remaining qty, record a partial exit so the
	// closed-positions tab shows this exit as its own row.
	if tradingMode == "PAPER" && remaining > 0 && exitPrice > 0 {
		m.recordPaperPartialExit(ctx, group, remaining, exitPrice, reason, 0)
		if err := m.repo.UpdatePaperPositionFilledQty(ctx, entryOrderID, 0); err != nil {
			m.logger.Warn("ml_cancel_for_exit_qty_update_failed",
				zap.String("group_id", group.GroupID.String()),
				zap.Error(err))
		}
	}

	m.groups.Delete(entryOrderID)
	m.logger.Info("ml_group_cancelled_for_exit",
		zap.String("group_id", group.GroupID.String()),
		zap.Float64("exit_price", exitPrice),
		zap.String("reason", reason),
		zap.Int32("remaining_qty", remaining))
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

// CancelGroupsByStrategy cancels all active groups for a user+strategy.
// For paper positions with remaining qty, a partial exit record is created at
// the current LTP (reason: STRATEGY_DEACTIVATED) so the closed-positions tab
// shows the deactivation exit as a distinct row alongside any level exits that
// already fired. Called by StrategyEventsConsumer when a strategy is paused or deleted.
func (m *Manager) CancelGroupsByStrategy(ctx context.Context, userID, strategyID string) {
	m.groups.Range(func(k, v interface{}) bool {
		group := v.(*Group)
		if group.UserID != userID || group.StrategyID != strategyID {
			return true
		}

		group.mu.Lock()
		if group.State != GroupStateActive {
			group.mu.Unlock()
			return true
		}
		remaining := group.RemainingQty
		tradingMode := group.TradingMode
		// Mark cancelled immediately so no concurrent level fires overlap.
		group.State = GroupStateCancelled
		cancel := group.cancelMonitor
		group.mu.Unlock()

		if cancel != nil {
			cancel()
		}

		// For paper positions, record the deactivation exit so closed-positions
		// shows a separate row for the remaining qty.
		if tradingMode == "PAPER" && remaining > 0 && m.priceLookup != nil {
			ltp, err := m.priceLookup.GetLTP(ctx, group.Exchange, group.StockCode)
			if err == nil && ltp > 0 {
				m.recordPaperPartialExit(ctx, group, remaining, ltp, "STRATEGY_DEACTIVATED", 0)
				if err2 := m.repo.UpdatePaperPositionFilledQty(ctx, group.EntryOrderID, 0); err2 != nil {
					m.logger.Warn("ml_cancel_strategy_qty_update_failed",
						zap.String("group_id", group.GroupID.String()),
						zap.Error(err2))
				}
			} else {
				m.logger.Warn("ml_cancel_strategy_ltp_unavailable",
					zap.String("group_id", group.GroupID.String()),
					zap.String("symbol", group.Symbol),
					zap.Error(err))
			}
		}

		m.groups.Delete(group.EntryOrderID)
		m.logger.Info("ml_group_cancelled_by_strategy",
			zap.String("group_id", group.GroupID.String()),
			zap.String("strategy_id", strategyID),
			zap.Int32("remaining_qty", remaining))

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
