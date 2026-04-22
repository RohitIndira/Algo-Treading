package executor

import (
	"context"
	"fmt"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/oco"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/scheduler"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/statusservice"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// SignalProcessor processes trade signals from Kafka.
// This is the primary entry point for all orders from the rules-engine.
type SignalProcessor struct {
	executor          *OrderExecutor
	orderRepo         repository.OrderRepository
	kafkaPub          *publisher.KafkaPublisher
	priceMonitor      *scheduler.PriceMonitor
	ocoManager        *oco.OCOManager
	multiLevelManager MultiLevelManager
	logger            *zap.Logger
}

// MultiLevelManager is the interface the SignalProcessor uses to register
// entry orders with the multi-level exit manager. Implemented by *multilevel.Manager.
type MultiLevelManager interface {
	RegisterEntry(
		entryOrder *models.Order,
		slMode, tpMode string,
		slLevels, tpLevels []models.MultiLevelExitLevel,
		fixedSLPct, trailingSLPct float64,
		auth *indiraClient.AuthContext,
	)
	OnEntryFill(ctx context.Context, entryOrderID uuid.UUID, fillPrice float64, filledQty int32,
		slLevels, tpLevels []models.MultiLevelExitLevel)
}

// NewSignalProcessor creates a new trade signal processor.
func NewSignalProcessor(
	executor *OrderExecutor,
	orderRepo repository.OrderRepository,
	kafkaPub *publisher.KafkaPublisher,
	// statusSvc is now wired inside OrderExecutor — no longer needed here
	_ *statusservice.OrderStatusService,
	logger *zap.Logger,
) *SignalProcessor {
	return &SignalProcessor{
		executor:  executor,
		orderRepo: orderRepo,
		kafkaPub:  kafkaPub,
		logger:    logger,
	}
}

// SetPriceMonitor sets the price monitor for routing below_min orders.
func (p *SignalProcessor) SetPriceMonitor(pm *scheduler.PriceMonitor) {
	p.priceMonitor = pm
}

// SetOCOManager sets the OCO manager for routing trailing SL orders.
func (p *SignalProcessor) SetOCOManager(m *oco.OCOManager) {
	p.ocoManager = m
}

// SetMultiLevelManager sets the multi-level exit manager.
func (p *SignalProcessor) SetMultiLevelManager(m MultiLevelManager) {
	p.multiLevelManager = m
}

// ProcessTradeSignal processes a trade signal from Kafka — hot path.
// DB persistence is synchronous; Indira API call is critical path.
// WS subscription and Kafka publishing are handled inside OrderExecutor.
func (p *SignalProcessor) ProcessTradeSignal(ctx context.Context, signal *models.TradeSignal, kafkaTime time.Time) error {
	processStart := time.Now()

	// Idempotency check: skip if this order was already processed.
	orderID, err := uuid.Parse(signal.OrderID)
	if err != nil {
		return fmt.Errorf("invalid order_id in signal: %w", err)
	}
	idempotencyStart := time.Now()
	exists, err := p.orderRepo.ExistsByID(ctx, orderID)
	idempotencyMs := float64(time.Since(idempotencyStart).Microseconds()) / 1000.0
	if err == nil && exists {
		p.logger.Info("signal_duplicate_skip", zap.String("oid", signal.OrderID))
		return nil
	}

	// Convert TradeSignal to Order
	order, err := p.convertSignalToOrder(signal)
	if err != nil {
		return fmt.Errorf("failed to convert signal to order: %w", err)
	}

	// Persist order to DB synchronously so subsequent Update calls can find the row.
	dbStart := time.Now()
	if err := p.orderRepo.Create(ctx, order); err != nil {
		p.logger.Error("signal_db_persist_failed",
			zap.String("oid", order.OrderID.String()),
			zap.Error(err))
		return fmt.Errorf("failed to persist order: %w", err)
	}
	dbMs := float64(time.Since(dbStart).Microseconds()) / 1000.0
	metrics.SignalDBPersistDuration.Observe(time.Since(dbStart).Seconds())

	// ── Detect multi-level SL / TP config ───────────────────────────────
	hasMultiLevelSL := len(signal.MultiLevelSL) > 0 && signal.StopLossType == "MULTI_LEVEL"
	hasMultiLevelTP := len(signal.MultiLevelTP) > 0 && signal.TakeProfitType == "MULTI_LEVEL"
	isMultiLevel := (hasMultiLevelSL || hasMultiLevelTP) && p.multiLevelManager != nil

	// ── Route 1: Trailing SL → Custom OCO ───────────────────────────────
	// NOTE: Trailing SL + Multi-level TP is handled by Route 4 (multi-level),
	// NOT by Route 1 (OCO), because multi-level TP requires N separate LIMIT orders.
	isTrailingSL := order.StopLossType != nil && *order.StopLossType == "TRAILING"
	hasAuth := order.BearerToken != nil && order.AppId != nil && order.Source != nil

	// If auth is missing on the signal (rules-engine doesn't carry credentials),
	// fetch from the credentials cache so OCO / PriceMonitor routes can proceed.
	if !hasAuth && !order.IsPaperTrade && p.executor.credsCache != nil {
		_, appId, source, bearerToken, credErr := p.executor.credsCache.Get(ctx, order.UserID)
		if credErr == nil && bearerToken != "" && appId != "" && source != "" {
			order.BearerToken = &bearerToken
			order.AppId = &appId
			order.Source = &source
			hasAuth = true
			p.logger.Debug("auth_backfilled_from_cache",
				zap.String("oid", order.OrderID.String()),
				zap.String("uid", order.UserID))
		} else if credErr != nil {
			p.logger.Warn("auth_backfill_failed",
				zap.String("oid", order.OrderID.String()),
				zap.String("uid", order.UserID),
				zap.Error(credErr))
		}
	}

	// Route 4 intercepts: Trailing SL with multi-level TP skips OCO.
	if isTrailingSL && p.ocoManager != nil && hasAuth && !order.IsPaperTrade && !isMultiLevel {
		err := p.routeToOCO(ctx, order, signal)
		p.logOrderTiming(signal, "oco", kafkaTime, processStart, idempotencyMs, dbMs, err)
		return err
	}

	// ── Route 2: below_min → Price Monitor ──────────────────────────────
	isBelowMin := order.OrderType == models.OrderTypeStopLoss &&
		(order.ProductType == "BRACKET" || order.ProductType == "BRACKET_ORDER" || order.ProductType == "BO")

	if isBelowMin && p.priceMonitor != nil {
		targetPrice := 0.0
		if order.Price != nil {
			targetPrice = *order.Price
		}
		if targetPrice <= 0 {
			return fmt.Errorf("below_min order %s has no target price", order.OrderID)
		}

		p.priceMonitor.Watch(order, targetPrice)
		p.logOrderTiming(signal, "price_monitor", kafkaTime, processStart, idempotencyMs, dbMs, nil)
		return nil
	}

	// ── Route 4: Multi-level SL / TP ────────────────────────────────────
	// Place entry order normally via executor, then register with ML manager.
	// After paper fill: call OnEntryFill immediately.
	// After live fill: HandleBrokerUpdate (via status service WS) calls OnEntryFill.
	if isMultiLevel {
		err = p.routeToMultiLevel(ctx, order, signal)
		p.logOrderTiming(signal, "multi_level", kafkaTime, processStart, idempotencyMs, dbMs, err)
		return err
	}

	// ── Route 3: Default → Broker API (bracket or regular order) ────────
	err = p.executor.ExecuteOrder(ctx, order)
	p.logOrderTiming(signal, "executor", kafkaTime, processStart, idempotencyMs, dbMs, err)
	if err != nil {
		return fmt.Errorf("order execution failed: %w", err)
	}

	// If a trailing-SL order reached Route 3 (e.g. auth was unavailable at
	// Route 1 check but the executor resolved it from cache), the position
	// has no OCO protection. Retroactively adopt the placed order into an
	// OCO group so that handleEntryUpdate fires on the WS EXECUTED event
	// and places SL/TP legs from the actual fill price.
	if isTrailingSL && p.ocoManager != nil && !order.IsPaperTrade && order.IndiraOrderID != nil {
		var auth *indiraClient.AuthContext
		if order.BearerToken != nil && order.AppId != nil && order.Source != nil {
			auth = &indiraClient.AuthContext{
				UserId:      order.UserID,
				BearerToken: *order.BearerToken,
				AppId:       *order.AppId,
				Source:      *order.Source,
			}
		}
		trailPct := 0.0
		if order.TrailingSLPct != nil {
			trailPct = *order.TrailingSLPct
		}
		p.ocoManager.AdoptOrder(order, signal.StopLossPct, signal.TakeProfitPct, trailPct, auth)
		p.logger.Info("Trailing SL order adopted into OCO after Route 3 placement",
			zap.String("order_id", order.OrderID.String()),
			zap.String("broker_id", *order.IndiraOrderID),
			zap.String("symbol", order.Symbol))
	}

	return nil
}

// logOrderTiming emits a single per-order log with all component timings in milliseconds,
// and records the aggregate Prometheus metrics.
func (p *SignalProcessor) logOrderTiming(signal *models.TradeSignal, route string, kafkaTime, processStart time.Time, idempotencyMs, dbMs float64, err error) {
	totalMs := float64(time.Since(processStart).Microseconds()) / 1000.0

	// Prometheus aggregates
	metrics.SignalRouteDuration.WithLabelValues(route).Observe(time.Since(processStart).Seconds())
	if !kafkaTime.IsZero() {
		metrics.SignalEndToEndDuration.WithLabelValues(route).Observe(time.Since(kafkaTime).Seconds())
	}

	// Per-order timing log — one line per order with all component durations.
	// Search by oid to find exactly how long each order took at each step.
	fields := []zap.Field{
		zap.String("oid", signal.OrderID),
		zap.String("uid", signal.UserID),
		zap.String("sym", signal.Symbol),
		zap.String("route", route),
		zap.Float64("idempotency_ms", idempotencyMs),
		zap.Float64("db_persist_ms", dbMs),
		zap.Float64("total_ms", totalMs),
	}
	if !kafkaTime.IsZero() {
		kafkaDelayMs := float64(processStart.Sub(kafkaTime).Microseconds()) / 1000.0
		e2eMs := float64(time.Since(kafkaTime).Microseconds()) / 1000.0
		fields = append(fields,
			zap.Float64("kafka_delay_ms", kafkaDelayMs),
			zap.Float64("e2e_ms", e2eMs),
		)
	}

	if err != nil {
		fields = append(fields, zap.Error(err))
		p.logger.Error("signal_timing", fields...)
	} else {
		p.logger.Info("signal_timing", fields...)
	}
}



// routeToMultiLevel places the entry order via the regular executor then registers
// the order with the multi-level exit manager. For paper trades the fill is
// immediate, so OnEntryFill is called inline. For live trades the ML manager
// waits for the broker WS EXECUTED event via HandleBrokerUpdate.
func (p *SignalProcessor) routeToMultiLevel(ctx context.Context, order *models.Order, signal *models.TradeSignal) error {
	slMode := signal.StopLossType
	if slMode == "" {
		slMode = "FIXED"
	}
	tpMode := signal.TakeProfitType
	if tpMode == "" {
		tpMode = "FIXED"
	}

	var auth *indiraClient.AuthContext
	if order.BearerToken != nil && order.AppId != nil && order.Source != nil {
		auth = &indiraClient.AuthContext{
			UserId:      order.UserID,
			BearerToken: *order.BearerToken,
			AppId:       *order.AppId,
			Source:      *order.Source,
		}
	}

	fixedSLPct := signal.StopLossPct
	trailingSLPct := signal.TrailingSLPct

	// Register BEFORE placing — so that if paper fill fires the OnPaperFilled
	// callback synchronously, the group is already in the map.
	p.multiLevelManager.RegisterEntry(
		order,
		slMode, tpMode,
		signal.MultiLevelSL, signal.MultiLevelTP,
		fixedSLPct, trailingSLPct,
		auth,
	)

	// Place entry order
	if err := p.executor.ExecuteOrder(ctx, order); err != nil {
		return fmt.Errorf("multi-level entry order execution failed: %w", err)
	}

	// For paper trades: entry is filled immediately — trigger ML fill processing now.
	if order.IsPaperTrade && order.FilledQuantity > 0 && order.FilledPrice != nil {
		fillPrice := *order.FilledPrice
		p.multiLevelManager.OnEntryFill(
			ctx, order.OrderID, fillPrice, order.FilledQuantity,
			signal.MultiLevelSL, signal.MultiLevelTP,
		)
	}

	// For live trades: ML manager's HandleBrokerUpdate fires when broker WS reports EXECUTED.
	// Ensure broker WS subscription is active.
	if !order.IsPaperTrade && p.executor.statusSvc != nil && auth != nil {
		go func() {
			if err := p.executor.statusSvc.StartSubscription(context.Background(), order.UserID, auth); err != nil {
				p.logger.Warn("ml_ws_sub_failed",
					zap.String("uid", order.UserID),
					zap.Error(err))
			}
		}()
	}

	p.logger.Info("Order routed to multi-level manager",
		zap.String("order_id", order.OrderID.String()),
		zap.String("symbol", order.Symbol),
		zap.String("sl_mode", slMode),
		zap.String("tp_mode", tpMode),
		zap.Int("ml_sl_levels", len(signal.MultiLevelSL)),
		zap.Int("ml_tp_levels", len(signal.MultiLevelTP)))

	return nil
}

// routeToOCO creates an OCO entry order for trailing SL signals.
// Instead of placing a broker bracket order, this creates a custom OCO group:
//   - Entry: SL order at the signal's target price (triggers when price reaches target)
//   - After entry fills (WS EXECUTED): OCO manager places SL+TP legs automatically
//   - Trailing monitor adjusts SL as price moves favorably
func (p *SignalProcessor) routeToOCO(ctx context.Context, order *models.Order, signal *models.TradeSignal) error {
	entryTriggerPrice := 0.0
	if order.Price != nil {
		entryTriggerPrice = *order.Price
	}
	if entryTriggerPrice <= 0 {
		return fmt.Errorf("OCO order %s has no entry trigger price", order.OrderID)
	}

	// Use original SL/TP percentages from strategy config (carried via signal).
	// Back-calculating from absolute prices and triggerPrice is wrong because
	// the rules-engine computes SL/TP from limitPrice (trigger + 0.5% buffer),
	// not from triggerPrice — the base price mismatch corrupts the percentages.
	slPct := signal.StopLossPct
	tpPct := signal.TakeProfitPct

	trailingSLPct := 0.0
	if order.TrailingSLPct != nil {
		trailingSLPct = *order.TrailingSLPct
	}

	auth := &indiraClient.AuthContext{
		UserId:      order.UserID,
		BearerToken: *order.BearerToken,
		AppId:       *order.AppId,
		Source:      *order.Source,
	}

	ocoGroup, err := p.ocoManager.CreateOCOEntry(
		ctx,
		order.UserID,
		order.StrategyID,
		order.EventID,
		order.StockCode,
		string(order.Exchange),
		order.Symbol,
		string(order.OrderSide),
		order.Quantity,
		entryTriggerPrice,
		slPct,
		tpPct,
		true, // trailing SL enabled
		trailingSLPct,
		order.ProductType,
		auth,
	)
	if err != nil {
		return fmt.Errorf("failed to create OCO entry: %w", err)
	}

	p.logger.Info("Order routed to OCO manager (trailing SL)",
		zap.String("order_id", order.OrderID.String()),
		zap.String("user_id", order.UserID),
		zap.String("symbol", order.Symbol),
		zap.String("oco_group_id", ocoGroup.GroupID.String()),
		zap.Float64("entry_trigger", entryTriggerPrice),
		zap.Float64("sl_pct", slPct),
		zap.Float64("tp_pct", tpPct),
		zap.Float64("trailing_sl_pct", trailingSLPct))

	// Start broker WS subscription (idempotent) so status updates
	// (EXECUTED, REJECTED, etc.) are received for the entry order.
	// Without this, HandleBrokerUpdate is never called and SL/TP legs
	// are never placed after the entry fills.
	if p.executor.statusSvc != nil {
		go func() {
			if err := p.executor.statusSvc.StartSubscription(context.Background(), order.UserID, auth); err != nil {
				p.logger.Warn("oco_ws_sub_failed",
					zap.String("uid", order.UserID),
					zap.Error(err))
			}
		}()
	}

	return nil
}

// convertSignalToOrder converts a TradeSignal from Kafka to an Order model.
func (p *SignalProcessor) convertSignalToOrder(signal *models.TradeSignal) (*models.Order, error) {
	orderID, err := uuid.Parse(signal.OrderID)
	if err != nil {
		return nil, fmt.Errorf("invalid order_id: %w", err)
	}

	eventID := uuid.Nil
	if signal.EventID != "" {
		if parsed, parseErr := uuid.Parse(signal.EventID); parseErr == nil {
			eventID = parsed
		}
	}

	now := time.Now()

	orderSide := models.OrderSideBuy
	if signal.Sentiment == "BEARISH" || signal.Sentiment == "NEGATIVE" {
		orderSide = models.OrderSideSell
	}

	price := signal.Price
	stopLoss := signal.StopLoss
	takeProfit := signal.TakeProfit
	riskScore := 0.0

	productType := signal.ProductType
	if productType == "" {
		productType = "INTRADAY"
	}
	stopLossType := signal.StopLossType
	if stopLossType == "" {
		stopLossType = "FIXED"
	}

	// Normalize trading mode: treat empty as PAPER (safe default —
	// strategies default to PAPER in user-config; empty here indicates
	// a misconfiguration rather than a deliberate LIVE request).
	tradingMode := signal.TradingMode
	if tradingMode == "" {
		p.logger.Warn("Signal has empty TradingMode — defaulting to PAPER. Check strategy config in rules-engine.",
			zap.String("order_id", signal.OrderID))
		tradingMode = "PAPER"
	}

	return &models.Order{
		OrderID:      orderID,
		UserID:       signal.UserID,
		StrategyID:   signal.StrategyID,
		StrategyName: signal.StrategyName,
		EventID:      eventID,
		StockCode:    signal.StockCode,
		Exchange:     models.Exchange(signal.Exchange),
		Symbol:       signal.Symbol,
		OrderType:    models.OrderType(signal.OrderType),
		OrderSide:    orderSide,
		Quantity:     signal.Quantity,
		Price:        &price,
		StopLoss:     &stopLoss,
		TakeProfit:   &takeProfit,
		Validity:     "DAY",
		ProductType:  productType,
		Status:       models.StatusReceived,
		RiskApproved: true,
		RiskScore:    &riskScore,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,
		BearerToken:  stringPtr(signal.BearerToken),
		AppId:        stringPtr(signal.AppId),
		Source:       stringPtr(signal.Source),
		// Stop loss config
		StopLossType:  &stopLossType,
		TrailingSLPct: float64Ptr(signal.TrailingSLPct),
		// Paper trading
		IsPaperTrade:     tradingMode == "PAPER",
		TradingMode:      tradingMode,
		CurrentPctChange: signal.CurrentPctChange,
		MaxMonitorPrice:  float64Ptr(signal.MaxMonitorPrice),
		// Per-user auto square-off override (e.g. "14:30" IST); nil if not set
		AutoSquareOffTime: stringPtr(signal.AutoSquareOffTime),
	}, nil
}

// stringPtr converts a non-empty string to a pointer; returns nil for empty strings.
func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// float64Ptr returns a pointer to the value, or nil if zero.
func float64Ptr(f float64) *float64 {
	if f == 0 {
		return nil
	}
	return &f
}
