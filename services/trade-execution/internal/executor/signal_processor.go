package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
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
	executor     *OrderExecutor
	orderRepo    repository.OrderRepository
	kafkaPub     *publisher.KafkaPublisher
	priceMonitor *scheduler.PriceMonitor
	logger       *zap.Logger
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

// ProcessTradeSignal processes a trade signal from Kafka — hot path.
// DB persistence is synchronous; Indira API call is critical path.
// WS subscription and Kafka publishing are handled inside OrderExecutor.
func (p *SignalProcessor) ProcessTradeSignal(ctx context.Context, signal *models.TradeSignal) error {
	p.logger.Info("Processing trade signal",
		zap.String("order_id", signal.OrderID),
		zap.String("user_id", signal.UserID),
		zap.String("symbol", signal.Symbol),
		zap.Float64("price", signal.Price),
		zap.String("trading_mode", signal.TradingMode))

	// Idempotency check: skip if this order was already processed.
	// On restart, Kafka may replay uncommitted signals — this prevents duplicate execution.
	orderID, err := uuid.Parse(signal.OrderID)
	if err != nil {
		return fmt.Errorf("invalid order_id in signal: %w", err)
	}
	existing, err := p.orderRepo.GetByID(ctx, orderID)
	if err == nil && existing != nil {
		p.logger.Info("Signal already processed, skipping duplicate",
			zap.String("order_id", signal.OrderID),
			zap.String("existing_status", string(existing.Status)))
		return nil
	}

	// Convert TradeSignal to Order
	order, err := p.convertSignalToOrder(signal)
	if err != nil {
		return fmt.Errorf("failed to convert signal to order: %w", err)
	}

	// Persist order to DB synchronously so subsequent Update calls can find the row.
	if err := p.orderRepo.Create(ctx, order); err != nil {
		p.logger.Error("DB: failed to save order — aborting to prevent duplicate execution",
			zap.String("order_id", order.OrderID.String()),
			zap.Error(err))
		return fmt.Errorf("failed to persist order: %w", err)
	}

	// Route: if this is a below_min order (STOP_LOSS + BRACKET), send to price
	// monitor instead of placing immediately. The monitor watches Redis LTP and
	// places a bracket order when the target price is reached.
	isBelowMin := order.OrderType == models.OrderTypeStopLoss &&
		(order.ProductType == "BRACKET" || order.ProductType == "BRACKET_ORDER" || order.ProductType == "BO")

	if isBelowMin && p.priceMonitor != nil && !order.IsPaperTrade {
		targetPrice := 0.0
		if order.Price != nil {
			targetPrice = *order.Price
		}
		if targetPrice <= 0 {
			return fmt.Errorf("below_min order %s has no target price", order.OrderID)
		}

		p.priceMonitor.Watch(order, targetPrice)

		p.logger.Info("Order routed to price monitor (below_min)",
			zap.String("order_id", signal.OrderID),
			zap.String("user_id", signal.UserID),
			zap.String("symbol", signal.Symbol),
			zap.Float64("target_price", targetPrice))
		return nil
	}

	// Execute via Indira API.
	// OrderExecutor handles: credentials, retries, WS subscription, and Kafka publishing.
	if err := p.executor.ExecuteOrder(ctx, order); err != nil {
		return fmt.Errorf("order execution failed: %w", err)
	}

	p.logger.Info("Order submitted",
		zap.String("order_id", signal.OrderID),
		zap.String("user_id", signal.UserID),
		zap.String("symbol", signal.Symbol))
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
	_ = stopLossType // stored in DB via order fields below

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
		// Paper trading
		IsPaperTrade: tradingMode == "PAPER",
		TradingMode:  tradingMode,
	}, nil
}

// stringPtr converts a non-empty string to a pointer; returns nil for empty strings.
func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
