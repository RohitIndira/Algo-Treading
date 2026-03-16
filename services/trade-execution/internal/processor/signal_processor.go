package processor

import (
	"context"
	"fmt"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/statusservice"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// BrokerExecutor executes orders with the broker
type BrokerExecutor interface {
	ExecuteOrder(ctx context.Context, signal *models.TradeSignal) (*BrokerExecutionResult, error)
}

// BrokerExecutionResult contains broker response
type BrokerExecutionResult struct {
	BrokerOrderID   string
	Status          string
	ExecutedPrice   float64
	ExecutedQty     int32
	Brokerage       float64
	ExchangeCharges float64
	Timestamp       time.Time
	// Auth for WebSocket subscription (only set on successful placement)
	UserID      string
	Auth        *indiraClient.AuthContext
}

// SignalProcessor processes trade signals and publishes results
type SignalProcessor struct {
	broker        BrokerExecutor
	publisher     *publisher.KafkaPublisher
	statusService *statusservice.OrderStatusService
	logger        *zap.Logger
}

// NewSignalProcessor creates a new signal processor
func NewSignalProcessor(broker BrokerExecutor, publisher *publisher.KafkaPublisher, statusService *statusservice.OrderStatusService, logger *zap.Logger) *SignalProcessor {
	return &SignalProcessor{
		broker:        broker,
		publisher:     publisher,
		statusService: statusService,
		logger:        logger,
	}
}

// ProcessTradeSignal processes a trade signal end-to-end
func (p *SignalProcessor) ProcessTradeSignal(ctx context.Context, signal *models.TradeSignal) error {
	p.logger.Info("Processing trade signal",
		zap.String("order_id", signal.OrderID),
		zap.String("symbol", signal.Symbol),
		zap.Float64("price", signal.Price))

	// Execute order with broker
	brokerResult, err := p.broker.ExecuteOrder(ctx, signal)
	if err != nil {
		return p.handleExecutionFailure(ctx, signal, err)
	}

	// Handle successful execution
	return p.handleExecutionSuccess(ctx, signal, brokerResult)
}

// handleExecutionSuccess handles successful order execution
func (p *SignalProcessor) handleExecutionSuccess(ctx context.Context, signal *models.TradeSignal, brokerResult *BrokerExecutionResult) error {
	// Start WebSocket subscription for real-time status updates (per user, idempotent)
	if p.statusService != nil && brokerResult.Auth != nil {
		if err := p.statusService.StartSubscription(ctx, signal.UserID, brokerResult.Auth); err != nil {
			p.logger.Warn("Failed to start WS subscription for user (order still placed)",
				zap.String("user_id", signal.UserID),
				zap.Error(err))
		}
	}

	executionID := uuid.New().String()

	// Calculate financial details
	totalValue := float64(brokerResult.ExecutedQty) * brokerResult.ExecutedPrice
	gst := (brokerResult.Brokerage + brokerResult.ExchangeCharges) * 0.18
	stt := totalValue * 0.001         // 0.1% STT
	stampDuty := totalValue * 0.00003 // 0.003%
	totalCharges := brokerResult.Brokerage + brokerResult.ExchangeCharges + gst + stt + stampDuty
	netAmount := totalValue + totalCharges

	// Create execution result
	executionResult := &models.ExecutionResult{
		ExecutionID:       executionID,
		OrderID:           signal.OrderID,
		UserID:            signal.UserID,
		StrategyID:        signal.StrategyID,
		StrategyName:      signal.StrategyName,
		BrokerOrderID:     brokerResult.BrokerOrderID,
		BrokerName:        "ODIN_TRADING",
		Status:            "EXECUTED",
		StatusMessage:     "Order executed successfully",
		StockCode:         signal.StockCode,
		Symbol:            signal.Symbol,
		Exchange:          signal.Exchange,
		OrderType:         signal.OrderType,
		OrderSide:         "BUY",
		RequestedQuantity: signal.Quantity,
		RequestedPrice:    signal.Price,
		ExecutedQuantity:  brokerResult.ExecutedQty,
		ExecutedPrice:     brokerResult.ExecutedPrice,
		AveragePrice:      brokerResult.ExecutedPrice,
		TotalValue:        totalValue,
		Brokerage:         brokerResult.Brokerage,
		ExchangeCharges:   brokerResult.ExchangeCharges,
		GST:               gst,
		STT:               stt,
		StampDuty:         stampDuty,
		TotalCharges:      totalCharges,
		NetAmount:         netAmount,
		OrderPlacedAt:     signal.Timestamp,
		ExecutionTime:     brokerResult.Timestamp,
		BrokerTimestamp:   brokerResult.Timestamp,
		EventID:           signal.EventID,
		NewsCategory:      signal.NewsCategory,
		ImpactScore:       signal.ImpactScore,
		MatchScore:        signal.MatchScore,
	}

	// Publish both results concurrently in background — don't block the response.
	orderUpdate := p.createOrderUpdate(signal, executionResult, true)
	go func() {
		if err := p.publisher.PublishExecutionResult(ctx, executionResult); err != nil {
			p.logger.Error("Failed to publish execution result", zap.Error(err))
		}
	}()
	go func() {
		if err := p.publisher.PublishOrderUpdate(ctx, orderUpdate); err != nil {
			p.logger.Error("Failed to publish order update", zap.Error(err))
		}
	}()

	p.logger.Info("Trade signal processed successfully",
		zap.String("order_id", signal.OrderID),
		zap.String("execution_id", executionID),
		zap.String("broker_order_id", brokerResult.BrokerOrderID),
		zap.Float64("executed_price", brokerResult.ExecutedPrice))

	return nil
}

// handleExecutionFailure handles failed order execution
func (p *SignalProcessor) handleExecutionFailure(ctx context.Context, signal *models.TradeSignal, execError error) error {
	executionID := uuid.New().String()

	// Create failure execution result
	executionResult := &models.ExecutionResult{
		ExecutionID:       executionID,
		OrderID:           signal.OrderID,
		UserID:            signal.UserID,
		StrategyID:        signal.StrategyID,
		StrategyName:      signal.StrategyName,
		Status:            "FAILED",
		StatusMessage:     "Order execution failed",
		StockCode:         signal.StockCode,
		Symbol:            signal.Symbol,
		Exchange:          signal.Exchange,
		OrderType:         signal.OrderType,
		OrderSide:         "BUY",
		RequestedQuantity: signal.Quantity,
		RequestedPrice:    signal.Price,
		ExecutedQuantity:  0,
		ExecutedPrice:     0,
		OrderPlacedAt:     signal.Timestamp,
		ExecutionTime:     time.Now(),
		EventID:           signal.EventID,
		ErrorMessage:      execError.Error(),
	}

	// Publish both failure results in background — don't block the response.
	orderUpdate := p.createOrderUpdate(signal, executionResult, false)
	go func() {
		if err := p.publisher.PublishExecutionResult(ctx, executionResult); err != nil {
			p.logger.Error("Failed to publish failure result", zap.Error(err))
		}
	}()
	go func() {
		if err := p.publisher.PublishOrderUpdate(ctx, orderUpdate); err != nil {
			p.logger.Error("Failed to publish failure update", zap.Error(err))
		}
	}()

	p.logger.Error("Trade signal execution failed",
		zap.String("order_id", signal.OrderID),
		zap.Error(execError))

	return fmt.Errorf("execution failed: %w", execError)
}

// createOrderUpdate creates a user-friendly order update
func (p *SignalProcessor) createOrderUpdate(signal *models.TradeSignal, result *models.ExecutionResult, success bool) *models.OrderUpdate {
	updateID := uuid.New().String()
	now := time.Now()

	update := &models.OrderUpdate{
		UpdateID:  updateID,
		OrderID:   signal.OrderID,
		UserID:    signal.UserID,
		CreatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
		OrderSummary: models.OrderSummary{
			Stock:     fmt.Sprintf("%s (%s)", signal.Symbol, signal.Symbol),
			Action:    "BUY",
			Quantity:  signal.Quantity,
			Price:     fmt.Sprintf("₹%.2f", signal.Price),
			Exchange:  signal.Exchange,
			OrderType: signal.OrderType,
		},
		Strategy: models.StrategyContext{
			ID:      signal.StrategyID,
			Name:    signal.StrategyName,
			Trigger: fmt.Sprintf("%s news detected", signal.NewsCategory),
		},
		Actions: []models.ActionItem{
			{
				ActionType: "VIEW_POSITION",
				Label:      "View Position",
				URL:        fmt.Sprintf("/portfolio/positions/%s", signal.Symbol),
			},
			{
				ActionType: "VIEW_ORDER_HISTORY",
				Label:      "Order Details",
				URL:        fmt.Sprintf("/orders/%s", signal.OrderID),
			},
		},
		NotificationChannels: models.NotificationChannels{
			Push:  true,
			Email: true,
			SMS:   false,
			InApp: true,
		},
	}

	if success {
		update.UpdateType = "EXECUTION_SUCCESS"
		update.Priority = "HIGH"
		update.Title = "Order Executed ✓"
		update.Message = fmt.Sprintf("Your %s buy order executed at ₹%.2f", signal.Symbol, result.ExecutedPrice)
		update.DetailedMessage = fmt.Sprintf("Successfully bought %d share(s) of %s at ₹%.2f on %s. Total amount: ₹%.2f (including charges)",
			result.ExecutedQuantity, signal.Symbol, result.ExecutedPrice, signal.Exchange, result.NetAmount)
		update.Status = "EXECUTED"
		update.StatusEmoji = "✅"
		update.StatusColor = "#00C851"
		update.ExecutionDetails = &models.ExecutionDetails{
			ExecutedAt:    result.ExecutionTime,
			ExecutedPrice: fmt.Sprintf("₹%.2f", result.ExecutedPrice),
			TotalAmount:   fmt.Sprintf("₹%.2f", result.NetAmount),
			Charges:       fmt.Sprintf("₹%.2f", result.TotalCharges),
			BrokerRef:     result.BrokerOrderID,
		}
	} else {
		update.UpdateType = "EXECUTION_FAILED"
		update.Priority = "HIGH"
		update.Title = "Order Failed ✗"
		update.Message = fmt.Sprintf("Your %s buy order failed", signal.Symbol)
		update.DetailedMessage = fmt.Sprintf("Failed to execute order for %s: %s", signal.Symbol, result.ErrorMessage)
		update.Status = "FAILED"
		update.StatusEmoji = "❌"
		update.StatusColor = "#FF4444"
	}

	return update
}
