package executor

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

// SignalProcessor processes trade signals from Kafka
type SignalProcessor struct {
	executor        *OrderExecutor
	orderRepo       repository.OrderRepository
	rabbitPublisher *publisher.RabbitMQPublisher
	skipDBSave      bool
}

// NewSignalProcessor creates a new trade signal processor
func NewSignalProcessor(executor *OrderExecutor, orderRepo repository.OrderRepository, 
    rabbitPublisher *publisher.RabbitMQPublisher, skipDBSave bool) *SignalProcessor {
    
    return &SignalProcessor{
        executor:        executor,
        orderRepo:       orderRepo,
        rabbitPublisher: rabbitPublisher,
        skipDBSave:      skipDBSave,
    }
}

// ProcessTradeSignal processes a trade signal from Kafka
func (p *SignalProcessor) ProcessTradeSignal(ctx context.Context, signal *models.TradeSignal) error {
    log.Printf("Processing trade signal: OrderID=%s, UserID=%s, Symbol=%s, Price=%.2f",
        signal.OrderID, signal.UserID, signal.Symbol, signal.Price)
    // Convert TradeSignal to Order
    order, err := p.convertSignalToOrder(signal)
    if err != nil {
        return fmt.Errorf("failed to convert signal to order: %w", err)
    }
    if !p.skipDBSave {
        // Save order to database if not skipping
        if err := p.orderRepo.Create(ctx, order); err != nil {
            return fmt.Errorf("failed to save order: %w", err)
        }
        log.Printf("Order %s saved to database with status %s", order.OrderID, order.Status)
    } else {
        log.Printf("Skipping database save for order %s", order.OrderID)
    }
    // If skipDBSave is true, execute directly without RabbitMQ
    if p.skipDBSave {
        log.Printf("Executing order %s directly (skipDBSave=true)", order.OrderID)
        if err := p.executor.ExecuteOrderDirectly(ctx, order); err != nil {
            log.Printf("Failed to execute order %s: %v", order.OrderID, err)
            return fmt.Errorf("failed to execute order: %w", err)
        }
    } else if p.rabbitPublisher != nil {
        // Publish to RabbitMQ if configured and not skipping DB save
        if err := p.rabbitPublisher.PublishOrder(ctx, order); err != nil {
            log.Printf("Failed to publish order %s to RabbitMQ: %v", order.OrderID, err)
            return fmt.Errorf("failed to publish order to RabbitMQ: %w", err)
        }
        log.Printf("✓ Order %s published to RabbitMQ for execution", order.OrderID)
    } else {
        // Fallback to direct execution if no RabbitMQ
        log.Printf("Executing order %s directly (no RabbitMQ)", order.OrderID)
        if err := p.executor.ExecuteOrderDirectly(ctx, order); err != nil {
            log.Printf("Failed to execute order %s: %v", order.OrderID, err)
            return fmt.Errorf("failed to execute order: %w", err)
        }
    }
    log.Printf("✓ Successfully processed and executed trade signal: OrderID=%s, Symbol=%s",
        signal.OrderID, signal.Symbol)
    return nil
}

// convertSignalToOrder converts a TradeSignal from Kafka to an Order model
func (p *SignalProcessor) convertSignalToOrder(signal *models.TradeSignal) (*models.Order, error) {
	// Parse UUIDs
	orderID, err := uuid.Parse(signal.OrderID)
	if err != nil {
		return nil, fmt.Errorf("invalid order_id: %w", err)
	}

	eventID := uuid.Nil
	if signal.EventID != "" {
		eventID, err = uuid.Parse(signal.EventID)
		if err != nil {
			log.Printf("Warning: invalid event_id %s, using nil UUID", signal.EventID)
			eventID = uuid.Nil
		}
	}

	now := time.Now()

	// Default order side to BUY (market depth algos can override via strategy config)
	orderSide := models.OrderSideBuy

	// Convert float64 values to pointers
	price := signal.Price
	stopLoss := signal.StopLoss
	takeProfit := signal.TakeProfit
	riskScore := 0.0

	// Default product type to INTRADAY if not specified
	productType := signal.ProductType
	if productType == "" {
		productType = "INTRADAY"
	}
	// Normalize product type - ensure it fits VARCHAR(20)
	if len(productType) > 20 {
		productType = productType[:20]
	}

	// Default stop loss type to FIXED if not specified
	stopLossType := signal.StopLossType
	if stopLossType == "" {
		stopLossType = "FIXED"
	}
	// Normalize stop loss type - remove prefixes if present
	if strings.HasPrefix(stopLossType, "STOP_LOSS_TYPE_") {
		stopLossType = strings.TrimPrefix(stopLossType, "STOP_LOSS_TYPE_")
	}

	// Normalize exchange to remove "EXCHANGE_" prefix if present
	exchange := signal.Exchange
	if strings.HasPrefix(exchange, "EXCHANGE_") {
		exchange = strings.TrimPrefix(exchange, "EXCHANGE_")
	}

	// Normalize order type to ensure consistency
	orderType := signal.OrderType
	if strings.HasPrefix(orderType, "ORDER_TYPE_") {
		orderType = strings.TrimPrefix(orderType, "ORDER_TYPE_")
	}
	if orderType == "" {
		orderType = "MARKET"
	}

	// Create Order model
	order := &models.Order{
		OrderID:      orderID,
		UserID:       signal.UserID,
		StrategyID:   signal.StrategyID,
		EventID:      eventID,
		StockCode:    signal.StockCode,
		Exchange:     models.Exchange(exchange),
		Symbol:       signal.Symbol,
		OrderType:    models.OrderType(orderType),
		OrderSide:    orderSide,
		Quantity:     signal.Quantity,
		Price:        &price,
		StopLoss:     &stopLoss,
		TakeProfit:   &takeProfit,
		Validity:     "DAY", // Default validity
		ProductType:  productType,
		Status:       models.StatusReceived,
		RiskApproved: true, // Signals from rules-engine are already risk-approved
		RiskScore:    &riskScore,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,

		// Authentication data from signal (originally from strategy)
		BearerToken: stringPtr(signal.BearerToken),
		AppId:       stringPtr(signal.AppId),
		Source:      stringPtr(signal.Source),
	}

	log.Printf("Converted signal to order: ID=%s, Side=%s, Qty=%d, Price=%.2f, Auth=%v",
		order.OrderID, order.OrderSide, order.Quantity, *order.Price,
		signal.BearerToken != "" && signal.AppId != "" && signal.Source != "")

	return order, nil
}

// stringPtr converts a string to a pointer
func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
