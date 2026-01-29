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
	executor          *OrderExecutor
	orderRepo         repository.OrderRepository
	rabbitPublisher   *publisher.RabbitMQPublisher
	paperTradeHandler *PaperTradeHandler // Add paper trade handler
}

// NewSignalProcessor creates a new trade signal processor
func NewSignalProcessor(
	executor *OrderExecutor,
	orderRepo repository.OrderRepository,
	rabbitPublisher *publisher.RabbitMQPublisher,
	paperTradeHandler *PaperTradeHandler,
) *SignalProcessor {
	return &SignalProcessor{
		executor:          executor,
		orderRepo:         orderRepo,
		rabbitPublisher:   rabbitPublisher,
		paperTradeHandler: paperTradeHandler,
	}
}

// ProcessTradeSignal processes a trade signal from Kafka
func (p *SignalProcessor) ProcessTradeSignal(ctx context.Context, signal *models.TradeSignal) error {
	log.Printf("Processing trade signal: OrderID=%s, UserID=%s, Symbol=%s, Price=%.2f, Mode=%s",
		signal.OrderID, signal.UserID, signal.Symbol, signal.Price, signal.TradingMode)

	// SAFEGUARD: Skip processing if the signal is for PAPER trading.
	// This prevents unintended real orders if rules-engine publishes to Kafka.
	if strings.ToUpper(signal.TradingMode) == "PAPER" {
		log.Printf("⏩ Processing paper trade signal: OrderID=%s (Simulated)", signal.OrderID)

		// Convert signal to order for paper trading
		order, err := p.convertSignalToOrder(signal)
		if err != nil {
			return fmt.Errorf("failed to convert paper signal to order: %w", err)
		}

		// For paper trading, immediately mark order as FILLED since there's no real broker execution
		// Use signal price as the filled price
		order.Status = models.StatusFilled
		order.FilledQuantity = order.Quantity
		order.FilledPrice = order.Price
		now := time.Now()
		order.SubmittedAt = &now
		order.ExecutedAt = &now

		// Save paper order to database
		if err := p.orderRepo.Create(ctx, order); err != nil {
			return fmt.Errorf("failed to save paper order: %w", err)
		}

		log.Printf("✓ Paper order %s saved to database with status FILLED", order.OrderID)

		// Process paper trade (create position, track PnL, etc.)
		if p.paperTradeHandler != nil {
			if err := p.paperTradeHandler.ProcessPaperOrder(ctx, order); err != nil {
				log.Printf("⚠️ Failed to process paper order: %v", err)
				// Don't fail completely, order is still saved
			}
		}

		return nil
	}

	// Convert TradeSignal to Order
	order, err := p.convertSignalToOrder(signal)
	if err != nil {
		return fmt.Errorf("failed to convert signal to order: %w", err)
	}

	// Save order to database
	if err := p.orderRepo.Create(ctx, order); err != nil {
		return fmt.Errorf("failed to save order: %w", err)
	}

	log.Printf("Order %s saved to database with status %s", order.OrderID, order.Status)

	// Publish order to RabbitMQ for odin-api-wrapper to execute
	if p.rabbitPublisher != nil {
		if err := p.rabbitPublisher.PublishOrder(ctx, order); err != nil {
			log.Printf("Failed to publish order %s to RabbitMQ: %v", order.OrderID, err)
			return fmt.Errorf("failed to publish order to RabbitMQ: %w", err)
		}
		log.Printf("✓ Order %s published to RabbitMQ for execution", order.OrderID)
	} else {
		log.Printf("⚠️ RabbitMQ publisher not configured, executing order directly")
		// Fallback to direct execution if RabbitMQ publisher is not configured
		if err := p.executor.ExecuteOrder(ctx, order); err != nil {
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

	// Determine order side based on sentiment or default to BUY
	orderSide := models.OrderSideBuy
	if signal.Sentiment == "BEARISH" || signal.Sentiment == "NEGATIVE" {
		orderSide = models.OrderSideSell
	}

	// Convert float64 values to pointers
	price := signal.Price
	stopLoss := signal.StopLoss
	takeProfit := signal.TakeProfit
	riskScore := 0.0

	// Create Order model
	order := &models.Order{
		OrderID:    orderID,
		UserID:     signal.UserID,
		StrategyID: signal.StrategyID,
		EventID:    eventID,
		StockCode:  signal.StockCode,
		// Token is the actual trading token (scrip token) used by Odin. The
		// rules-engine publishes both StockCode and Token; StockCode is used for
		// analytics while Token is what the broker expects as scrip_token. This
		// ensures we avoid e-101 "Scrip details not found" when placing orders.
		Token:        signal.Token,
		Exchange:     models.Exchange(signal.Exchange),
		Symbol:       signal.Symbol,
		OrderType:    models.OrderType(signal.OrderType),
		OrderSide:    orderSide,
		Quantity:     signal.Quantity,
		Price:        &price,
		StopLoss:     &stopLoss,
		TakeProfit:   &takeProfit,
		Validity:     "DAY", // Default validity
		Status:       models.StatusReceived,
		RiskApproved: true, // Signals from rules-engine are already risk-approved
		RiskScore:    &riskScore,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,
		TradingMode:  signal.TradingMode,
	}

	log.Printf("Converted signal to order: ID=%s, Side=%s, Qty=%d, Price=%.2f",
		order.OrderID, order.OrderSide, order.Quantity, *order.Price)

	return order, nil
}
