package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

// OrderExecutorFunc executes an order at the broker. Implemented by executor.OrderExecutor.
type OrderExecutorFunc interface {
	ExecuteOrder(ctx context.Context, order *models.Order) error
}

// AutoSquareOffScheduler manages automatic square-off of positions at market close
type AutoSquareOffScheduler struct {
	orderRepo     repository.OrderRepository
	credsRepo     repository.CredentialsRepository
	orderExecutor OrderExecutorFunc
	squareOffTime string // Format: "15:05" for 3:05 PM
	stopChan      chan struct{}
}

// NewAutoSquareOffScheduler creates a new auto square-off scheduler
func NewAutoSquareOffScheduler(
	orderRepo repository.OrderRepository,
	credsRepo repository.CredentialsRepository,
	orderExecutor OrderExecutorFunc,
	squareOffTime string,
) *AutoSquareOffScheduler {
	if squareOffTime == "" {
		squareOffTime = "15:05" // Default to 3:05 PM
	}

	return &AutoSquareOffScheduler{
		orderRepo:     orderRepo,
		credsRepo:     credsRepo,
		orderExecutor: orderExecutor,
		squareOffTime: squareOffTime,
		stopChan:      make(chan struct{}),
	}
}

// Start starts the auto square-off scheduler
func (s *AutoSquareOffScheduler) Start(ctx context.Context) error {
	log.Printf("Starting Auto Square-Off Scheduler (Time: %s)", s.squareOffTime)

	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Auto Square-Off Scheduler stopped")
			return nil

		case <-s.stopChan:
			log.Println("Auto Square-Off Scheduler stopped")
			return nil

		case <-ticker.C:
			if s.shouldSquareOff() {
				log.Println("Auto Square-Off Time Reached - Initiating square-off for all open positions")
				if err := s.squareOffAllPositions(ctx); err != nil {
					log.Printf("Error during auto square-off: %v", err)
				}
			}
		}
	}
}

// Stop stops the scheduler
func (s *AutoSquareOffScheduler) Stop() {
	close(s.stopChan)
}

// shouldSquareOff checks if current time matches square-off time
func (s *AutoSquareOffScheduler) shouldSquareOff() bool {
	now := time.Now()

	// Only execute on weekdays (Monday-Friday)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	// Parse square-off time
	hour, minute := s.parseTime(s.squareOffTime)
	if hour == -1 || minute == -1 {
		return false
	}

	// Check if current time matches square-off time (with 1-minute window)
	currentHour := now.Hour()
	currentMinute := now.Minute()

	return currentHour == hour && currentMinute == minute
}

// parseTime parses time string in "HH:MM" format
func (s *AutoSquareOffScheduler) parseTime(timeStr string) (hour int, minute int) {
	_, err := fmt.Sscanf(timeStr, "%d:%d", &hour, &minute)
	if err != nil {
		log.Printf("Error parsing time %s: %v", timeStr, err)
		return -1, -1
	}
	return hour, minute
}

// squareOffAllPositions squares off all open positions for all users
func (s *AutoSquareOffScheduler) squareOffAllPositions(ctx context.Context) error {
	log.Println("=== Auto Square-Off: Starting ===")

	// Get all open/partially filled orders
	openOrders, err := s.getOpenOrders(ctx)
	if err != nil {
		return fmt.Errorf("failed to get open orders: %w", err)
	}

	log.Printf("Found %d open orders to square off", len(openOrders))

	successCount := 0
	failCount := 0

	for _, order := range openOrders {
		log.Printf("Squaring off order %s for user %s (Symbol: %s, Qty: %d)",
			order.OrderID, order.UserID, order.Symbol, order.Quantity)

		// Create reverse order (opposite side)
		if err := s.createSquareOffOrder(ctx, order); err != nil {
			log.Printf("Failed to square off order %s: %v", order.OrderID, err)
			failCount++
			continue
		}

		successCount++
	}

	log.Printf("=== Auto Square-Off: Complete (Success: %d, Failed: %d) ===", successCount, failCount)
	return nil
}

// getOpenOrders retrieves all orders that are filled or partially filled
func (s *AutoSquareOffScheduler) getOpenOrders(ctx context.Context) ([]*models.Order, error) {
	// Get all orders with status = FILLED or PARTIALLY_FILLED and product_type = INTRADAY
	return s.orderRepo.GetOpenOrders(ctx)
}

// createSquareOffOrder creates a reverse order to square off a position
func (s *AutoSquareOffScheduler) createSquareOffOrder(ctx context.Context, originalOrder *models.Order) error {
	// Determine reverse order side
	reverseOrderSide := models.OrderSideSell
	if originalOrder.OrderSide == models.OrderSideSell {
		reverseOrderSide = models.OrderSideBuy
	}

	// Create square-off order
	squareOffOrder := &models.Order{
		UserID:       originalOrder.UserID,
		StrategyID:   originalOrder.StrategyID,
		StockCode:    originalOrder.StockCode,
		Exchange:     originalOrder.Exchange,
		Symbol:       originalOrder.Symbol,
		OrderType:    models.OrderTypeMarket, // Always use market order for square-off
		OrderSide:    reverseOrderSide,
		Quantity:     originalOrder.FilledQuantity, // Square off filled quantity
		Validity:     "IOC",                        // Immediate or Cancel
		ProductType:  originalOrder.ProductType,
		Status:       models.StatusReceived,
		BearerToken:  originalOrder.BearerToken,
		AppId:        originalOrder.AppId,
		Source:       originalOrder.Source,
		RiskApproved: true, // Auto square-off bypasses risk checks
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Save order to database
	if err := s.orderRepo.Create(ctx, squareOffOrder); err != nil {
		return fmt.Errorf("failed to create square-off order: %w", err)
	}

	// Execute the square-off order
	if err := s.orderExecutor.ExecuteOrder(ctx, squareOffOrder); err != nil {
		return fmt.Errorf("failed to execute square-off order: %w", err)
	}

	log.Printf("Successfully created and executed square-off order for %s", originalOrder.OrderID)
	return nil
}
