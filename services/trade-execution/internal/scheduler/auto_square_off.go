package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

// OrderExecutorFunc executes an order at the broker. Implemented by executor.OrderExecutor.
type OrderExecutorFunc interface {
	ExecuteOrder(ctx context.Context, order *models.Order) error
}

// AutoSquareOffScheduler manages automatic square-off of intraday positions
// placed through our algo system at market close (default 15:05 IST).
// Only orders belonging to a strategy in the local orders table are affected —
// positions opened manually on other platforms are untouched.
type AutoSquareOffScheduler struct {
	orderRepo     repository.OrderRepository
	credsRepo     repository.CredentialsRepository
	orderExecutor OrderExecutorFunc
	squareOffTime string // Format: "15:05" for 3:05 PM
	stopChan      chan struct{}

	// Guard against re-execution within the same minute / same day.
	mu              sync.Mutex
	lastExecuteDate string // "2006-01-02"

	// paperSquareOff, if set, is called alongside the live square-off to close paper positions.
	paperSquareOff func(ctx context.Context) error
}

// NewAutoSquareOffScheduler creates a new auto square-off scheduler.
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

// SetPaperSquareOff registers a callback that is invoked during square-off to close
// all open paper positions. Pass paperMonitor.SquareOffAll to wire it up.
func (s *AutoSquareOffScheduler) SetPaperSquareOff(fn func(ctx context.Context) error) {
	s.paperSquareOff = fn
}

// Start begins the auto square-off check loop (every 1 minute).
func (s *AutoSquareOffScheduler) Start(ctx context.Context) error {
	log.Printf("[auto-square-off] Scheduler started (trigger time: %s IST, weekdays only)", s.squareOffTime)

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[auto-square-off] Scheduler stopped (context cancelled)")
			return nil

		case <-s.stopChan:
			log.Println("[auto-square-off] Scheduler stopped")
			return nil

		case <-ticker.C:
			if s.shouldSquareOff() {
				log.Println("[auto-square-off] ========== TRIGGER — squaring off all open algo positions ==========")
				if err := s.squareOffAllPositions(ctx); err != nil {
					log.Printf("[auto-square-off] Error during square-off: %v", err)
				}
			}
		}
	}
}

// Stop stops the scheduler gracefully.
func (s *AutoSquareOffScheduler) Stop() {
	close(s.stopChan)
}

// shouldSquareOff returns true when current time matches squareOffTime on a
// weekday and we haven't already executed today.
func (s *AutoSquareOffScheduler) shouldSquareOff() bool {
	now := time.Now()

	// Only execute on weekdays (Monday-Friday)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	// Parse configured square-off time
	hour, minute := s.parseTime(s.squareOffTime)
	if hour == -1 || minute == -1 {
		return false
	}

	// Check if current HH:MM matches
	if now.Hour() != hour || now.Minute() != minute {
		return false
	}

	// Prevent re-execution if we already ran today
	today := now.Format("2006-01-02")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastExecuteDate == today {
		return false
	}
	s.lastExecuteDate = today
	return true
}

// parseTime parses time string in "HH:MM" format.
func (s *AutoSquareOffScheduler) parseTime(timeStr string) (hour int, minute int) {
	_, err := fmt.Sscanf(timeStr, "%d:%d", &hour, &minute)
	if err != nil {
		log.Printf("[auto-square-off] Error parsing time %q: %v", timeStr, err)
		return -1, -1
	}
	return hour, minute
}

// squareOffAllPositions fetches today's open algo positions and places
// reverse MARKET/IOC orders to close each one.
func (s *AutoSquareOffScheduler) squareOffAllPositions(ctx context.Context) error {
	log.Println("[auto-square-off] Fetching today's open INTRADAY algo positions...")

	// GetOpenOrders returns FILLED/PARTIALLY_FILLED INTRADAY live orders
	// placed today through strategies (is_square_off_order=false, is_paper_trade=false,
	// strategy_id != '', created_at >= today).
	openOrders, err := s.orderRepo.GetOpenOrders(ctx)
	if err != nil {
		return fmt.Errorf("failed to get open orders: %w", err)
	}

	if len(openOrders) == 0 {
		log.Println("[auto-square-off] No open positions to square off")
		return nil
	}

	log.Printf("[auto-square-off] Found %d open position(s) to square off", len(openOrders))

	successCount := 0
	failCount := 0

	for _, order := range openOrders {
		// Skip orders with zero filled quantity — nothing to reverse
		if order.FilledQuantity <= 0 {
			log.Printf("[auto-square-off] Skipping order %s (filled_qty=0)", order.OrderID)
			continue
		}

		log.Printf("[auto-square-off] Squaring off: user=%s strategy=%s symbol=%s side=%s filled_qty=%d",
			order.UserID, order.StrategyID, order.Symbol, order.OrderSide, order.FilledQuantity)

		if err := s.createAndExecuteSquareOffOrder(ctx, order); err != nil {
			log.Printf("[auto-square-off] FAILED order %s: %v", order.OrderID, err)
			failCount++
			continue
		}

		successCount++
	}

	log.Printf("[auto-square-off] ========== COMPLETE: %d succeeded, %d failed ==========", successCount, failCount)

	// Square off paper positions alongside live positions.
	if s.paperSquareOff != nil {
		log.Println("[auto-square-off] Closing paper positions...")
		if err := s.paperSquareOff(ctx); err != nil {
			log.Printf("[auto-square-off] Paper square-off error (non-fatal): %v", err)
		}
	}

	return nil
}

// createAndExecuteSquareOffOrder creates a reverse order to close a position
// and executes it via the broker.
func (s *AutoSquareOffScheduler) createAndExecuteSquareOffOrder(ctx context.Context, originalOrder *models.Order) error {
	// Determine reverse side
	reverseSide := models.OrderSideSell
	if originalOrder.OrderSide == models.OrderSideSell {
		reverseSide = models.OrderSideBuy
	}

	squareOffOrder := &models.Order{
		OrderID:          uuid.New(),
		EventID:          uuid.New(),
		UserID:           originalOrder.UserID,
		StrategyID:       originalOrder.StrategyID,
		StrategyName:     originalOrder.StrategyName,
		StockCode:        originalOrder.StockCode,
		Exchange:         originalOrder.Exchange,
		Symbol:           originalOrder.Symbol,
		OrderType:        models.OrderTypeMarket, // MARKET for guaranteed execution
		OrderSide:        reverseSide,
		Quantity:         originalOrder.FilledQuantity, // only exit the filled portion
		Validity:         "IOC",                        // Immediate or Cancel
		ProductType:      originalOrder.ProductType,
		Status:           models.StatusReceived,
		IsSquareOffOrder: true,  // mark so it won't be picked up again
		IsPaperTrade:     false, // live order
		TradingMode:      "LIVE",
		RiskApproved:     true, // auto square-off bypasses risk checks
		BearerToken:      originalOrder.BearerToken,
		AppId:            originalOrder.AppId,
		Source:           originalOrder.Source,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}

	// Persist to DB first
	if err := s.orderRepo.Create(ctx, squareOffOrder); err != nil {
		return fmt.Errorf("failed to save square-off order: %w", err)
	}

	// Execute via broker (executor handles credential lookup + retries)
	if err := s.orderExecutor.ExecuteOrder(ctx, squareOffOrder); err != nil {
		return fmt.Errorf("failed to execute square-off order: %w", err)
	}

	log.Printf("[auto-square-off] OK — placed %s %s %d qty for user %s (sq_off_id=%s, original=%s)",
		reverseSide, originalOrder.Symbol, originalOrder.FilledQuantity,
		originalOrder.UserID, squareOffOrder.OrderID, originalOrder.OrderID)

	return nil
}
