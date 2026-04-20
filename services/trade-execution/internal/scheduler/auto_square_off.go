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

// ist is the Indian Standard Time zone (UTC+5:30).
// All auto square-off times are entered and compared in IST.
var ist = time.FixedZone("IST", 5*60*60+30*60)

// OrderExecutorFunc executes an order at the broker. Implemented by executor.OrderExecutor.
type OrderExecutorFunc interface {
	ExecuteOrder(ctx context.Context, order *models.Order) error
}

// AutoSquareOffScheduler manages automatic square-off of intraday positions
// placed through our algo system at market close (default 15:05 IST).
// Only orders belonging to a strategy in the local orders table are affected —
// positions opened manually on other platforms are untouched.
//
// In addition to the global close time, individual users can request a custom
// auto_square_off_time via their trade signal. At that time the scheduler closes
// ALL open positions (paper + live) for that user only.
type AutoSquareOffScheduler struct {
	orderRepo     repository.OrderRepository
	credsRepo     repository.CredentialsRepository
	orderExecutor OrderExecutorFunc
	squareOffTime      string // Format: "15:05" — live positions (global)
	paperSquareOffTime string // Format: "15:00" — paper positions (global, fires before live)
	stopChan      chan struct{}

	// Guard against re-execution within the same minute / same day.
	mu                   sync.Mutex
	lastExecuteDate      string            // "2006-01-02" — global live guard
	lastPaperExecuteDate string            // "2006-01-02" — global paper guard
	lastUserExecuteDates map[string]string // "userID:HH:MM" → "2006-01-02" — per-user guard

	// paperSquareOff, if set, is called at paperSquareOffTime to close all paper positions (global).
	paperSquareOff func(ctx context.Context) error

	// paperForceExitUser, if set, is called with a specific userID to close that user's paper
	// positions at their custom auto_square_off_time.
	paperForceExitUser func(ctx context.Context, userID string) error
}

// NewAutoSquareOffScheduler creates a new auto square-off scheduler.
func NewAutoSquareOffScheduler(
	orderRepo repository.OrderRepository,
	credsRepo repository.CredentialsRepository,
	orderExecutor OrderExecutorFunc,
	squareOffTime string,
) *AutoSquareOffScheduler {
	if squareOffTime == "" {
		squareOffTime = "15:05" // Default to 3:05 PM IST for live
	}

	return &AutoSquareOffScheduler{
		orderRepo:            orderRepo,
		credsRepo:            credsRepo,
		orderExecutor:        orderExecutor,
		squareOffTime:        squareOffTime,
		paperSquareOffTime:   "15:00", // Paper closes at 3:00 PM IST, before live
		stopChan:             make(chan struct{}),
		lastUserExecuteDates: make(map[string]string),
	}
}

// SetPaperSquareOff registers a callback invoked at paperSquareOffTime (15:00 IST)
// to close all open paper positions. Pass paperMonitor.SquareOffAll to wire it up.
func (s *AutoSquareOffScheduler) SetPaperSquareOff(fn func(ctx context.Context) error) {
	s.paperSquareOff = fn
}

// SetPaperForceExitUser registers a callback used to close a single user's paper positions
// when their custom auto_square_off_time is reached.
// Pass paperMonitor.ForceExitAll to wire it up.
func (s *AutoSquareOffScheduler) SetPaperForceExitUser(fn func(ctx context.Context, userID string) error) {
	s.paperForceExitUser = fn
}

// Start begins the auto square-off check loop (every 1 minute).
// Paper positions close at paperSquareOffTime (15:00); live positions close at squareOffTime (15:05).
func (s *AutoSquareOffScheduler) Start(ctx context.Context) error {
	log.Printf("[auto-square-off] Scheduler started (live: %s IST, paper: %s IST, weekdays only)",
		s.squareOffTime, s.paperSquareOffTime)

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
			// Per-user custom auto square-off times (both paper + live).
			s.checkUserSquareOffs(ctx)

			// Global paper square-off fires independently at its own time (15:00).
			if s.shouldPaperSquareOff() && s.paperSquareOff != nil {
				log.Println("[auto-square-off] ========== PAPER TRIGGER — closing all paper positions at 15:00 ==========")
				if err := s.paperSquareOff(ctx); err != nil {
					log.Printf("[auto-square-off] Paper square-off error: %v", err)
				}
			}
			// Global live square-off fires at its own time (15:05).
			if s.shouldSquareOff() {
				log.Println("[auto-square-off] ========== LIVE TRIGGER — squaring off all open algo positions ==========")
				if err := s.squareOffAllPositions(ctx); err != nil {
					log.Printf("[auto-square-off] Error during live square-off: %v", err)
				}
			}
		}
	}
}

// Stop stops the scheduler gracefully.
func (s *AutoSquareOffScheduler) Stop() {
	close(s.stopChan)
}

// shouldSquareOff returns true when current IST time matches squareOffTime (live) on a
// weekday and we haven't already executed today.
func (s *AutoSquareOffScheduler) shouldSquareOff() bool {
	now := time.Now().In(ist)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}
	hour, minute := s.parseTime(s.squareOffTime)
	if hour == -1 || minute == -1 {
		return false
	}
	if now.Hour() != hour || now.Minute() != minute {
		return false
	}
	today := now.Format("2006-01-02")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastExecuteDate == today {
		return false
	}
	s.lastExecuteDate = today
	return true
}

// shouldPaperSquareOff returns true when current IST time matches paperSquareOffTime on a
// weekday and paper square-off hasn't already run today.
func (s *AutoSquareOffScheduler) shouldPaperSquareOff() bool {
	now := time.Now().In(ist)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}
	hour, minute := s.parseTime(s.paperSquareOffTime)
	if hour == -1 || minute == -1 {
		return false
	}
	if now.Hour() != hour || now.Minute() != minute {
		return false
	}
	today := now.Format("2006-01-02")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastPaperExecuteDate == today {
		return false
	}
	s.lastPaperExecuteDate = today
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

	log.Printf("[auto-square-off] ========== LIVE COMPLETE: %d succeeded, %d failed ==========", successCount, failCount)

	return nil
}

// checkUserSquareOffs queries the DB for users who have a custom auto_square_off_time
// matching the current IST minute and closes all their positions (paper + live).
// Runs every minute tick on weekdays. Guards against firing twice per user per day.
func (s *AutoSquareOffScheduler) checkUserSquareOffs(ctx context.Context) {
	now := time.Now().In(ist)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return
	}
	currentTime := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute())
	today := now.Format("2006-01-02")

	users, err := s.orderRepo.GetUsersWithAutoSquareOffAtTime(ctx, currentTime)
	if err != nil {
		log.Printf("[auto-square-off] Failed to fetch users with custom sq-off time %s: %v", currentTime, err)
		return
	}
	if len(users) == 0 {
		return
	}

	for _, userID := range users {
		// Per-user deduplication: don't fire twice for the same user+time on the same day.
		guardKey := userID + ":" + currentTime
		s.mu.Lock()
		if s.lastUserExecuteDates[guardKey] == today {
			s.mu.Unlock()
			continue
		}
		s.lastUserExecuteDates[guardKey] = today
		s.mu.Unlock()

		log.Printf("[auto-square-off] ===== USER SQUARE-OFF: user=%s time=%s =====", userID, currentTime)

		// Close paper positions for this user.
		if s.paperForceExitUser != nil {
			if err := s.paperForceExitUser(ctx, userID); err != nil {
				log.Printf("[auto-square-off] Paper force-exit failed for user=%s: %v", userID, err)
			}
		}

		// Close live positions for this user.
		if err := s.squareOffUserPositions(ctx, userID); err != nil {
			log.Printf("[auto-square-off] Live square-off failed for user=%s: %v", userID, err)
		}
	}
}

// squareOffUserPositions fetches and closes all open INTRADAY live positions for a single user.
func (s *AutoSquareOffScheduler) squareOffUserPositions(ctx context.Context, userID string) error {
	openOrders, err := s.orderRepo.GetOpenOrdersByUser(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to get open orders for user %s: %w", userID, err)
	}
	if len(openOrders) == 0 {
		return nil
	}

	log.Printf("[auto-square-off] Squaring off %d live position(s) for user=%s", len(openOrders), userID)

	for _, order := range openOrders {
		if order.FilledQuantity <= 0 {
			continue
		}
		if err := s.createAndExecuteSquareOffOrder(ctx, order); err != nil {
			log.Printf("[auto-square-off] FAILED live sq-off order=%s user=%s: %v", order.OrderID, userID, err)
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
