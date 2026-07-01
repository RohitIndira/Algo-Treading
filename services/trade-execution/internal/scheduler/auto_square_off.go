package scheduler

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/timezone"
)

// roundTwoDP rounds a price to two decimal places (NSE tick-size rounding is handled
// later by the Indira client; this just prevents floating-point noise in log output).
func roundTwoDP(v float64) float64 { return math.Round(v*100) / 100 }

// OrderExecutorFunc executes an order at the broker. Implemented by executor.OrderExecutor.
type OrderExecutorFunc interface {
	ExecuteOrder(ctx context.Context, order *models.Order) error
}

// LTPLookup fetches the last-traded price for an instrument from Redis market data.
// Implemented by paper.RedisPriceClient (structurally typed — no import needed).
type LTPLookup interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

// sqOffSlippagePct is the limit-price buffer for auto square-off SL-L orders.
// 1.5% gives a wide enough band to guarantee near-instant fill (IOC cancels any remainder).
const sqOffSlippagePct = 0.015

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
	// positions at their custom auto_square_off_time (UI-level override — all strategies).
	paperForceExitUser func(ctx context.Context, userID string) error

	// paperForceExitStrategy, if set, closes paper positions for a single (user, strategy)
	// pair at that strategy's auto_square_off_time. More targeted than paperForceExitUser.
	paperForceExitStrategy func(ctx context.Context, userID, strategyID string) error

	// ltpLookup, if set, fetches the current LTP from Redis to compute trigger and
	// limit prices for SL-L square-off orders. Nil-safe: falls back to 0/0 prices
	// (broker will reject, preventing an accidental plain-market order).
	ltpLookup LTPLookup

	// positionChecker, if set, queries the broker position book before placing a
	// square-off order and skips symbols whose NetQty is already 0. Nil-safe.
	positionChecker *PositionChecker

	// cancelProtectiveLegs, if set, cancels the broker SL/TP (OCO) and multi-level
	// exit legs for one (user, symbol) right before its reverse square-off order is
	// placed — so a resting stop can't fire into a now-flat book and open a fresh
	// position. Nil-safe. Wired in main.go to OCO + ML CancelGroupsBySymbol.
	cancelProtectiveLegs func(ctx context.Context, userID, symbol string)

	// onMarketClose, if set, is called once after the global live square-off
	// completes. Used to clear the broker-WS idle-sweep protection set so
	// post-market idle sweeps can close connections with no remaining exposure.
	onMarketClose func()
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
// when their custom auto_square_off_time is reached (UI-level override — all strategies).
// Pass paperMonitor.ForceExitAll to wire it up.
func (s *AutoSquareOffScheduler) SetPaperForceExitUser(fn func(ctx context.Context, userID string) error) {
	s.paperForceExitUser = fn
}

// SetPaperForceExitStrategy registers a callback used to close paper positions for a
// single (user, strategy) pair at that strategy's auto_square_off_time.
// Pass paperMonitor.ForceExitByStrategy to wire it up.
func (s *AutoSquareOffScheduler) SetPaperForceExitStrategy(fn func(ctx context.Context, userID, strategyID string) error) {
	s.paperForceExitStrategy = fn
}

// SetLTPLookup wires a Redis LTP source so square-off orders are placed as
// SL-L at trigger=LTP±0.1%, limit=LTP±1.5% instead of plain MARKET orders.
func (s *AutoSquareOffScheduler) SetLTPLookup(l LTPLookup) {
	s.ltpLookup = l
}

// SetPositionChecker wires the broker position-book checker so square-off skips
// symbols already flat (NetQty == 0) at the broker. Without it, every live
// square-off proceeds based on local DB state alone (legacy behaviour).
func (s *AutoSquareOffScheduler) SetPositionChecker(pc *PositionChecker) {
	s.positionChecker = pc
}

// SetProtectiveLegCanceller wires the OCO/ML SL-TP canceller invoked for each
// symbol immediately before its reverse square-off order is placed, so protective
// legs are removed from the exchange before the position goes flat. Nil-safe;
// call before Start().
func (s *AutoSquareOffScheduler) SetProtectiveLegCanceller(fn func(ctx context.Context, userID, symbol string)) {
	s.cancelProtectiveLegs = fn
}

// SetOnMarketClose wires a callback invoked once after the global live square-off
// fires at market close. Wired in main.go to statusService.UnmarkAllActiveStrategyUsers
// so subsequent idle sweeps can close connections with no remaining exposure.
func (s *AutoSquareOffScheduler) SetOnMarketClose(fn func()) {
	s.onMarketClose = fn
}

// Start begins the auto square-off check loop (every 1 minute).
// Paper positions close at paperSquareOffTime (15:00); live positions close at squareOffTime (15:05).
func (s *AutoSquareOffScheduler) Start(ctx context.Context) error {
	log.Printf("[auto-square-off] Scheduler started (live: %s IST, paper: %s IST, weekdays only)",
		s.squareOffTime, s.paperSquareOffTime)

	// Fire immediately for any square-off windows missed while the service was down.
	s.runStartupCatchUp(ctx)

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
			// Strategy-level square-off: closes only the matching strategy's positions
			// (paper + live) when orders.auto_square_off_time matches the current minute.
			s.checkStrategySquareOffs(ctx)

			// Per-user custom auto square-off times (UI-level override, all strategies).
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
				// Clear the broker-WS protection set so post-market idle sweeps
				// can close connections for users with no remaining exposure.
				if s.onMarketClose != nil {
					s.onMarketClose()
				}
			}
		}
	}
}

// Stop stops the scheduler gracefully.
func (s *AutoSquareOffScheduler) Stop() {
	close(s.stopChan)
}

// marketCloseMinutes is the hard cutoff (15:30 IST) after which live orders cannot be placed.
// NSE closes the continuous session at 15:30; any order sent after this becomes an AMO
// (After Market Order) and executes the next morning at open — which is never what we want.
const marketCloseMinutes = 15*60 + 30 // 15:30 IST

// runStartupCatchUp fires square-off for any windows that were missed while
// the service was down (e.g. restarted after 15:05). Safe to call multiple
// times — the lastExecuteDate guards prevent double-execution.
func (s *AutoSquareOffScheduler) runStartupCatchUp(ctx context.Context) {
	now := time.Now().In(timezone.IST)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return
	}
	today := now.Format("2006-01-02")
	currentMinutes := now.Hour()*60 + now.Minute()

	// Paper catch-up — paper is simulated so no AMO risk; always safe to fire.
	ph, pm := s.parseTime(s.paperSquareOffTime)
	if ph != -1 && currentMinutes > ph*60+pm && s.paperSquareOff != nil {
		s.mu.Lock()
		alreadyRan := s.lastPaperExecuteDate == today
		if !alreadyRan {
			s.lastPaperExecuteDate = today
		}
		s.mu.Unlock()
		if !alreadyRan {
			log.Printf("[auto-square-off] Startup catch-up: missed paper square-off at %s — firing now", s.paperSquareOffTime)
			if err := s.paperSquareOff(ctx); err != nil {
				log.Printf("[auto-square-off] Startup paper catch-up error: %v", err)
			}
		}
	}

	// Live catch-up — ONLY if we are still before market close (15:30 IST).
	// After 15:30 the exchange has closed all intraday positions already; placing any
	// exit order after this point creates an AMO that executes the next morning.
	lh, lm := s.parseTime(s.squareOffTime)
	scheduledMinutes := lh*60 + lm
	if lh != -1 && currentMinutes > scheduledMinutes {
		if currentMinutes < marketCloseMinutes {
			// Still within market hours — safe to place exit orders.
			s.mu.Lock()
			alreadyRan := s.lastExecuteDate == today
			if !alreadyRan {
				s.lastExecuteDate = today
			}
			s.mu.Unlock()
			if !alreadyRan {
				log.Printf("[auto-square-off] Startup catch-up: missed live square-off at %s — firing now", s.squareOffTime)
				if err := s.squareOffAllPositions(ctx); err != nil {
					log.Printf("[auto-square-off] Startup live catch-up error: %v", err)
				}
			}
		} else {
			// Past market close — mark today done WITHOUT placing orders to prevent AMOs.
			s.mu.Lock()
			if s.lastExecuteDate != today {
				s.lastExecuteDate = today
				log.Printf("[auto-square-off] Startup: market already closed (now %02d:%02d IST) — skipping live catch-up to prevent AMOs",
					now.Hour(), now.Minute())
			}
			s.mu.Unlock()
		}
	}

	// Strategy-level custom time catch-up (orders.auto_square_off_time).
	s.runStartupStrategyCatchUp(ctx, now, today, currentMinutes)

	// Per-user custom time catch-up (user_square_off_config, UI-level override).
	s.runStartupUserCatchUp(ctx, now, today, currentMinutes)
}

// runStartupUserCatchUp closes positions for users whose custom square-off time
// has already passed today (e.g. service was down during their configured time).
// Live orders are only placed if we are still before market close (15:30 IST).
func (s *AutoSquareOffScheduler) runStartupUserCatchUp(ctx context.Context, now time.Time, today string, currentMinutes int) {
	currentTimeStr := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute())

	users, err := s.orderRepo.GetUsersWithExpiredAutoSquareOff(ctx, currentTimeStr)
	if err != nil {
		log.Printf("[auto-square-off] Startup user catch-up: failed to fetch users: %v", err)
		return
	}
	if len(users) == 0 {
		return
	}

	log.Printf("[auto-square-off] Startup user catch-up: %d user(s) with custom time already passed", len(users))

	// If we're past market close, mark all users done without placing orders.
	if currentMinutes >= marketCloseMinutes {
		log.Printf("[auto-square-off] Startup user catch-up: market already closed (now %02d:%02d IST) — marking users done without placing orders to prevent AMOs",
			now.Hour(), now.Minute())
		s.mu.Lock()
		for _, userID := range users {
			guardKey := userID + ":startup-catchup"
			s.lastUserExecuteDates[guardKey] = today
		}
		s.mu.Unlock()
		return
	}

	for _, userID := range users {
		// Use a distinct guard key so this doesn't collide with the per-minute tick guards.
		guardKey := userID + ":startup-catchup"
		s.mu.Lock()
		if s.lastUserExecuteDates[guardKey] == today {
			s.mu.Unlock()
			continue
		}
		s.lastUserExecuteDates[guardKey] = today
		s.mu.Unlock()

		log.Printf("[auto-square-off] Startup catch-up: closing positions for user=%s", userID)

		if s.paperForceExitUser != nil {
			if err := s.paperForceExitUser(ctx, userID); err != nil {
				log.Printf("[auto-square-off] Startup paper force-exit failed user=%s: %v", userID, err)
			}
		}
		if err := s.squareOffUserPositions(ctx, userID); err != nil {
			log.Printf("[auto-square-off] Startup live sq-off failed user=%s: %v", userID, err)
		}
	}
}

// shouldSquareOff returns true when current IST time matches squareOffTime (live) on a
// weekday and we haven't already executed today.
func (s *AutoSquareOffScheduler) shouldSquareOff() bool {
	now := time.Now().In(timezone.IST)
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
	now := time.Now().In(timezone.IST)
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
// Orders whose auto_square_off_time is set to a time later than now are skipped —
// their dedicated per-user scheduler run will handle them at the correct time.
func (s *AutoSquareOffScheduler) squareOffAllPositions(ctx context.Context) error {
	log.Println("[auto-square-off] Fetching today's open INTRADAY algo positions...")

	// Position-book driven path: uses broker position book as source of truth.
	// Bypasses the NOT EXISTS DB bug and handles partial/manual positions safely.
	// squareQty = min(brokerNetQty, ourFilledQty) so we never unwind the user's manual qty.
	if s.positionChecker != nil {
		return s.squareOffViaPositionBook(ctx)
	}

	// FALLBACK: DB-only path (legacy — GetOpenOrders has a NOT EXISTS bug with cancelled bracket legs).
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

	now := time.Now().In(timezone.IST)
	currentTime := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute())

	// positionChecker is nil here (we returned early above if it was set),
	// so openByUser stays empty and IsExited will always return false (fail-open).
	openByUser := make(map[string]*OpenPositions)

	successCount := 0
	failCount := 0
	skippedCount := 0

	for _, order := range openOrders {
		// Skip orders with zero filled quantity — nothing to reverse
		if order.FilledQuantity <= 0 {
			log.Printf("[auto-square-off] Skipping order %s (filled_qty=0)", order.OrderID)
			continue
		}

		// Skip orders whose user has set a custom square-off time that's later than now.
		// Those will be closed by checkUserSquareOffs at their designated time.
		if order.AutoSquareOffTime != nil && *order.AutoSquareOffTime > currentTime {
			log.Printf("[auto-square-off] Skipping order %s: user=%s has custom sq-off at %s (now %s)",
				order.OrderID, order.UserID, *order.AutoSquareOffTime, currentTime)
			continue
		}

		// Skip if broker already shows NetQty == 0 for this symbol — squaring off
		// a flat position would open a fresh short.
		if snapshot := openByUser[order.UserID]; snapshot.IsExited(order) {
			log.Printf("[auto-square-off] Skipping order %s: broker NetQty=0 for user=%s symbol=%s (already exited)",
				order.OrderID, order.UserID, order.Symbol)
			skippedCount++
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

	log.Printf("[auto-square-off] ========== LIVE COMPLETE: %d succeeded, %d failed, %d skipped (already flat) ==========",
		successCount, failCount, skippedCount)

	return nil
}

// checkUserSquareOffs queries the DB for users who have a custom auto_square_off_time
// matching the current IST minute and closes all their positions (paper + live).
// Runs every minute tick on weekdays. Guards against firing twice per user per day.
func (s *AutoSquareOffScheduler) checkUserSquareOffs(ctx context.Context) {
	now := time.Now().In(timezone.IST)
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

	// Fetch broker positions once for this user — skip orders already flat.
	// Fail-open: a nil snapshot bypasses the check.
	var snapshot *OpenPositions
	if s.positionChecker != nil {
		var fetchErr error
		snapshot, fetchErr = s.positionChecker.FetchOpenPositions(ctx, userID)
		if fetchErr != nil {
			log.Printf("[auto-square-off] Position-book check unavailable for user=%s; proceeding without skip: %v", userID, fetchErr)
			snapshot = nil
		}
	}

	for _, order := range openOrders {
		if order.FilledQuantity <= 0 {
			continue
		}
		if snapshot.IsExited(order) {
			log.Printf("[auto-square-off] Skipping live sq-off order=%s user=%s symbol=%s: broker NetQty=0 (already exited)",
				order.OrderID, userID, order.Symbol)
			continue
		}

		// Clamp to the broker's actual remaining qty when a snapshot is available.
		// Multiple order rows (e.g. an OCO's SL leg and TP leg, both EXECUTED after
		// a double-fill) can share one broker position; without this, each row
		// would reverse the full FilledQuantity independently and a second reverse
		// order would flip the already-flattened position open again. Nil snapshot
		// keeps the old fail-open behaviour (full FilledQuantity, no clamp).
		squareQty := int(order.FilledQuantity)
		if snapshot != nil {
			brokerQty := snapshot.GetNetQty(order)
			if brokerQty < 0 {
				brokerQty = -brokerQty
			}
			squareQty = min(brokerQty, int(order.FilledQuantity))
			if squareQty <= 0 {
				continue
			}
		}
		orderCopy := *order
		orderCopy.FilledQuantity = int32(squareQty)

		if err := s.createAndExecuteSquareOffOrder(ctx, &orderCopy); err != nil {
			log.Printf("[auto-square-off] FAILED live sq-off order=%s user=%s: %v", order.OrderID, userID, err)
			continue
		}

		if snapshot != nil {
			snapshot.Consume(order, squareQty)
		}
	}
	return nil
}

// squareOffViaPositionBook is the position-book-driven replacement for the DB-only
// squareOffAllPositions loop. It fetches the broker position book per user (in parallel)
// and uses GetExitableLiveOrdersByUser (no NOT EXISTS bug) to determine what to close.
//
// squareQty = min(brokerNetQty, ourFilledQty) — ensures we only unwind algo-placed
// quantity even when the user holds additional manual positions in the same symbol.
func (s *AutoSquareOffScheduler) squareOffViaPositionBook(ctx context.Context) error {
	users, err := s.orderRepo.GetDistinctActiveUsersToday(ctx)
	if err != nil {
		return fmt.Errorf("position-book sq-off: failed to get active users: %w", err)
	}
	if len(users) == 0 {
		log.Println("[auto-square-off] Position-book sq-off: no active users today")
		return nil
	}

	log.Printf("[auto-square-off] Position-book sq-off: %d active user(s)", len(users))

	now := time.Now().In(timezone.IST)
	currentTime := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute())

	var wg sync.WaitGroup
	for _, userID := range users {
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			// Recover per user so one user's panic can't crash the whole EOD
			// square-off (and the process) and abandon every other user.
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[auto-square-off] PANIC squaring off user=%s: %v", uid, r)
				}
			}()
			s.squareOffUserViaPositionBook(ctx, uid, currentTime)
		}(userID)
	}
	wg.Wait()
	return nil
}

// squareOffUserViaPositionBook fetches the broker position book for userID and
// closes each open algo entry order using squareQty = min(brokerNetQty, filledQty).
// On any position-book failure it falls back to squareOffUserPositions (DB-only).
func (s *AutoSquareOffScheduler) squareOffUserViaPositionBook(ctx context.Context, userID string, currentTime string) {
	snapshot, err := s.positionChecker.FetchOpenPositions(ctx, userID)
	if err != nil {
		log.Printf("[auto-square-off] Position book unavailable for user=%s, falling back to DB sq-off: %v", userID, err)
		if sqErr := s.squareOffUserPositions(ctx, userID); sqErr != nil {
			log.Printf("[auto-square-off] Fallback DB sq-off also failed for user=%s: %v", userID, sqErr)
		}
		return
	}

	orders, err := s.orderRepo.GetExitableLiveOrdersByUser(ctx, userID)
	if err != nil {
		log.Printf("[auto-square-off] Failed to get exitable orders for user=%s: %v", userID, err)
		return
	}
	if len(orders) == 0 {
		return
	}

	log.Printf("[auto-square-off] Position-book sq-off: user=%s has %d exitable order(s)", userID, len(orders))

	for _, order := range orders {
		if order.FilledQuantity <= 0 {
			continue
		}
		// Skip orders whose user-level custom sq-off time is later than now.
		if order.AutoSquareOffTime != nil && *order.AutoSquareOffTime > currentTime {
			log.Printf("[auto-square-off] Skipping order=%s user=%s sym=%s: custom sq-off at %s (now %s)",
				order.OrderID, userID, order.Symbol, *order.AutoSquareOffTime, currentTime)
			continue
		}

		brokerQty := snapshot.GetNetQty(order)
		if brokerQty == 0 {
			log.Printf("[auto-square-off] Skipping order=%s user=%s sym=%s: broker NetQty=0 (already flat)",
				order.OrderID, userID, order.Symbol)
			continue
		}

		// brokerQty is signed (negative for a short / SELL-entry position). Use its
		// magnitude so the square-off quantity is positive — a negative qty would be
		// rejected by the broker, leaving shorts un-squared-off. The reverse side is
		// derived from the entry's OrderSide inside createAndExecuteSquareOffOrder.
		if brokerQty < 0 {
			brokerQty = -brokerQty
		}

		squareQty := min(brokerQty, int(order.FilledQuantity))
		orderCopy := *order
		orderCopy.FilledQuantity = int32(squareQty)

		log.Printf("[auto-square-off] Position-book sq-off: user=%s sym=%s brokerQty=%d ourQty=%d squareQty=%d",
			userID, order.Symbol, brokerQty, order.FilledQuantity, squareQty)

		if err := s.createAndExecuteSquareOffOrder(ctx, &orderCopy); err != nil {
			log.Printf("[auto-square-off] FAILED user=%s sym=%s: %v", userID, order.Symbol, err)
			continue
		}

		// Multiple order rows (e.g. an OCO's SL leg and TP leg after a double-fill)
		// can share the same broker position. Decrement the snapshot so a later
		// row for the same symbol/exchange/product-type sees the reduced — likely
		// zero — remaining qty instead of re-reading the stale pre-loop NetQty and
		// placing a second reverse order that flips the now-flat position open
		// again in the other direction.
		snapshot.Consume(order, squareQty)
	}
}

// createAndExecuteSquareOffOrder creates a reverse order to close a position
// and executes it via the broker.
func (s *AutoSquareOffScheduler) createAndExecuteSquareOffOrder(ctx context.Context, originalOrder *models.Order) error {
	// Hard guard: never place live orders after market close (15:30 IST).
	// This is the last line of defence against AMOs regardless of how this function was reached.
	now := time.Now().In(timezone.IST)
	if now.Hour()*60+now.Minute() >= marketCloseMinutes {
		return fmt.Errorf("market closed (%02d:%02d IST ≥ 15:30) — refusing to place order to avoid AMO", now.Hour(), now.Minute())
	}

	// Cancel this symbol's resting SL/TP (OCO) and multi-level exit legs BEFORE
	// placing the reverse order. Otherwise a stop could trigger in the window
	// between our reverse fill and the broker's own EOD cancellation, executing
	// into a now-flat book and opening a fresh (short) position. Per-symbol, so a
	// position whose custom square-off time hasn't arrived keeps its protection.
	// Nil-safe: when unwired, behaviour is unchanged (reverse order only).
	if s.cancelProtectiveLegs != nil {
		s.cancelProtectiveLegs(ctx, originalOrder.UserID, originalOrder.Symbol)
	}

	// Determine reverse side
	reverseSide := models.OrderSideSell
	if originalOrder.OrderSide == models.OrderSideSell {
		reverseSide = models.OrderSideBuy
	}

	// Compute a marketable IOC LIMIT price for immediate square-off.
	// MARKET orders are prohibited in NSE algo trading; a LIMIT priced sqOffSlippagePct
	// through the LTP (SELL below / BUY above) crosses the spread and fills instantly
	// against the resting book, while IOC cancels any unfilled remainder.
	//
	// NOTE: a prior version sent an SL-L order with the trigger on the far side of LTP
	// ("trigger ≈ LTP"). A stop only fires after an adverse move, so the order could rest
	// unfilled and leave the position OPEN past EOD if price didn't move against it.
	// A marketable limit has no trigger and so no directional dependency.
	var limitPrice float64
	if s.ltpLookup != nil {
		ltp, ltpErr := s.ltpLookup.GetLTP(ctx, string(originalOrder.Exchange), originalOrder.StockCode)
		if ltpErr == nil && ltp > 0 {
			if reverseSide == models.OrderSideSell {
				limitPrice = roundTwoDP(ltp * (1 - sqOffSlippagePct))
			} else {
				limitPrice = roundTwoDP(ltp * (1 + sqOffSlippagePct))
			}
		} else {
			log.Printf("[auto-square-off] LTP unavailable for %s:%d (%v) — limit price left at 0, broker will reject",
				originalOrder.Exchange, originalOrder.StockCode, ltpErr)
		}
	} else {
		log.Printf("[auto-square-off] No LTP lookup wired — limit price left at 0 for order %s (configure SetLTPLookup)", originalOrder.OrderID)
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
		// Marketable IOC LIMIT (no trigger) → fills immediately; StopLoss left nil so the
		// broker payload builder emits ordType "Limit", not "SL". MARKET barred for algo.
		OrderType:        models.OrderTypeLimit,
		OrderSide:        reverseSide,
		Quantity:         originalOrder.FilledQuantity, // only exit the filled portion
		Price:            &limitPrice,                  // marketable limit = LTP ∓ sqOffSlippagePct
		Validity:         "IOC",                        // Immediate or Cancel
		ProductType:      originalOrder.ProductType,
		Status:           models.StatusReceived,
		IsSquareOffOrder: true,  // mark so it won't be picked up again
		IsPaperTrade:     false, // live order
		TradingMode:      "LIVE",
		// Link back to the entry order so statusservice can record the exact
		// exit price / P&L on the parent when this reverse order fills.
		ParentOrderID: &originalOrder.OrderID,
		RiskApproved:  true, // auto square-off bypasses risk checks
		// BearerToken / AppId / Source intentionally omitted (left nil).
		// The token stored on the original order was captured at entry time and is
		// likely expired by square-off time. Leaving them nil forces executor.go to
		// fetch fresh credentials from the DB via CredentialsCache (credSource="cache"),
		// so the most recently stored token is used and the 401-retry path can
		// pick up any token refresh that happened since the original order was placed.
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
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

// ════════════════════════════════════════════════════════════════════════════
// Strategy-scoped square-off (auto_square_off_time set per strategy signal)
// ════════════════════════════════════════════════════════════════════════════

// checkStrategySquareOffs fires every minute tick on weekdays. It queries the orders
// table for (user, strategy) pairs whose auto_square_off_time matches the current IST
// minute, then closes ONLY those strategies' positions — both paper and live — leaving
// every other strategy's open positions untouched.
func (s *AutoSquareOffScheduler) checkStrategySquareOffs(ctx context.Context) {
	now := time.Now().In(timezone.IST)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return
	}
	currentTime := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute())
	today := now.Format("2006-01-02")

	targets, err := s.orderRepo.GetStrategiesDueForSquareOff(ctx, currentTime)
	if err != nil {
		log.Printf("[auto-square-off] Strategy sq-off: failed to fetch targets at %s: %v", currentTime, err)
		return
	}
	if len(targets) == 0 {
		return
	}

	for _, t := range targets {
		// Per-(user, strategy) dedup so we don't fire twice in the same minute/day.
		guardKey := t.UserID + ":" + t.StrategyID + ":" + currentTime
		s.mu.Lock()
		if s.lastUserExecuteDates[guardKey] == today {
			s.mu.Unlock()
			continue
		}
		s.lastUserExecuteDates[guardKey] = today
		s.mu.Unlock()

		log.Printf("[auto-square-off] ===== STRATEGY SQUARE-OFF: user=%s strategy=%s time=%s =====",
			t.UserID, t.StrategyID, currentTime)

		// Close paper positions for this strategy.
		if s.paperForceExitStrategy != nil {
			if err := s.paperForceExitStrategy(ctx, t.UserID, t.StrategyID); err != nil {
				log.Printf("[auto-square-off] Paper strategy exit failed user=%s strategy=%s: %v",
					t.UserID, t.StrategyID, err)
			}
		}

		// Close live positions for this strategy.
		if err := s.squareOffStrategyLivePositions(ctx, t.UserID, t.StrategyID); err != nil {
			log.Printf("[auto-square-off] Live strategy sq-off failed user=%s strategy=%s: %v",
				t.UserID, t.StrategyID, err)
		}
	}
}

// squareOffStrategyLivePositions closes all open live positions for one (user, strategy)
// pair. It uses the broker position-book check when available so symbols already flat
// (NetQty == 0) are skipped, and otherwise falls open (closes based on DB state).
func (s *AutoSquareOffScheduler) squareOffStrategyLivePositions(ctx context.Context, userID, strategyID string) error {
	orders, err := s.orderRepo.GetExitableLiveOrdersByStrategy(ctx, strategyID, userID)
	if err != nil {
		return fmt.Errorf("get exitable orders user=%s strategy=%s: %w", userID, strategyID, err)
	}
	if len(orders) == 0 {
		log.Printf("[auto-square-off] Strategy sq-off: no open live positions user=%s strategy=%s", userID, strategyID)
		return nil
	}

	log.Printf("[auto-square-off] Strategy sq-off: user=%s strategy=%s has %d live order(s)",
		userID, strategyID, len(orders))

	// Fetch the broker position book once for this user. Fail-open: a nil snapshot
	// makes IsExited return false so the square-off proceeds on DB state alone.
	var snapshot *OpenPositions
	if s.positionChecker != nil {
		var fetchErr error
		snapshot, fetchErr = s.positionChecker.FetchOpenPositions(ctx, userID)
		if fetchErr != nil {
			log.Printf("[auto-square-off] Position-book unavailable user=%s; proceeding without skip: %v", userID, fetchErr)
			snapshot = nil
		}
	}

	for _, order := range orders {
		if order.FilledQuantity <= 0 {
			continue
		}
		if snapshot.IsExited(order) {
			log.Printf("[auto-square-off] Skipping order=%s user=%s sym=%s: broker NetQty=0 (already flat)",
				order.OrderID, userID, order.Symbol)
			continue
		}

		// Clamp to the broker's actual remaining qty when a snapshot is available,
		// and decrement it after a successful close. Multiple order rows (e.g. an
		// OCO's SL leg and TP leg, both EXECUTED after a double-fill) can share one
		// broker position; without this, each row would reverse its full
		// FilledQuantity independently against the same stale pre-loop NetQty,
		// flipping the already-flattened position open again in the other
		// direction. Nil snapshot keeps the old fail-open behaviour.
		squareQty := int(order.FilledQuantity)
		if snapshot != nil {
			brokerQty := snapshot.GetNetQty(order)
			if brokerQty < 0 {
				brokerQty = -brokerQty
			}
			squareQty = min(brokerQty, int(order.FilledQuantity))
			if squareQty <= 0 {
				continue
			}
		}
		orderCopy := *order
		orderCopy.FilledQuantity = int32(squareQty)

		if err := s.createAndExecuteSquareOffOrder(ctx, &orderCopy); err != nil {
			log.Printf("[auto-square-off] FAILED strategy sq-off order=%s user=%s strategy=%s: %v",
				order.OrderID, userID, strategyID, err)
			continue
		}

		if snapshot != nil {
			snapshot.Consume(order, squareQty)
		}
	}
	return nil
}

// runStartupStrategyCatchUp closes positions for (user, strategy) pairs whose
// auto_square_off_time has already passed today (e.g. the service was down at that
// time). Live orders are only placed if we are still before market close (15:30 IST)
// to avoid creating AMOs; paper exits are always safe.
func (s *AutoSquareOffScheduler) runStartupStrategyCatchUp(ctx context.Context, now time.Time, today string, currentMinutes int) {
	currentTimeStr := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute())

	targets, err := s.orderRepo.GetStrategiesWithExpiredSquareOff(ctx, currentTimeStr)
	if err != nil {
		log.Printf("[auto-square-off] Startup strategy catch-up: failed to fetch targets: %v", err)
		return
	}
	if len(targets) == 0 {
		return
	}

	log.Printf("[auto-square-off] Startup strategy catch-up: %d (user, strategy) pair(s) with sq-off time already passed", len(targets))

	pastClose := currentMinutes >= marketCloseMinutes
	if pastClose {
		log.Printf("[auto-square-off] Startup strategy catch-up: market closed (now %02d:%02d IST) — closing paper only, marking live done without orders to prevent AMOs",
			now.Hour(), now.Minute())
	}

	for _, t := range targets {
		guardKey := t.UserID + ":" + t.StrategyID + ":startup-catchup"
		s.mu.Lock()
		if s.lastUserExecuteDates[guardKey] == today {
			s.mu.Unlock()
			continue
		}
		s.lastUserExecuteDates[guardKey] = today
		s.mu.Unlock()

		log.Printf("[auto-square-off] Startup strategy catch-up: closing user=%s strategy=%s", t.UserID, t.StrategyID)

		// Paper is simulated — no AMO risk, always safe to close.
		if s.paperForceExitStrategy != nil {
			if err := s.paperForceExitStrategy(ctx, t.UserID, t.StrategyID); err != nil {
				log.Printf("[auto-square-off] Startup paper strategy exit failed user=%s strategy=%s: %v",
					t.UserID, t.StrategyID, err)
			}
		}

		// Live exits only before market close — otherwise an exit order becomes an AMO.
		if pastClose {
			continue
		}
		if err := s.squareOffStrategyLivePositions(ctx, t.UserID, t.StrategyID); err != nil {
			log.Printf("[auto-square-off] Startup live strategy sq-off failed user=%s strategy=%s: %v",
				t.UserID, t.StrategyID, err)
		}
	}
}
