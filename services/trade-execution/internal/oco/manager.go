// Package oco implements a custom One-Cancels-the-Other order management system.
//
// Architecture overview:
//
//	                  ┌─────────────────────┐
//	TradeSignal ──→   │    OCO Manager       │
//	                  │  (in-memory registry │
//	                  │   + broker ID index) │
//	                  └──────────┬──────────┘
//	                             │
//	   ┌──────────┬──────────────┼───────────────────┐
//	   ▼          ▼              ▼                    ▼
//	PlaceEntry  OnFill →     HandleBrokerUpdate   TrailingMonitor
//	(SL order)  PlaceLegs   (cancel counterpart)  (ModifyOrder)
//
// All state is in-memory (sync.Map) for O(1) lookups from broker WS events.
// State changes are persisted to PostgreSQL asynchronously.
package oco

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/fillprice"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

// ocoRecover recovers a panic in a detached OCO goroutine and logs it with a
// stack trace, matching the existing [oco] log style. Every method launched with
// `go m.<method>(…)` is a fire-and-forget goroutine, so an unrecovered panic in
// one would crash the whole process and abandon every other OCO group. This only
// logs and returns — it never alters order logic or the non-panic path.
func ocoRecover(where string) {
	if r := recover(); r != nil {
		log.Printf("[oco] PANIC RECOVERED in %s: %v\n%s", where, r, debug.Stack())
	}
}

// defaultFillPriceDeadline bounds how long an entry-filled group waits for a real
// execution price (WS or trade book) before SL/TP legs are placed off the
// buffer-stripped entry limit. Keeps the unprotected window small but lets the
// trade book win in the common case.
const defaultFillPriceDeadline = 30 * time.Second

// defaultFillPriceRetryInterval is the tick of the fill-price retry worker.
const defaultFillPriceRetryInterval = 3 * time.Second

// maxEntryRetries is how many times we retry placing the OCO entry order on transient errors (e.g. 401).
const maxEntryRetries = 2

// maxLegPlacementRetries is how many times we retry placing SL/TP legs after entry fill.
const maxLegPlacementRetries = 3

// brokerOpTimeout is the max duration for any single broker API call (place, cancel, modify).
const brokerOpTimeout = 30 * time.Second

// dbRetryAttempts is how many times async DB updates are retried before giving up.
const dbRetryAttempts = 3

// defaultPartialFillTimeout is how long to wait for remaining entry qty
// after a partial fill before cancelling the unfilled portion.
const defaultPartialFillTimeout = 50 * time.Second

// CredentialsRefresher allows the OCO manager to invalidate stale auth tokens
// and reload fresh credentials from the database on 401 errors.
type CredentialsRefresher interface {
	Invalidate(userID string)
	Get(ctx context.Context, userID string) (userId, appId, source, bearerToken string, err error)
}

// OCOManager is the central orchestrator for all OCO order groups.
//
// Thread-safety: all exported methods are safe for concurrent use.
// Internal state uses sync.Map for lock-free reads (the dominant operation:
// every broker WS event does a read; writes happen only on group creation/completion).
type OCOManager struct {
	// ── Dependencies ────────────────────────────────────────────────────────
	repo         repository.OrderRepository
	indiraClient *indira.ExecutionClient
	credsCache   CredentialsRefresher // nil-safe; when set, enables 401 auth refresh

	// wsBroadcaster pushes real-time updates to the frontend WS.
	wsBroadcaster func(userID string, eventType string, order *models.Order)

	// wsEnsure (re)establishes the user's broker order-WS subscription so the
	// EXECUTED event that triggers SL/TP leg placement is actually consumed.
	// Wired in main.go to OrderStatusService.StartSubscription; nil-safe.
	wsEnsure func(userID string, auth *indiraClient.AuthContext)

	// partialFillTimeout is how long to wait for remaining entry qty after a
	// partial fill before cancelling the unfilled portion. Configurable via
	// OCO_PARTIAL_FILL_TIMEOUT env var (default 50s).
	partialFillTimeout time.Duration

	// fillCache resolves the execution price from the broker trade book (batched
	// per user) when the order WS carried no TradedPrice. nil-safe.
	fillCache *fillprice.Cache
	// fillPriceDeadline bounds the AWAITING_FILL_PRICE wait before the limit
	// fallback. fillRetryInterval is the retry worker's tick.
	fillPriceDeadline time.Duration
	fillRetryInterval time.Duration

	// ── In-memory state (O(1) lookup) ───────────────────────────────────────
	// groups: groupID (uuid.UUID) → *OCOGroup
	groups sync.Map

	// brokerIndex: brokerOrderID (string) → groupID (uuid.UUID)
	// This is the critical index: when a broker WS event arrives with a brokerOrderID,
	// we look up which OCO group it belongs to in O(1).
	brokerIndex sync.Map

	// groupMu provides per-group mutual exclusion for state transitions.
	// Key: groupID string → *sync.Mutex
	// This prevents race conditions when two WS events for the same group arrive
	// simultaneously (e.g., both SL and TP fill at the same instant).
	groupMu sync.Map
}

// NewOCOManager creates a new OCO manager.
func NewOCOManager(
	repo repository.OrderRepository,
	indiraClient *indira.ExecutionClient,
) *OCOManager {
	return &OCOManager{
		repo:               repo,
		indiraClient:       indiraClient,
		partialFillTimeout: defaultPartialFillTimeout,
		fillPriceDeadline:  defaultFillPriceDeadline,
		fillRetryInterval:  defaultFillPriceRetryInterval,
	}
}

// SetFillPriceCache wires the trade-book execution-price resolver used by the
// fill-price retry worker, along with its deadline and retry interval. Values
// <= 0 keep the existing defaults. nil-safe.
func (m *OCOManager) SetFillPriceCache(c *fillprice.Cache, deadline, retryInterval time.Duration) {
	m.fillCache = c
	if deadline > 0 {
		m.fillPriceDeadline = deadline
	}
	if retryInterval > 0 {
		m.fillRetryInterval = retryInterval
	}
}

// SetPartialFillTimeout sets the timeout for waiting on remaining entry qty
// after a partial fill. If not called, defaults to 50 seconds.
func (m *OCOManager) SetPartialFillTimeout(d time.Duration) {
	if d > 0 {
		m.partialFillTimeout = d
	}
}

// SetWSBroadcaster wires the frontend WS push callback.
// SetWSEnsurer wires a callback that (re)establishes the user's broker order-WS
// subscription. AdoptOrder calls it so a price-monitor entry placed after the
// connection was idle-swept still receives its EXECUTED fill and gets SL/TP legs.
func (m *OCOManager) SetWSEnsurer(fn func(userID string, auth *indiraClient.AuthContext)) {
	m.wsEnsure = fn
}

func (m *OCOManager) SetWSBroadcaster(fn func(userID string, eventType string, order *models.Order)) {
	m.wsBroadcaster = fn
}

// SetCredentialsCache wires the credentials cache for 401 auth-refresh on order placement.
func (m *OCOManager) SetCredentialsCache(cc CredentialsRefresher) {
	m.credsCache = cc
}

// isSessionExpiredError returns true if the error indicates a 401 / session-expired
// response from the broker, meaning the bearer token is stale.
func isSessionExpiredError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "HTTP error 401") || strings.Contains(msg, "Session expired")
}

// isSessionNotReadyError returns true if the broker rejected the order because its
// trading session hasn't finished initializing after a WS reconnect (AU004).
// This is a transient condition — the session typically becomes ready within
// a few seconds, so the caller should wait longer than a 401 retry before retrying.
func isSessionNotReadyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "AU004") || strings.Contains(msg, "Session data not received")
}

// resolveAuth proactively checks the credentials cache for a fresher bearer token
// than the one stored on the group, and updates group.Auth in place if so. This is
// cheap (cache hit = single RLock + map lookup) and matters most for groups
// reconstructed from DB on restart: their stored token may already be stale, but
// another code path may have refreshed the cache in the meantime. Without this,
// every restart-reconstructed group wastes one broker round-trip on a 401 before
// recovering. Safe to call without holding the group mutex — the swap is a single
// pointer write.
//
// Returns the auth to use (never nil — falls back to group.Auth on any error).
func (m *OCOManager) resolveAuth(ctx context.Context, group *OCOGroup) *indiraClient.AuthContext {
	// NOTE: group.Auth can be nil for a group reconstructed from DB on restart
	// (the bearer token is never persisted). We must still build a fresh auth from
	// the credentials cache here — otherwise leg cancellation runs with a nil auth,
	// which both panics CancelOrder and leaves the SL/TP legs resting at the broker.
	if m.credsCache == nil || group.UserID == "" {
		return group.Auth
	}
	userId, appId, source, bearerToken, err := m.credsCache.Get(ctx, group.UserID)
	if err != nil || bearerToken == "" {
		return group.Auth
	}
	if group.Auth != nil && bearerToken == group.Auth.BearerToken {
		return group.Auth
	}
	fresh := &indiraClient.AuthContext{
		UserId:      userId,
		AppId:       appId,
		Source:      source,
		BearerToken: bearerToken,
	}
	group.Auth = fresh
	return fresh
}

// refreshAuth invalidates the cache for userID, reloads fresh credentials from DB,
// and returns an updated AuthContext. Returns nil if refresh is unavailable or fails.
func (m *OCOManager) refreshAuth(ctx context.Context, userID string, currentAuth *indiraClient.AuthContext) *indiraClient.AuthContext {
	if m.credsCache == nil {
		return nil
	}
	m.credsCache.Invalidate(userID)
	userId, appId, source, bearerToken, err := m.credsCache.Get(ctx, userID)
	if err != nil {
		log.Printf("[oco] Auth refresh from DB failed for user %s: %v", userID, err)
		return nil
	}
	// Only return new auth if the token actually changed
	if bearerToken == currentAuth.BearerToken {
		log.Printf("[oco] Auth refresh for user %s returned same token — cannot recover", userID)
		return nil
	}
	log.Printf("[oco] Auth refreshed from DB for user %s", userID)
	return &indiraClient.AuthContext{
		UserId:      userId,
		AppId:       appId,
		Source:      source,
		BearerToken: bearerToken,
	}
}

// GetGroupMu returns the per-group mutex, creating it if needed.
func (m *OCOManager) GetGroupMu(groupID uuid.UUID) *sync.Mutex {
	val, _ := m.groupMu.LoadOrStore(groupID.String(), &sync.Mutex{})
	return val.(*sync.Mutex)
}

// dbUpdateAsync persists an order update to DB asynchronously with retry.
func (m *OCOManager) dbUpdateAsync(order *models.Order, label string) {
	go func() {
		defer ocoRecover("dbUpdateAsync")
		ctx, cancel := context.WithTimeout(context.Background(), brokerOpTimeout)
		defer cancel()
		var lastErr error
		for attempt := 0; attempt < dbRetryAttempts; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
			if err := m.repo.Update(ctx, order); err != nil {
				lastErr = err
				continue
			}
			return
		}
		log.Printf("[oco] DB update failed for %s after %d retries: %v", label, dbRetryAttempts, lastErr)
	}()
}

// recordEntryExit writes the exit price, final P&L, exit time, and exit reason
// onto the OCO group's ENTRY order when an SL or TP leg fills. Without this the
// entry order would never be marked closed in the DB — the closed-orders view
// and the LiveOrderMonitor's position_exit event both depend on live_exit_price
// being set. exitPrice is the leg's actual broker fill price, so what the user
// sees always matches what executed at the exchange.
//
// Idempotent: the repository guards on live_exit_price IS NULL, so a race
// between the SL fill and an EOD square-off resolves to first-writer-wins.
// Runs async so it never blocks the broker WS read loop.
func (m *OCOManager) recordEntryExit(group *OCOGroup, exitPrice float64, reason string, exitTime *time.Time) {
	t := time.Now()
	if exitTime != nil && !exitTime.IsZero() {
		t = *exitTime
	}
	pnl := group.PnL
	entryID := group.EntryOrderID
	groupID := group.GroupID
	go func() {
		defer ocoRecover("recordEntryExitAsync")
		ctx, cancel := context.WithTimeout(context.Background(), brokerOpTimeout)
		defer cancel()
		if err := m.repo.UpdateLiveTradeExit(ctx, entryID, exitPrice, pnl, t, reason); err != nil {
			log.Printf("[oco] Failed to record entry exit for group %s: %v", groupID, err)
			return
		}
		log.Printf("[oco] Entry exit recorded for group %s: exit=%.2f pnl=%.2f reason=%s", groupID, exitPrice, pnl, reason)
	}()
}

// ════════════════════════════════════════════════════════════════════════════
// STEP 1: Create OCO Entry Order
// ════════════════════════════════════════════════════════════════════════════

// CreateOCOEntry places the initial SL entry order and registers the OCO group.
//
// Parameters:
//   - entryTriggerPrice: the price level at which the entry should trigger
//     (calculated from user's minimum percentage criteria)
//   - slPercent, tpPercent: stop loss and take profit percentages
//   - trailingSL: whether to enable trailing stop loss
//   - trailingSLPct: trailing SL percentage (0 = use slPercent)
//
// The entry order is placed as:
//   - OrderType: SL (Stop Loss Limit)
//   - TriggerPrice: entryTriggerPrice
//   - LimitPrice: entryTriggerPrice * 1.005 (0.5% buffer for fill)
func (m *OCOManager) CreateOCOEntry(
	ctx context.Context,
	userID string,
	strategyID string,
	eventID uuid.UUID,
	stockCode int64,
	exchange string,
	symbol string,
	orderSide string, // BUY or SELL
	quantity int32,
	entryTriggerPrice float64,
	slPercent float64,
	tpPercent float64,
	trailingSL bool,
	trailingSLPct float64,
	productType string,
	auth *indiraClient.AuthContext,
) (*OCOGroup, error) {

	groupID := uuid.New()
	entryOrderID := uuid.New()

	// Entry is placed as a Limit (RL) order at the trigger price with a small buffer.
	// The broker rejects SL as the "main leg" order type (EG003: must be RL or RL-MKT).
	// Since the rules engine fires the signal only when price is already AT the trigger,
	// a limit order at that price fills immediately without needing a stop-trigger mechanism.
	// A 0.5% buffer ensures fill even if price moved slightly by the time the order arrives.
	var entryLimitPrice float64
	if orderSide == "BUY" {
		entryLimitPrice = roundNSE(entryTriggerPrice * 1.005)
	} else {
		entryLimitPrice = roundNSE(entryTriggerPrice * 0.995)
	}

	// OCO entries must always be INTRADAY — the OCO manager places SL/TP legs
	// itself, so the broker must NOT see a bracket order (BRACKET_ORDER with
	// boStpLoss:0 / boTgtPrice:0 causes EG003 "price could not be negative").
	productType = "INTRADAY"
	// NOTE: Do NOT default trailingSLPct to slPercent. TrailingSLPct is the
	// minimum threshold for broker modify calls (to avoid API spam on tiny ticks),
	// NOT the trailing distance. A 2%+ threshold makes the trailing SL unresponsive.
	// CalculateTrailingSL already defaults to 0.1% when TrailingSLPct <= 0.

	// Create the OCO group
	group := &OCOGroup{
		GroupID:         groupID,
		UserID:          userID,
		EntryOrderID:    entryOrderID,
		EntryLimitPrice: entryLimitPrice,
		SLPercent:       slPercent,
		TPPercent:       tpPercent,
		TrailingSL:      trailingSL,
		TrailingSLPct:   trailingSLPct,
		State:           StatePendingEntry,
		Symbol:          symbol,
		Exchange:        exchange,
		StockCode:       stockCode,
		Quantity:        quantity,
		OrderSide:       orderSide,
		Auth:            auth,
		ProductType:     productType,
		Validity:        "DAY",
		StrategyID:      strategyID,
		EventID:         eventID,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	// Build the entry order model
	entryOrder := &models.Order{
		OrderID:      entryOrderID,
		UserID:       userID,
		StrategyID:   strategyID,
		StrategyName: group.StrategyName,
		EventID:      eventID,
		StockCode:    stockCode,
		Exchange:     models.Exchange(exchange),
		Symbol:       symbol,
		OrderType:    models.OrderTypeLimit, // RL (Regular Limit) — broker requires RL/RL-MKT for main leg
		OrderSide:    models.OrderSide(orderSide),
		Quantity:     quantity,
		Price:        &entryLimitPrice, // trigger price + 0.5% buffer for guaranteed fill
		// No StopLoss field: sending a trigger price makes the broker see this as an SL order (EG003)
		Validity:     "DAY",
		ProductType:  productType,
		Status:       models.StatusReceived,
		RiskApproved: true,
		IsPaperTrade: false,
		TradingMode:  "LIVE",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		BearerToken:  &auth.BearerToken,
		AppId:        &auth.AppId,
		Source:       &auth.Source,
		// OCO fields
		OCOGroupID: &groupID,
		OCORole:    stringPtr(string(RoleEntry)),
	}
	// Persist trailing SL config on the entry order so reconstructGroup can
	// restore TrailingSL = true after a service restart.
	if trailingSL {
		s := "TRAILING"
		entryOrder.StopLossType = &s
	}
	if trailingSLPct > 0 {
		entryOrder.TrailingSLPct = &trailingSLPct
	}

	// Persist entry order to DB
	if err := m.repo.Create(ctx, entryOrder); err != nil {
		return nil, fmt.Errorf("failed to persist OCO entry order: %w", err)
	}

	// Place the entry SL order at broker with retry on 401 (session expired) and
	// AU004 (session not ready yet after WS reconnect).
	var brokerID string
	var placeErr error
	currentAuth := auth
	for attempt := 0; attempt <= maxEntryRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * 500 * time.Millisecond
			// AU004 means the broker session is still initializing after a WS
			// reconnect — use a longer backoff to give it time to become ready.
			if isSessionNotReadyError(placeErr) {
				delay = time.Duration(attempt) * 3 * time.Second
			}
			time.Sleep(delay)

			// On 401, refresh credentials from DB before retrying.
			if isSessionExpiredError(placeErr) {
				newAuth := m.refreshAuth(ctx, userID, currentAuth)
				if newAuth == nil {
					break // Can't recover — stop retrying
				}
				currentAuth = newAuth
				// Update the order and group with refreshed auth
				entryOrder.BearerToken = &currentAuth.BearerToken
				entryOrder.AppId = &currentAuth.AppId
				entryOrder.Source = &currentAuth.Source
			}
		}

		brokerID, placeErr = m.indiraClient.PlaceOrder(ctx, entryOrder, currentAuth)
		if placeErr == nil {
			break // Success
		}

		log.Printf("[oco] Entry order placement attempt %d failed for group %s: %v",
			attempt+1, groupID, placeErr)

		// Retry on 401 (stale token) and AU004 (broker session not yet ready).
		// All other broker rejections are permanent — stop retrying immediately.
		if !isSessionExpiredError(placeErr) && !isSessionNotReadyError(placeErr) {
			break
		}
	}
	if placeErr != nil {
		entryOrder.Status = models.StatusFailed
		errMsg := placeErr.Error()
		entryOrder.ErrorMessage = &errMsg
		m.repo.Update(ctx, entryOrder)
		return nil, fmt.Errorf("failed to place OCO entry order: %w", placeErr)
	}

	// If auth was refreshed, update the group's auth so legs use the fresh token.
	if currentAuth != auth {
		auth = currentAuth
		group.Auth = auth
	}

	// Update order with broker ID
	entryOrder.IndiraOrderID = &brokerID
	entryOrder.Status = models.StatusSubmitted
	now := time.Now()
	entryOrder.SubmittedAt = &now
	group.EntryBrokerID = brokerID

	// Persist SL/TP percentages on the entry order so reconstructGroup can restore
	// group.SLPercent and group.TPPercent after a service restart.
	// These are set AFTER PlaceOrder so the broker never sees them as TriggerPrice.
	entryOrder.StopLoss = &slPercent
	entryOrder.TakeProfit = &tpPercent

	// Register in memory IMMEDIATELY — before DB persist.
	// The broker WS can fire an EXECUTED update within milliseconds of PlaceOrder
	// returning. If the status service receives that update before the DB write
	// at repo.Update completes, GetByIndiraOrderID fails and the status service
	// incorrectly triggers handleManualExitDetection, which cancels the OCO group
	// and prevents SL/TP legs from ever being placed.
	// By registering in memory first, the status service can check IsKnownBrokerID
	// to avoid false manual-exit detection.
	m.groups.Store(groupID, group)
	m.brokerIndex.Store(brokerID, groupID)

	// Persist broker ID synchronously to DB for durability.
	if err := m.repo.Update(ctx, entryOrder); err != nil {
		log.Printf("[oco] WARNING: failed to persist entry order broker ID for group %s: %v", groupID, err)
	}

	log.Printf("[oco] Created OCO group %s: entry=%s broker=%s symbol=%s trigger=%.2f limit=%.2f SL=%.1f%% TP=%.1f%% trailing=%v",
		groupID, entryOrderID, brokerID, symbol, entryTriggerPrice, entryLimitPrice, slPercent, tpPercent, trailingSL)

	// Broadcast to frontend
	if m.wsBroadcaster != nil {
		m.wsBroadcaster(userID, "oco_entry_placed", entryOrder)
	}

	return group, nil
}

// ════════════════════════════════════════════════════════════════════════════
// STEP 2: Handle Broker WS Events (Core OCO Logic)
// ════════════════════════════════════════════════════════════════════════════

// HandleBrokerUpdate is called by StatusService on every broker WS event.
// It checks if the updated order belongs to an OCO group and acts accordingly:
//   - Entry filled → place SL + TP legs
//   - SL leg filled → cancel TP leg
//   - TP leg filled → cancel SL leg
//   - Any leg rejected/cancelled externally → handle gracefully
//
// This is the HOT PATH — must be fast. Two sync.Map lookups (O(1)) to find the group.
func (m *OCOManager) HandleBrokerUpdate(ctx context.Context, order *models.Order, brokerStatus string) {
	defer ocoRecover("HandleBrokerUpdate")
	if order == nil || order.IndiraOrderID == nil {
		return
	}

	brokerID := *order.IndiraOrderID
	brokerStatusUpper := strings.ToUpper(strings.TrimSpace(brokerStatus))

	// O(1) lookup: does this broker order ID belong to an OCO group?
	groupIDVal, ok := m.brokerIndex.Load(brokerID)
	if !ok {
		// Also check by OCOGroupID on the order itself (for orders loaded from DB)
		if order.OCOGroupID == nil {
			return // Not an OCO order
		}
		groupIDVal = *order.OCOGroupID
	}

	groupID := groupIDVal.(uuid.UUID)

	// O(1) lookup: get the OCO group
	groupVal, ok := m.groups.Load(groupID)
	if !ok {
		return // Group not in memory (might have been cleaned up)
	}
	group := groupVal.(*OCOGroup)

	// Per-group mutex: prevents race if two events for same group arrive simultaneously
	mu := m.GetGroupMu(groupID)
	mu.Lock()
	defer mu.Unlock()

	// Skip if group is already terminal
	if group.State.IsTerminal() {
		return
	}

	// Route based on which order was updated
	switch brokerID {
	case group.EntryBrokerID:
		m.handleEntryUpdate(ctx, group, order, brokerStatusUpper)
	case group.SLBrokerID:
		m.handleSLLegUpdate(ctx, group, order, brokerStatusUpper)
	case group.TPBrokerID:
		m.handleTPLegUpdate(ctx, group, order, brokerStatusUpper)
	default:
		// Race: WS PENDING/EXECUTED arrived for a leg before placeOCOLegs stored
		// the broker ID on the group (race window between PlaceOrder returning and
		// the post-wg.Wait mutex section). Use OCORole from the DB-loaded order to
		// route correctly, and backfill the broker ID so future events hit the
		// normal switch cases above.
		if order.OCORole == nil || group.State.IsTerminal() {
			return
		}
		isTerminalBrokerStatus := brokerStatusUpper == "CANCELLED" ||
			brokerStatusUpper == "REJECTED" ||
			brokerStatusUpper == "A.REJECTED" ||
			brokerStatusUpper == "EXECUTED" ||
			brokerStatusUpper == "TRADED"

		switch *order.OCORole {
		case string(RoleSLLeg):
			if group.SLBrokerID == "" {
				// SLBrokerID was cleared by a prior handler for this terminal event.
				// Duplicate WS deliveries must not re-register and re-invoke the handler,
				// or each duplicate triggers another clear→backfill→clear log storm.
				if isTerminalBrokerStatus {
					return
				}
				group.SLBrokerID = brokerID
				m.brokerIndex.Store(brokerID, group.GroupID)
				log.Printf("[oco] Race-recovery: backfilled SL broker ID %s for group %s", brokerID, group.GroupID)
			}
			m.handleSLLegUpdate(ctx, group, order, brokerStatusUpper)
		case string(RoleTPLeg):
			if group.TPBrokerID == "" {
				// Same guard as SL leg above.
				if isTerminalBrokerStatus {
					return
				}
				group.TPBrokerID = brokerID
				m.brokerIndex.Store(brokerID, group.GroupID)
				log.Printf("[oco] Race-recovery: backfilled TP broker ID %s for group %s", brokerID, group.GroupID)
			}
			m.handleTPLegUpdate(ctx, group, order, brokerStatusUpper)
		}
	}
}

// handleEntryUpdate processes broker status updates for the entry order.
func (m *OCOManager) handleEntryUpdate(ctx context.Context, group *OCOGroup, order *models.Order, status string) {
	switch status {
	case "EXECUTED", "TRADED":
		// ── Case A: Full fill after a partial fill — remaining qty filled ──
		if group.PartialFillActive {
			log.Printf("[oco] Entry FULLY FILLED after partial fill for group %s (qty=%d) — cancelling timer, modifying legs",
				group.GroupID, group.Quantity)

			// Cancel the partial fill timeout timer
			if group.PartialFillCancelFunc != nil {
				group.PartialFillCancelFunc()
				group.PartialFillCancelFunc = nil
			}
			group.PartialFillActive = false
			group.PartialFillTimerStarted = false
			group.FilledQty = group.Quantity
			group.UpdatedAt = time.Now()

			// Modify SL and TP legs to full quantity
			go m.modifyLegsQty(group, group.Quantity)
			return
		}

		// ── Case B: Normal full fill (no prior partial) ──
		// Accept the fill from the initial PENDING_ENTRY state or from a group
		// already parked in AWAITING_FILL_PRICE — a later WS event may carry the
		// TradedPrice the first one lacked.
		if group.State != StatePendingEntry && group.State != StateAwaitingFillPrice {
			return // Already handled
		}

		// Execution-price resolution — STEP 1 (order WS): FilledPrice is the broker
		// WS TradedPrice (VWAP'd by the status service) — the actual execution price.
		// This is the only step on the WS hot path; it never makes a network call.
		// When it is absent (e.g. a MARKET order whose EXECUTED event omits a price)
		// we DEFER: park in AWAITING_FILL_PRICE so the fill-price retry worker
		// resolves via the trade book (STEP 2) and, past a deadline, the
		// buffer-stripped entry limit (STEP 3). This guarantees SL/TP come from the
		// real execution price, never the inflated entry-limit price.
		if order.FilledPrice == nil || *order.FilledPrice <= 0 {
			if group.State != StateAwaitingFillPrice {
				group.State = StateAwaitingFillPrice
				group.EntryFilledAt = time.Now()
				group.UpdatedAt = time.Now()
				log.Printf("[oco] Entry FILLED for group %s but WS carried no fill price — deferring SL/TP to retry worker (trade-book resolve)", group.GroupID)
			}
			return
		}

		fillPrice := *order.FilledPrice

		group.FilledQty = group.Quantity
		group.EntryFillPrice = fillPrice
		group.HighestPrice = fillPrice // initialize for trailing SL
		group.State = StatePlacingLegs
		group.UpdatedAt = time.Now()

		metrics.OCOFillPriceSource.WithLabelValues("order_ws").Inc()
		log.Printf("[oco] Entry FILLED for group %s at %.2f (source=order_ws) — placing SL+TP legs", group.GroupID, fillPrice)

		// Place SL and TP legs (in background goroutine so we don't block the WS handler)
		legCtx, legCancel := context.WithTimeout(context.Background(), brokerOpTimeout)
		go func() {
			defer legCancel()
			m.placeOCOLegs(legCtx, group)
		}()

	case "PARTIALLY TRADED", "PARTIALLY EXECUTED", "PARTIALLY_FILLED":
		filledQty := order.FilledQuantity
		if filledQty <= 0 {
			return
		}

		if group.State == StatePendingEntry && !group.PartialFillActive {
			// ── First partial fill: place SL+TP legs for filled qty ──
			log.Printf("[oco] Entry PARTIALLY FILLED for group %s: %d/%d — placing SL+TP legs for filled qty",
				group.GroupID, filledQty, group.Quantity)

			// Resolve fill price: prefer the real broker fill (FilledPrice =
			// TradedPrice); fall back to the buffer-stripped entry limit only when
			// no real price is available. Partial fills place legs immediately
			// rather than deferring (the partial-fill timer logic depends on it).
			fillPrice, source := wsFillOrStrippedLimit(order, group.OrderSide)
			if fillPrice <= 0 {
				log.Printf("[oco] WARNING: Partial fill price is 0 for group %s — cannot place legs", group.GroupID)
				return // Don't fail the group — wait for full fill or next partial with price
			}

			group.FilledQty = filledQty
			group.PartialFillActive = true
			group.EntryFillPrice = fillPrice
			group.HighestPrice = fillPrice
			group.State = StatePlacingLegs
			group.UpdatedAt = time.Now()
			metrics.OCOFillPriceSource.WithLabelValues(source).Inc()

			// Place SL+TP legs with partial qty, then start timeout timer
			legCtx, legCancel := context.WithTimeout(context.Background(), brokerOpTimeout)
			go func() {
				defer legCancel()
				m.placeOCOLegs(legCtx, group)

				// Start the partial fill timeout timer AFTER legs are placed.
				// If legs failed (state=FAILED), don't start the timer.
				mu := m.GetGroupMu(group.GroupID)
				mu.Lock()
				if group.State != StateFailed && group.PartialFillActive && !group.PartialFillTimerStarted {
					group.PartialFillTimerStarted = true
					mu.Unlock()
					m.startPartialFillTimer(group)
				} else {
					mu.Unlock()
				}
			}()

		} else if group.PartialFillActive {
			// ── Subsequent partial fill: modify SL+TP legs to new filled qty ──
			log.Printf("[oco] Entry additional partial fill for group %s: %d/%d (was %d) — modifying SL+TP legs",
				group.GroupID, filledQty, group.Quantity, group.FilledQty)

			group.FilledQty = filledQty
			group.UpdatedAt = time.Now()

			go m.modifyLegsQty(group, filledQty)
		}

	case "REJECTED", "A.REJECTED", "CANCELLED":
		// If partial fill was active and entry gets cancelled, keep the legs
		// for the already-filled qty — the position exists and needs protection.
		if group.PartialFillActive {
			log.Printf("[oco] Entry %s for group %s while partial fill active (filled=%d) — keeping legs, stopping timer",
				status, group.GroupID, group.FilledQty)
			if group.PartialFillCancelFunc != nil {
				group.PartialFillCancelFunc()
				group.PartialFillCancelFunc = nil
			}
			group.PartialFillActive = false
			group.PartialFillTimerStarted = false
			group.UpdatedAt = time.Now()
			return
		}

		log.Printf("[oco] Entry %s for group %s: %s", status, group.GroupID, safeString(order.RejectionReason))
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		m.scheduleCleanup(group)
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_entry_rejected", order)
		}
	}
}

// handleSLLegUpdate processes broker status updates for the SL leg.
func (m *OCOManager) handleSLLegUpdate(ctx context.Context, group *OCOGroup, order *models.Order, status string) {
	switch status {
	case "PENDING", "OPEN", "TRIGGER PENDING", "TRIGGER_PENDING", "AFTER MARKET ORDER REQ RECEIVED":
		// WS confirms SL leg is on the exchange — mark as confirmed
		if group.SLLegConfirmed {
			return // already confirmed
		}
		group.SLLegConfirmed = true
		group.UpdatedAt = time.Now()
		log.Printf("[oco] SL leg confirmed on exchange for group %s (broker=%s)", group.GroupID, group.SLBrokerID)
		m.checkLegsConfirmed(group)

	case "EXECUTED", "TRADED":
		if group.State != StateActive && group.State != StateLegsSubmitted {
			return
		}

		log.Printf("[oco] SL LEG EXECUTED for group %s — cancelling TP leg (broker=%s)", group.GroupID, group.TPBrokerID)
		group.State = StateSLTriggered
		group.UpdatedAt = time.Now()

		// Calculate P&L using FilledQty (accounts for partial fills)
		pnlQty := group.Quantity
		if group.FilledQty > 0 {
			pnlQty = group.FilledQty
		}
		if order.FilledPrice != nil {
			if group.OrderSide == "BUY" {
				group.PnL = (*order.FilledPrice - group.EntryFillPrice) * float64(pnlQty)
			} else {
				group.PnL = (group.EntryFillPrice - *order.FilledPrice) * float64(pnlQty)
			}
			// Mark the entry position closed in the DB at the SL leg's actual fill price.
			m.recordEntryExit(group, *order.FilledPrice, "STOP_LOSS", order.ExecutedAt)
		}

		// Cancel TP leg if one was placed; otherwise complete directly (SL-only mode)
		if group.TPBrokerID != "" {
			go m.cancelLeg(group, group.TPBrokerID, "TP", "SL leg executed (OCO)")
		} else {
			group.State = StateCompleted
			group.UpdatedAt = time.Now()
			log.Printf("[oco] Group %s COMPLETED via SL (no TP leg)", group.GroupID)
			if m.wsBroadcaster != nil {
				m.wsBroadcaster(group.UserID, "oco_completed", nil)
			}
		}

	case "REJECTED", "A.REJECTED":
		log.Printf("[oco] SL leg REJECTED for group %s — cancelling TP and marking failed", group.GroupID)
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		go m.cancelLeg(group, group.TPBrokerID, "TP", "SL leg rejected (OCO cleanup)")
		m.scheduleCleanup(group)

	case "PARTIALLY TRADED", "PARTIALLY EXECUTED", "PARTIALLY_FILLED":
		// SL leg partially filled — treat as triggered to prevent position mismatch.
		// Cancel TP immediately so we don't end up with both legs partially/fully filled.
		if group.State != StateActive && group.State != StateLegsSubmitted {
			return
		}
		log.Printf("[oco] SL LEG PARTIALLY FILLED for group %s (filled=%d) — cancelling TP leg to prevent mismatch",
			group.GroupID, order.FilledQuantity)
		group.State = StateSLTriggered
		group.UpdatedAt = time.Now()

		go m.cancelLeg(group, group.TPBrokerID, "TP", "SL leg partially filled (OCO)")

	case "CANCELLED":
		// User cancelled the SL leg — leave TP running independently.
		if group.State == StateActive || group.State == StateLegsSubmitted {
			log.Printf("[oco] SL leg CANCELLED externally for group %s — keeping TP active", group.GroupID)
			m.brokerIndex.Delete(group.SLBrokerID)
			group.SLBrokerID = ""
			group.SLLegConfirmed = false
			group.UpdatedAt = time.Now()
			if group.TPBrokerID == "" {
				group.State = StateCancelled
				m.scheduleCleanup(group)
			}
		}
	}
}

// handleTPLegUpdate processes broker status updates for the TP leg.
func (m *OCOManager) handleTPLegUpdate(ctx context.Context, group *OCOGroup, order *models.Order, status string) {
	switch status {
	case "PENDING", "OPEN", "TRIGGER PENDING", "TRIGGER_PENDING", "AFTER MARKET ORDER REQ RECEIVED":
		// WS confirms TP leg is on the exchange — mark as confirmed
		if group.TPLegConfirmed {
			return // already confirmed
		}
		group.TPLegConfirmed = true
		group.UpdatedAt = time.Now()
		log.Printf("[oco] TP leg confirmed on exchange for group %s (broker=%s)", group.GroupID, group.TPBrokerID)
		m.checkLegsConfirmed(group)

	case "EXECUTED", "TRADED":
		if group.State != StateActive && group.State != StateLegsSubmitted {
			return
		}

		log.Printf("[oco] TP LEG EXECUTED for group %s — cancelling SL leg (broker=%s)", group.GroupID, group.SLBrokerID)
		group.State = StateTPTriggered
		group.UpdatedAt = time.Now()

		// Calculate P&L using FilledQty (accounts for partial fills)
		pnlQty := group.Quantity
		if group.FilledQty > 0 {
			pnlQty = group.FilledQty
		}
		if order.FilledPrice != nil {
			if group.OrderSide == "BUY" {
				group.PnL = (*order.FilledPrice - group.EntryFillPrice) * float64(pnlQty)
			} else {
				group.PnL = (group.EntryFillPrice - *order.FilledPrice) * float64(pnlQty)
			}
			// Mark the entry position closed in the DB at the TP leg's actual fill price.
			m.recordEntryExit(group, *order.FilledPrice, "TAKE_PROFIT", order.ExecutedAt)
		}

		// Cancel SL leg if one was placed; otherwise complete directly (TP-only mode)
		if group.SLBrokerID != "" {
			go m.cancelLeg(group, group.SLBrokerID, "SL", "TP leg executed (OCO)")
		} else {
			group.State = StateCompleted
			group.UpdatedAt = time.Now()
			log.Printf("[oco] Group %s COMPLETED via TP (no SL leg)", group.GroupID)
			if m.wsBroadcaster != nil {
				m.wsBroadcaster(group.UserID, "oco_completed", nil)
			}
		}

	case "REJECTED", "A.REJECTED":
		log.Printf("[oco] WARNING: TP leg REJECTED for group %s — SL leg remains active (user has SL protection but NO take-profit)", group.GroupID)
		// Don't cancel SL — user at least has stop-loss protection.
		// Broadcast so the frontend can notify the user.
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_tp_rejected", order)
		}

	case "PARTIALLY TRADED", "PARTIALLY EXECUTED", "PARTIALLY_FILLED":
		// TP leg partially filled — treat as triggered to prevent position mismatch.
		// Cancel SL immediately so we don't end up with both legs partially/fully filled.
		if group.State != StateActive && group.State != StateLegsSubmitted {
			return
		}
		log.Printf("[oco] TP LEG PARTIALLY FILLED for group %s (filled=%d) — cancelling SL leg to prevent mismatch",
			group.GroupID, order.FilledQuantity)
		group.State = StateTPTriggered
		group.UpdatedAt = time.Now()

		go m.cancelLeg(group, group.SLBrokerID, "SL", "TP leg partially filled (OCO)")

	case "CANCELLED":
		// User cancelled the TP leg — leave SL running independently.
		if group.State == StateActive || group.State == StateLegsSubmitted {
			log.Printf("[oco] TP leg CANCELLED externally for group %s — keeping SL active", group.GroupID)
			m.brokerIndex.Delete(group.TPBrokerID)
			group.TPBrokerID = ""
			group.TPLegConfirmed = false
			group.UpdatedAt = time.Now()
			if group.SLBrokerID == "" {
				group.State = StateCancelled
				m.scheduleCleanup(group)
			}
		}
	}
}

// checkLegsConfirmed transitions from StateLegsSubmitted → StateActive
// when both legs (or only SL if TP wasn't submitted) have been confirmed
// on the exchange via broker WS status updates (PENDING/OPEN).
// Must be called while holding the group mutex.
func (m *OCOManager) checkLegsConfirmed(group *OCOGroup) {
	if group.State != StateLegsSubmitted {
		return
	}

	slOK := group.SLLegConfirmed || group.SLBrokerID == "" // no SL leg to confirm
	tpOK := group.TPLegConfirmed || group.TPBrokerID == "" // no TP leg to confirm

	if slOK && tpOK {
		group.State = StateActive
		group.UpdatedAt = time.Now()
		log.Printf("[oco] Group %s ACTIVE: both legs confirmed on exchange", group.GroupID)

		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_legs_confirmed", nil)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// STEP 2b: Execution-price resolution (fill-price retry worker)
// ════════════════════════════════════════════════════════════════════════════

// wsFillOrStrippedLimit returns the execution-price reference for SL/TP from an
// order whose entry has filled: the real broker fill price (preferred) or, only
// when no real price is available, the buffer-stripped entry limit. The ÷1.005
// strip is applied ONLY on the limit fallback — when an actual fill price exists
// it is used as-is so SL/TP are relative to the true execution price.
func wsFillOrStrippedLimit(order *models.Order, side string) (price float64, source string) {
	if order.FilledPrice != nil && *order.FilledPrice > 0 {
		return *order.FilledPrice, "order_ws"
	}
	if order.Price != nil && *order.Price > 0 {
		return stripEntryBuffer(*order.Price, side), "limit_fallback"
	}
	return 0, ""
}

// StartFillPriceRetry runs the background worker that resolves the execution
// price for groups parked in AWAITING_FILL_PRICE (entry filled, but the order WS
// carried no TradedPrice). Each tick it tries the trade book (STEP 2) for every
// awaiting group, and once a group passes the deadline with still no price it
// falls back to the buffer-stripped entry limit (STEP 3) so the position is never
// left without protective legs. Safe to call once at startup; returns immediately.
func (m *OCOManager) StartFillPriceRetry(ctx context.Context) {
	if m.fillRetryInterval <= 0 {
		m.fillRetryInterval = defaultFillPriceRetryInterval
	}
	if m.fillPriceDeadline <= 0 {
		m.fillPriceDeadline = defaultFillPriceDeadline
	}
	log.Printf("[oco] Fill-price retry worker started (interval=%v deadline=%v)", m.fillRetryInterval, m.fillPriceDeadline)
	go func() {
		defer ocoRecover("fillPriceRetryWorker")
		ticker := time.NewTicker(m.fillRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("[oco] Fill-price retry worker stopped")
				return
			case <-ticker.C:
				m.resolveAwaitingGroups(ctx)
			}
		}
	}()
}

// resolveAwaitingGroups processes one pass over all AWAITING_FILL_PRICE groups,
// bounding concurrent broker calls. Trade-book lookups are batched and cached
// per user by the fill cache, so many awaiting groups for the same user share a
// single broker round-trip.
func (m *OCOManager) resolveAwaitingGroups(ctx context.Context) {
	defer ocoRecover("resolveAwaitingGroups")
	var awaiting []*OCOGroup
	m.groups.Range(func(_, v any) bool {
		g := v.(*OCOGroup)
		if g.State == StateAwaitingFillPrice {
			awaiting = append(awaiting, g)
		}
		return true
	})
	metrics.OCOAwaitingFillPrice.Set(float64(len(awaiting)))
	if len(awaiting) == 0 {
		return
	}

	const maxConcurrent = 16
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for _, g := range awaiting {
		wg.Add(1)
		sem <- struct{}{}
		go func(group *OCOGroup) {
			defer wg.Done()
			defer func() { <-sem }()
			m.resolveAwaitingGroup(ctx, group)
		}(g)
	}
	wg.Wait()
}

// resolveAwaitingGroup resolves a single awaiting group's execution price and,
// when found (or once the deadline passes), transitions it to PLACING_LEGS and
// places the SL/TP legs. The trade-book call runs OUTSIDE the group mutex; the
// state transition is done under the mutex and re-checks the state in case a WS
// event resolved the group concurrently.
func (m *OCOManager) resolveAwaitingGroup(ctx context.Context, group *OCOGroup) {
	defer ocoRecover("resolveAwaitingGroup")
	auth := m.resolveAuth(ctx, group)
	if auth == nil {
		return
	}

	// STEP 2 (trade book) — off the hot path, batched/cached per user.
	var tbPrice float64
	if m.fillCache != nil && group.EntryBrokerID != "" {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		f, ok, err := m.fillCache.FillForOrder(cctx, auth, group.UserID, group.EntryBrokerID)
		cancel()
		if err != nil {
			log.Printf("[oco] fill-retry: trade-book fetch failed for group %s: %v", group.GroupID, err)
		} else if ok && f.Price > 0 {
			tbPrice = f.Price
		}
	}

	mu := m.GetGroupMu(group.GroupID)
	mu.Lock()
	if group.State != StateAwaitingFillPrice {
		mu.Unlock()
		return // resolved concurrently (e.g. a later WS event carried the price)
	}

	var fillPrice float64
	var source string
	switch {
	case tbPrice > 0:
		fillPrice, source = tbPrice, "trade_book"
	case !group.EntryFilledAt.IsZero() && time.Since(group.EntryFilledAt) >= m.fillPriceDeadline:
		// STEP 3 (deadline) — no real price after the deadline; place legs off the
		// buffer-stripped entry limit so the position is never left unprotected.
		if group.EntryLimitPrice <= 0 {
			log.Printf("[oco] fill-retry: CRITICAL group %s past deadline but no entry limit price known — cannot place legs (position UNPROTECTED)", group.GroupID)
			mu.Unlock()
			return
		}
		fillPrice = stripEntryBuffer(group.EntryLimitPrice, group.OrderSide)
		source = "limit_fallback"
		log.Printf("[oco] fill-retry: DEADLINE exceeded for group %s — placing SL/TP off stripped entry limit %.2f (UNVERIFIED fill price)", group.GroupID, fillPrice)
	default:
		mu.Unlock()
		return // still within deadline — try again next tick
	}

	group.FilledQty = group.Quantity
	group.EntryFillPrice = fillPrice
	group.HighestPrice = fillPrice
	group.State = StatePlacingLegs
	group.UpdatedAt = time.Now()
	mu.Unlock()

	metrics.OCOFillPriceSource.WithLabelValues(source).Inc()
	log.Printf("[oco] fill-retry: resolved group %s fill=%.2f (source=%s) — placing SL+TP legs", group.GroupID, fillPrice, source)

	legCtx, legCancel := context.WithTimeout(context.Background(), brokerOpTimeout)
	go func() {
		defer legCancel()
		m.placeOCOLegs(legCtx, group)
	}()
}

// ════════════════════════════════════════════════════════════════════════════
// STEP 3: Place SL + TP Legs After Entry Fill
// ════════════════════════════════════════════════════════════════════════════

// placeOCOLegs places SL and/or TP legs after the entry order fills.
// Either leg is skipped when its percentage is 0 (user disabled it).
// Both are placed in parallel when both are enabled.
func (m *OCOManager) placeOCOLegs(ctx context.Context, group *OCOGroup) {
	defer ocoRecover("placeOCOLegs")
	fillPrice := group.EntryFillPrice
	exitSide := group.ExitSide()

	hasSL := group.SLPercent > 0
	hasTP := group.TPPercent > 0

	if !hasSL && !hasTP {
		log.Printf("[oco] CRITICAL: Both SL and TP disabled for group %s — aborting legs (position unprotected)", group.GroupID)
		mu := m.GetGroupMu(group.GroupID)
		mu.Lock()
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		mu.Unlock()
		return
	}

	// Calculate and validate prices for enabled legs only
	var slTrigger, tpLimit float64

	if hasSL {
		slTrigger = group.CalculateSLFromFill(fillPrice)
		if group.OrderSide == "BUY" && slTrigger >= fillPrice {
			log.Printf("[oco] CRITICAL: SL trigger %.2f >= fill %.2f for BUY group %s (SLPct=%.1f%%) — aborting legs",
				slTrigger, fillPrice, group.GroupID, group.SLPercent)
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			group.State = StateFailed
			group.UpdatedAt = time.Now()
			mu.Unlock()
			return
		}
		if group.OrderSide == "SELL" && slTrigger <= fillPrice {
			log.Printf("[oco] CRITICAL: SL trigger %.2f <= fill %.2f for SELL group %s (SLPct=%.1f%%) — aborting legs",
				slTrigger, fillPrice, group.GroupID, group.SLPercent)
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			group.State = StateFailed
			group.UpdatedAt = time.Now()
			mu.Unlock()
			return
		}
	}

	if hasTP {
		tpLimit = group.CalculateTPFromFill(fillPrice)
		if group.OrderSide == "BUY" && tpLimit <= fillPrice {
			log.Printf("[oco] CRITICAL: TP limit %.2f <= fill %.2f for BUY group %s (TPPct=%.1f%%) — aborting legs",
				tpLimit, fillPrice, group.GroupID, group.TPPercent)
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			group.State = StateFailed
			group.UpdatedAt = time.Now()
			mu.Unlock()
			return
		}
		if group.OrderSide == "SELL" && tpLimit >= fillPrice {
			log.Printf("[oco] CRITICAL: TP limit %.2f >= fill %.2f for SELL group %s (TPPct=%.1f%%) — aborting legs",
				tpLimit, fillPrice, group.GroupID, group.TPPercent)
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			group.State = StateFailed
			group.UpdatedAt = time.Now()
			mu.Unlock()
			return
		}
	}

	log.Printf("[oco] Placing legs for group %s: fillPrice=%.2f hasSL=%v(%.1f%%) hasTP=%v(%.1f%%) side=%s",
		group.GroupID, fillPrice, hasSL, group.SLPercent, hasTP, group.TPPercent, exitSide)

	// Build order models for enabled legs; pre-confirm disabled legs so the
	// state machine can transition to ACTIVE without waiting for a WS event.
	var slOrder, tpOrder *models.Order

	if hasSL {
		slOrderID := uuid.New()
		slLimit := calcSLLimit(slTrigger, exitSide)
		group.SLOrderID = slOrderID
		group.SLTriggerPrice = slTrigger
		group.SLLimitPrice = slLimit
		slOrder = m.buildLegOrder(group, slOrderID, exitSide, models.OrderTypeStopLoss, &slLimit, &slTrigger, RoleSLLeg)
	} else {
		group.SLLegConfirmed = true // no SL leg — skip WS confirmation
	}

	if hasTP {
		tpOrderID := uuid.New()
		group.TPOrderID = tpOrderID
		group.TPLimitPrice = tpLimit
		tpOrder = m.buildLegOrder(group, tpOrderID, exitSide, models.OrderTypeLimit, &tpLimit, nil, RoleTPLeg)
	} else {
		group.TPLegConfirmed = true // no TP leg — skip WS confirmation
	}

	// Persist enabled leg orders to DB
	var dbWg sync.WaitGroup
	if slOrder != nil {
		dbWg.Add(1)
		go func() {
			defer dbWg.Done()
			defer ocoRecover("persistSLLegOrder")
			if err := m.repo.Create(ctx, slOrder); err != nil {
				log.Printf("[oco] Failed to persist SL leg order: %v", err)
			}
		}()
	}
	if tpOrder != nil {
		dbWg.Add(1)
		go func() {
			defer dbWg.Done()
			defer ocoRecover("persistTPLegOrder")
			if err := m.repo.Create(ctx, tpOrder); err != nil {
				log.Printf("[oco] Failed to persist TP leg order: %v", err)
			}
		}()
	}
	dbWg.Wait()

	// Place enabled legs at broker in parallel
	var wg sync.WaitGroup
	var slBrokerID, tpBrokerID string
	var slErr, tpErr error

	if slOrder != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// On panic, surface it as slErr so the caller's existing
			// leg-failure handling runs (rather than treating the leg as placed).
			defer func() {
				if r := recover(); r != nil {
					slErr = fmt.Errorf("panic placing SL leg: %v", r)
					log.Printf("[oco] PANIC RECOVERED placing SL leg for group %s: %v\n%s", group.GroupID, r, debug.Stack())
				}
			}()
			slBrokerID, slErr = m.placeLegWithRetry(ctx, slOrder, group.Auth, "SL", group.GroupID)
		}()
	}
	if tpOrder != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					tpErr = fmt.Errorf("panic placing TP leg: %v", r)
					log.Printf("[oco] PANIC RECOVERED placing TP leg for group %s: %v\n%s", group.GroupID, r, debug.Stack())
				}
			}()
			tpBrokerID, tpErr = m.placeLegWithRetry(ctx, tpOrder, group.Auth, "TP", group.GroupID)
		}()
	}
	wg.Wait()

	// Lock the group for state updates
	mu := m.GetGroupMu(group.GroupID)
	mu.Lock()
	defer mu.Unlock()

	// Handle placement results
	slFailed := slOrder != nil && slErr != nil
	tpFailed := tpOrder != nil && tpErr != nil

	if slFailed && tpFailed {
		log.Printf("[oco] CRITICAL: Both legs failed for group %s: SL=%v TP=%v — POSITION UNPROTECTED", group.GroupID, slErr, tpErr)
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		m.scheduleCleanup(group)
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_legs_failed", slOrder)
		}
		return
	}

	if slFailed {
		// SL failed — cancel any placed TP and fail (no downside protection)
		log.Printf("[oco] SL leg failed for group %s (no protection) — cancelling TP", group.GroupID)
		if tpBrokerID != "" {
			group.TPBrokerID = tpBrokerID
			m.brokerIndex.Store(tpBrokerID, group.GroupID)
		}
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		go m.cancelLeg(group, tpBrokerID, "TP", "SL leg failed (no protection)")
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_legs_failed", slOrder)
		}
		return
	}

	if tpFailed {
		// TP failed but SL placed — user has downside protection. Continue SL-only.
		log.Printf("[oco] TP leg failed for group %s — SL submitted, user protected (SL-only mode)", group.GroupID)
		if slBrokerID != "" {
			group.SLBrokerID = slBrokerID
			m.brokerIndex.Store(slBrokerID, group.GroupID)
			group.SLLegConfirmed = true // broker API accepted — pre-confirm to avoid stuck LEGS_SUBMITTED
		}
		group.TPLegConfirmed = true // no TP leg to wait for
		group.State = StateLegsSubmitted
		group.UpdatedAt = time.Now()
		m.checkLegsConfirmed(group)
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_tp_rejected", slOrder)
		}
		return
	}

	// All placed legs succeeded — register broker IDs and pre-confirm immediately.
	// Pre-confirming (rather than waiting for WS PENDING) eliminates the race window
	// where the WS event arrives before brokerIndex is populated and gets dropped
	// by HandleBrokerUpdate, causing the group to get stuck in LEGS_SUBMITTED forever
	// and miss the trailing SL monitor. WS PENDING events that arrive later are
	// idempotent — handleSLLegUpdate / handleTPLegUpdate return early when the
	// confirmed flags are already true.
	if slBrokerID != "" {
		group.SLBrokerID = slBrokerID
		m.brokerIndex.Store(slBrokerID, group.GroupID)
		group.SLLegConfirmed = true
	}
	if tpBrokerID != "" {
		group.TPBrokerID = tpBrokerID
		m.brokerIndex.Store(tpBrokerID, group.GroupID)
		group.TPLegConfirmed = true
	}
	group.State = StateLegsSubmitted
	group.UpdatedAt = time.Now()

	log.Printf("[oco] Group %s LEGS_SUBMITTED: SL(broker=%s trigger=%.2f) TP(broker=%s limit=%.2f)",
		group.GroupID, slBrokerID, slTrigger, tpBrokerID, tpLimit)

	// Transition to ACTIVE immediately — both legs accepted by broker API.
	m.checkLegsConfirmed(group)

	if m.wsBroadcaster != nil {
		m.wsBroadcaster(group.UserID, "oco_legs_submitted", slOrder)
	}
}

// buildLegOrder creates an Order model for an OCO leg.
func (m *OCOManager) buildLegOrder(
	group *OCOGroup,
	orderID uuid.UUID,
	side string,
	orderType models.OrderType,
	price *float64,
	triggerPrice *float64, // stopLoss field — used as trigger for SL orders
	role OCORole,
) *models.Order {
	groupID := group.GroupID
	// Use FilledQty for leg orders when partial fill is active (or completed).
	// For full fills, FilledQty == Quantity so this is always correct.
	legQty := group.Quantity
	if group.FilledQty > 0 {
		legQty = group.FilledQty
	}
	return &models.Order{
		OrderID:      orderID,
		UserID:       group.UserID,
		StrategyID:   group.StrategyID,
		StrategyName: group.StrategyName,
		EventID:      group.EventID,
		StockCode:    group.StockCode,
		Exchange:     models.Exchange(group.Exchange),
		Symbol:       group.Symbol,
		OrderType:    orderType,
		OrderSide:    models.OrderSide(side),
		Quantity:     legQty,
		Price:        price,
		StopLoss:     triggerPrice,
		Validity:     group.Validity,
		ProductType:  group.ProductType,
		Status:       models.StatusReceived,
		RiskApproved: true,
		IsPaperTrade: false,
		TradingMode:  "LIVE",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		BearerToken:  &group.Auth.BearerToken,
		AppId:        &group.Auth.AppId,
		Source:       &group.Auth.Source,
		// OCO fields
		OCOGroupID:    &groupID,
		OCORole:       stringPtr(string(role)),
		ParentOrderID: &group.EntryOrderID,
	}
}

// placeLegWithRetry places a leg order at broker with retry logic.
// On 401 (session expired), it refreshes auth from DB before retrying.
func (m *OCOManager) placeLegWithRetry(
	ctx context.Context,
	order *models.Order,
	auth *indiraClient.AuthContext,
	legName string,
	groupID uuid.UUID,
) (string, error) {
	var lastErr error
	currentAuth := auth
	for attempt := 0; attempt < maxLegPlacementRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)

			// On 401, refresh auth from DB before retrying with stale token.
			if isSessionExpiredError(lastErr) {
				newAuth := m.refreshAuth(ctx, order.UserID, currentAuth)
				if newAuth != nil {
					currentAuth = newAuth
					order.BearerToken = &currentAuth.BearerToken
					order.AppId = &currentAuth.AppId
					order.Source = &currentAuth.Source
				}
			}
		}

		brokerID, err := m.indiraClient.PlaceOrder(ctx, order, currentAuth)
		if err != nil {
			lastErr = err
			log.Printf("[oco] %s leg placement attempt %d failed for group %s: %v",
				legName, attempt+1, groupID, err)
			continue
		}

		// Update order with broker ID — synchronous write so the status service
		// can find this order by IndiraOrderID when the broker WS fires back.
		order.IndiraOrderID = &brokerID
		order.Status = models.StatusSubmitted
		now := time.Now()
		order.SubmittedAt = &now
		if err := m.repo.Update(ctx, order); err != nil {
			log.Printf("[oco] WARNING: failed to persist %s leg broker ID for group %s: %v", legName, groupID, err)
		}

		log.Printf("[oco] %s leg placed: broker=%s order=%s group=%s", legName, brokerID, order.OrderID, groupID)
		return brokerID, nil
	}

	// All retries exhausted
	order.Status = models.StatusFailed
	errMsg := fmt.Sprintf("%s leg placement failed after %d attempts: %v", legName, maxLegPlacementRetries, lastErr)
	order.ErrorMessage = &errMsg
	if err := m.repo.Update(ctx, order); err != nil {
		log.Printf("[oco] WARNING: failed to persist %s leg failure for group %s: %v", legName, groupID, err)
	}

	return "", fmt.Errorf(errMsg)
}

// ════════════════════════════════════════════════════════════════════════════
// Partial Fill: Timer, Modify, Cancel Remaining
// ════════════════════════════════════════════════════════════════════════════

// startPartialFillTimer starts a timer that waits partialFillTimeout for the
// remaining entry qty to fill. If it doesn't fill in time, the remaining
// entry order is cancelled and the SL/TP legs remain at the filled qty.
func (m *OCOManager) startPartialFillTimer(group *OCOGroup) {
	ctx, cancel := context.WithCancel(context.Background())

	mu := m.GetGroupMu(group.GroupID)
	mu.Lock()
	group.PartialFillCancelFunc = cancel
	mu.Unlock()

	go func() {
		defer ocoRecover("partialFillTimeout")
		select {
		case <-time.After(m.partialFillTimeout):
			// Timer expired — cancel remaining entry qty at broker
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			if !group.PartialFillActive {
				mu.Unlock()
				return // Already fully filled or cancelled — nothing to do
			}
			group.PartialFillActive = false
			group.PartialFillTimerStarted = false
			group.PartialFillCancelFunc = nil
			remainingQty := group.Quantity - group.FilledQty
			mu.Unlock()

			log.Printf("[oco] Partial fill timeout expired for group %s — cancelling remaining %d qty (filled=%d/%d)",
				group.GroupID, remainingQty, group.FilledQty, group.Quantity)

			m.cancelEntryRemaining(group)

			if m.wsBroadcaster != nil {
				m.wsBroadcaster(group.UserID, "oco_partial_fill_timeout", nil)
			}

		case <-ctx.Done():
			// Cancelled — full fill arrived or group terminated, no action needed
		}
	}()

	log.Printf("[oco] Partial fill timer started for group %s: %v timeout (filled=%d/%d)",
		group.GroupID, m.partialFillTimeout, group.FilledQty, group.Quantity)
}

// cancelEntryRemaining cancels the unfilled portion of the entry order at the broker.
// Unlike cancelLeg, this does NOT mark the group as completed — the SL/TP legs remain active.
func (m *OCOManager) cancelEntryRemaining(group *OCOGroup) {
	brokerID := group.EntryBrokerID
	if brokerID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), brokerOpTimeout)
	defer cancel()

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}

		authToUse := m.resolveAuth(ctx, group)
		err := m.indiraClient.CancelOrder(ctx, group.Exchange, brokerID, group.Symbol, authToUse)
		if err != nil {
			errLower := strings.ToLower(err.Error())
			// If already traded/cancelled, the remaining qty is gone — nothing to cancel
			if strings.Contains(errLower, "already") ||
				strings.Contains(errLower, "traded") ||
				strings.Contains(errLower, "executed") {
				log.Printf("[oco] Entry order already in terminal state for group %s — cancel not needed", group.GroupID)
				break
			}
			// On 401, invalidate + reload from DB once; next attempt uses the fresh token.
			if isSessionExpiredError(err) && m.credsCache != nil {
				if newAuth := m.refreshAuth(ctx, group.UserID, authToUse); newAuth != nil {
					group.Auth = newAuth
				}
				continue
			}
			log.Printf("[oco] Cancel remaining entry attempt %d failed (group=%s broker=%s): %v",
				attempt+1, group.GroupID, brokerID, err)
			continue
		}

		log.Printf("[oco] Remaining entry qty cancelled for group %s (broker=%s)", group.GroupID, brokerID)
		break
	}
}

// modifyLegsQty modifies the quantity on both SL and TP legs at the broker.
// Called when additional partial fills arrive or when the entry fully fills.
func (m *OCOManager) modifyLegsQty(group *OCOGroup, newQty int32) {
	defer ocoRecover("modifyLegsQty")
	ctx, cancel := context.WithTimeout(context.Background(), brokerOpTimeout)
	defer cancel()

	mu := m.GetGroupMu(group.GroupID)
	mu.Lock()
	slBrokerID := group.SLBrokerID
	tpBrokerID := group.TPBrokerID
	slTrigger := group.SLTriggerPrice
	tpLimit := group.TPLimitPrice
	mu.Unlock()

	log.Printf("[oco] Modifying SL+TP legs qty to %d for group %s", newQty, group.GroupID)

	// modifyWith401Retry wraps a single ModifyOrder call with a one-shot 401
	// refresh+retry, mirroring the pattern in ModifySLLeg. Without this the
	// first call after restart guarantees a failure when the stored token has
	// expired, leaving leg quantities desynced between our DB and the broker.
	modifyWith401Retry := func(label string, modOrder *models.Order) error {
		authToUse := m.resolveAuth(ctx, group)
		err := m.indiraClient.ModifyOrder(ctx, modOrder, authToUse)
		if err != nil && isSessionExpiredError(err) && m.credsCache != nil {
			if newAuth := m.refreshAuth(ctx, group.UserID, authToUse); newAuth != nil {
				group.Auth = newAuth
				err = m.indiraClient.ModifyOrder(ctx, modOrder, newAuth)
			}
		}
		return err
	}

	// Modify SL leg
	if slBrokerID != "" {
		slLimit := group.SLLimitPrice
		slOrder := &models.Order{
			OrderID:       group.SLOrderID,
			IndiraOrderID: &slBrokerID,
			StockCode:     group.StockCode,
			Exchange:      models.Exchange(group.Exchange),
			Symbol:        group.Symbol,
			OrderType:     models.OrderTypeStopLoss, // SL-L (algo compliance)
			OrderSide:     models.OrderSide(group.ExitSide()),
			Quantity:      newQty,
			Price:         &slLimit,
			StopLoss:      &slTrigger,
			Validity:      group.Validity,
			ProductType:   group.ProductType,
		}
		if err := modifyWith401Retry("SL", slOrder); err != nil {
			log.Printf("[oco] Failed to modify SL leg qty to %d for group %s: %v", newQty, group.GroupID, err)
		} else {
			log.Printf("[oco] SL leg qty modified to %d for group %s (broker=%s)", newQty, group.GroupID, slBrokerID)
			m.dbUpdateAsync(slOrder, fmt.Sprintf("SL leg qty modify group=%s", group.GroupID))
		}
	}

	// Modify TP leg
	if tpBrokerID != "" {
		tpOrder := &models.Order{
			OrderID:       group.TPOrderID,
			IndiraOrderID: &tpBrokerID,
			StockCode:     group.StockCode,
			Exchange:      models.Exchange(group.Exchange),
			Symbol:        group.Symbol,
			OrderType:     models.OrderTypeLimit,
			OrderSide:     models.OrderSide(group.ExitSide()),
			Quantity:      newQty,
			Price:         &tpLimit,
			Validity:      group.Validity,
			ProductType:   group.ProductType,
		}
		if err := modifyWith401Retry("TP", tpOrder); err != nil {
			log.Printf("[oco] Failed to modify TP leg qty to %d for group %s: %v", newQty, group.GroupID, err)
		} else {
			log.Printf("[oco] TP leg qty modified to %d for group %s (broker=%s)", newQty, group.GroupID, tpBrokerID)
			m.dbUpdateAsync(tpOrder, fmt.Sprintf("TP leg qty modify group=%s", group.GroupID))
		}
	}

	// Update in-memory FilledQty
	mu.Lock()
	group.FilledQty = newQty
	group.UpdatedAt = time.Now()
	mu.Unlock()
}

// ════════════════════════════════════════════════════════════════════════════
// Cancel + Cleanup
// ════════════════════════════════════════════════════════════════════════════

// cancelLeg cancels a single leg order at the broker with retry and timeout.
func (m *OCOManager) cancelLeg(group *OCOGroup, brokerID string, legName string, reason string) {
	// Always invoked as a goroutine — recover so a panic here (e.g. a nil auth or
	// broker client) can never crash the whole process and abandon everything else.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[oco] PANIC in cancelLeg (group=%s leg=%s broker=%s): %v", group.GroupID, legName, brokerID, r)
		}
	}()
	if brokerID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), brokerOpTimeout)
	defer cancel()

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}

		// Pull a fresher token from the cache before each attempt so stale auth
		// (e.g. on restart-reconstructed groups) doesn't burn a full 3-retry cycle.
		authToUse := m.resolveAuth(ctx, group)
		err := m.indiraClient.CancelOrder(ctx, group.Exchange, brokerID, group.Symbol, authToUse)
		if err != nil {
			log.Printf("[oco] Cancel %s leg attempt %d failed (group=%s broker=%s): %v",
				legName, attempt+1, group.GroupID, brokerID, err)
			// On 401, invalidate + reload from DB once; the next attempt picks up the
			// new token via resolveAuth (after Invalidate, cache reload returns fresh).
			if isSessionExpiredError(err) && m.credsCache != nil {
				if newAuth := m.refreshAuth(ctx, group.UserID, authToUse); newAuth != nil {
					group.Auth = newAuth
				}
				continue
			}
			// If error contains "already traded" or "already cancelled", stop retrying
			if strings.Contains(strings.ToLower(err.Error()), "already") ||
				strings.Contains(strings.ToLower(err.Error()), "traded") ||
				strings.Contains(strings.ToLower(err.Error()), "executed") {
				log.Printf("[oco] %s leg already in terminal state — cancel not needed", legName)
				break
			}
			continue
		}

		log.Printf("[oco] %s leg cancelled: broker=%s group=%s reason=%s", legName, brokerID, group.GroupID, reason)
		break
	}

	// Mark group as completed
	group.State = StateCompleted
	group.UpdatedAt = time.Now()

	log.Printf("[oco] Group %s COMPLETED (PnL=%.2f)", group.GroupID, group.PnL)

	// Broadcast to frontend
	if m.wsBroadcaster != nil {
		m.wsBroadcaster(group.UserID, "oco_completed", nil)
	}
}

// scheduleCleanup removes a terminal group from memory indices after a delay.
// Uses time.AfterFunc so it doesn't block a goroutine during the wait.
func (m *OCOManager) scheduleCleanup(group *OCOGroup) {
	time.AfterFunc(30*time.Second, func() {
		m.groups.Delete(group.GroupID)
		if group.EntryBrokerID != "" {
			m.brokerIndex.Delete(group.EntryBrokerID)
		}
		if group.SLBrokerID != "" {
			m.brokerIndex.Delete(group.SLBrokerID)
		}
		if group.TPBrokerID != "" {
			m.brokerIndex.Delete(group.TPBrokerID)
		}
		m.groupMu.Delete(group.GroupID.String())

		log.Printf("[oco] Cleaned up group %s from memory", group.GroupID)
	})
}

// ════════════════════════════════════════════════════════════════════════════
// Modify SL (used by Trailing Monitor)
// ════════════════════════════════════════════════════════════════════════════

// ModifySLLeg modifies the SL leg order at the broker with a new trigger/limit.
// Called by the trailing SL monitor when LTP makes a new high.
func (m *OCOManager) ModifySLLeg(ctx context.Context, group *OCOGroup, newTrigger, newLimit, newHighestPrice float64) error {
	mu := m.GetGroupMu(group.GroupID)
	mu.Lock()
	defer mu.Unlock()

	if group.State != StateActive || group.SLBrokerID == "" {
		return fmt.Errorf("group %s not in ACTIVE state or SL broker ID missing", group.GroupID)
	}

	// Use FilledQty for trailing SL modify (matches the qty legs were placed with)
	legQty := group.Quantity
	if group.FilledQty > 0 {
		legQty = group.FilledQty
	}

	// Build a modify order
	slOrder := &models.Order{
		OrderID:       group.SLOrderID,
		IndiraOrderID: &group.SLBrokerID,
		StockCode:     group.StockCode,
		Exchange:      models.Exchange(group.Exchange),
		Symbol:        group.Symbol,
		OrderType:     models.OrderTypeStopLoss, // SL-L (algo compliance)
		OrderSide:     models.OrderSide(group.ExitSide()),
		Quantity:      legQty,
		Price:         &newLimit, // SL-L limit = newTrigger ± ocoSLSlippagePct
		StopLoss:      &newTrigger,
		Validity:      group.Validity,
		ProductType:   group.ProductType,
	}

	// Proactively pull a fresher token from the cache if one exists — saves a
	// guaranteed-401 round-trip for groups reconstructed from DB after restart.
	currentAuth := m.resolveAuth(ctx, group)
	modifyErr := m.indiraClient.ModifyOrder(ctx, slOrder, currentAuth)
	if modifyErr != nil && isSessionExpiredError(modifyErr) && m.credsCache != nil {
		// 401: refresh credentials and retry once.
		// We hold the group mutex here; refreshAuth may do a DB read but that is
		// bounded by the caller's 10-second context so it won't block indefinitely.
		newAuth := m.refreshAuth(ctx, group.UserID, currentAuth)
		if newAuth != nil {
			group.Auth = newAuth // update so future modify/cancel calls use fresh token
			currentAuth = newAuth
			modifyErr = m.indiraClient.ModifyOrder(ctx, slOrder, currentAuth)
		}
	}
	if modifyErr != nil {
		return fmt.Errorf("modify SL leg failed: %w", modifyErr)
	}

	// Update in-memory state ATOMICALLY — both SLTriggerPrice and HighestPrice
	// must be updated together, and ONLY after the broker modify succeeds.
	// If HighestPrice is advanced before ModifyOrder and the call fails,
	// subsequent trailing calculations use a stale SLTriggerPrice against an
	// advanced HighestPrice, producing tiny changePct values that never reach
	// the TrailingSLPct threshold — permanently stalling the trailing SL.
	oldTrigger := group.SLTriggerPrice
	group.SLTriggerPrice = newTrigger
	group.SLLimitPrice = newLimit
	if newHighestPrice > 0 {
		group.HighestPrice = newHighestPrice
	}
	group.UpdatedAt = time.Now()

	log.Printf("[oco] Trailing SL modified for group %s: trigger %.2f→%.2f limit=%.2f highest=%.2f",
		group.GroupID, oldTrigger, newTrigger, newLimit, group.HighestPrice)

	// Persist to DB synchronously — if the service crashes before this write,
	// on restart we'd reload the old SL trigger while the broker has the new one.
	if err := m.repo.Update(ctx, slOrder); err != nil {
		log.Printf("[oco] WARNING: failed to persist SL modify for group %s: %v", group.GroupID, err)
	}

	return nil
}

// ════════════════════════════════════════════════════════════════════════════
// Restart Recovery
// ════════════════════════════════════════════════════════════════════════════

// Reload reconstructs in-memory OCO state from the database.
// Called once on service startup to recover active OCO groups.
func (m *OCOManager) Reload(ctx context.Context) error {
	orders, err := m.repo.GetActiveOCOOrders(ctx)
	if err != nil {
		return fmt.Errorf("failed to load active OCO orders: %w", err)
	}

	if len(orders) == 0 {
		log.Println("[oco] No active OCO orders to reload")
		return nil
	}

	// Group orders by oco_group_id
	groupMap := make(map[uuid.UUID][]*models.Order)
	for _, o := range orders {
		if o.OCOGroupID != nil {
			groupMap[*o.OCOGroupID] = append(groupMap[*o.OCOGroupID], o)
		}
	}

	loaded := 0
	for groupID, groupOrders := range groupMap {
		group := m.reconstructGroup(groupID, groupOrders)
		if group == nil {
			continue
		}

		m.groups.Store(groupID, group)
		if group.EntryBrokerID != "" {
			m.brokerIndex.Store(group.EntryBrokerID, groupID)
		}
		if group.SLBrokerID != "" {
			m.brokerIndex.Store(group.SLBrokerID, groupID)
		}
		if group.TPBrokerID != "" {
			m.brokerIndex.Store(group.TPBrokerID, groupID)
		}

		// Restart partial fill timer for groups that were mid-partial-fill when service stopped.
		if group.PartialFillActive {
			group.PartialFillTimerStarted = true
			m.startPartialFillTimer(group)
		}

		// Recovery: if the entry filled before the crash but legs were never
		// created in the DB, inferState returns StatePlacingLegs. Without this
		// re-trigger the position stays naked forever — handleEntryUpdate early-
		// returns once state != StatePendingEntry, so no future WS event will
		// place the legs. placeOCOLegs is safe here because the only way to
		// reach this branch is slStatus=="" && tpStatus=="" (no legs in DB),
		// so no duplicate legs can be created.
		if group.State == StatePlacingLegs {
			if group.EntryFillPrice <= 0 {
				log.Printf("[oco] Reload: group %s in StatePlacingLegs but EntryFillPrice unknown — cannot recover legs", group.GroupID)
			} else if group.Auth == nil {
				log.Printf("[oco] Reload: group %s in StatePlacingLegs but auth missing — cannot recover legs", group.GroupID)
			} else {
				log.Printf("[oco] Reload recovery: group %s in StatePlacingLegs — re-invoking placeOCOLegs (fill=%.2f)", group.GroupID, group.EntryFillPrice)
				g := group
				go func() {
					legCtx, legCancel := context.WithTimeout(context.Background(), brokerOpTimeout)
					defer legCancel()
					m.placeOCOLegs(legCtx, g)
				}()
			}
		}

		loaded++
	}

	log.Printf("[oco] Reloaded %d active OCO groups from DB", loaded)
	return nil
}

// reconstructGroup rebuilds an OCOGroup from its constituent orders.
func (m *OCOManager) reconstructGroup(groupID uuid.UUID, orders []*models.Order) *OCOGroup {
	group := &OCOGroup{
		GroupID: groupID,
	}

	for _, o := range orders {
		role := ""
		if o.OCORole != nil {
			role = *o.OCORole
		}

		switch OCORole(role) {
		case RoleEntry:
			group.UserID = o.UserID
			group.EntryOrderID = o.OrderID
			group.Symbol = o.Symbol
			group.Exchange = string(o.Exchange)
			group.StockCode = o.StockCode
			group.Quantity = o.Quantity
			group.OrderSide = string(o.OrderSide)
			// Restore FilledQty from DB for partial fill recovery
			if o.FilledQuantity > 0 {
				group.FilledQty = o.FilledQuantity
			}
			group.ProductType = o.ProductType
			group.Validity = o.Validity
			group.StrategyID = o.StrategyID
			group.StrategyName = o.StrategyName
			group.EventID = o.EventID
			group.CreatedAt = o.CreatedAt
			if o.IndiraOrderID != nil {
				group.EntryBrokerID = *o.IndiraOrderID
			}
			// Prefer the actual execution price (FilledPrice from broker WS TradedPrice)
			// over the LIMIT price the order was placed at. order.Price for BUY OCO entries
			// is signal_price × 1.005 — using it raw as the SL/TP reference or HighestPrice
			// silently desensitizes the trailing SL after every restart. Record the raw
			// limit as EntryLimitPrice, and fall back to its buffer-stripped value only when
			// FilledPrice is missing.
			if o.Price != nil && *o.Price > 0 {
				group.EntryLimitPrice = *o.Price
			}
			if o.FilledPrice != nil && *o.FilledPrice > 0 {
				group.EntryFillPrice = *o.FilledPrice
				group.HighestPrice = *o.FilledPrice
			} else if o.Price != nil && *o.Price > 0 {
				stripped := stripEntryBuffer(*o.Price, string(o.OrderSide))
				group.EntryFillPrice = stripped
				group.HighestPrice = stripped
			}
			if o.StopLoss != nil {
				group.SLPercent = *o.StopLoss // stored as percentage in entry
			}
			if o.TakeProfit != nil {
				group.TPPercent = *o.TakeProfit
			}
			if o.StopLossType != nil && *o.StopLossType == "TRAILING" {
				group.TrailingSL = true
			}
			if o.TrailingSLPct != nil {
				group.TrailingSLPct = *o.TrailingSLPct
			}
			if o.HighestPrice != nil {
				group.HighestPrice = *o.HighestPrice
			}
			// Reconstruct auth from order
			if o.BearerToken != nil && o.AppId != nil && o.Source != nil {
				group.Auth = &indiraClient.AuthContext{
					UserId:      o.UserID,
					BearerToken: *o.BearerToken,
					AppId:       *o.AppId,
					Source:      *o.Source,
				}
			}

		case RoleSLLeg:
			group.SLOrderID = o.OrderID
			if o.IndiraOrderID != nil {
				group.SLBrokerID = *o.IndiraOrderID
			}
			if o.StopLoss != nil {
				group.SLTriggerPrice = *o.StopLoss
			}
			if o.Price != nil {
				group.SLLimitPrice = *o.Price
			}

		case RoleTPLeg:
			group.TPOrderID = o.OrderID
			if o.IndiraOrderID != nil {
				group.TPBrokerID = *o.IndiraOrderID
			}
			if o.Price != nil {
				group.TPLimitPrice = *o.Price
			}
		}
	}

	// Determine state from order statuses
	group.State = m.inferState(orders)
	if group.State.IsTerminal() {
		return nil // Don't load terminal groups
	}

	// Detect partial fill state: entry is partially filled AND legs exist.
	// On restart, we treat this as an active partial fill and restart the timer
	// to cancel any remaining unfilled entry qty.
	if group.FilledQty > 0 && group.FilledQty < group.Quantity && group.SLBrokerID != "" {
		group.PartialFillActive = true
		log.Printf("[oco] Restored partial fill state for group %s (filled=%d/%d) — will restart timer",
			group.GroupID, group.FilledQty, group.Quantity)
	}

	group.UpdatedAt = time.Now()
	return group
}

// inferState determines the OCO group state from individual order statuses.
func (m *OCOManager) inferState(orders []*models.Order) OCOState {
	roleStatus := make(map[string]models.OrderStatus)
	for _, o := range orders {
		if o.OCORole != nil {
			roleStatus[*o.OCORole] = o.Status
		}
	}

	entryStatus := roleStatus[string(RoleEntry)]
	slStatus := roleStatus[string(RoleSLLeg)]
	tpStatus := roleStatus[string(RoleTPLeg)]

	// If entry not filled yet (and not partially filled with legs)
	if !models.IsFilledStatus(entryStatus) && !models.IsTerminalStatus(entryStatus) &&
		!models.IsPartiallyFilledStatus(entryStatus) {
		return StatePendingEntry
	}

	// If entry is terminal but not filled (and not partially filled)
	if models.IsTerminalStatus(entryStatus) && !models.IsFilledStatus(entryStatus) &&
		!models.IsPartiallyFilledStatus(entryStatus) {
		return StateFailed
	}

	// Entry is filled — check legs
	if slStatus == "" && tpStatus == "" {
		return StatePlacingLegs // legs not created yet
	}

	// If either leg is filled/executed
	if models.IsFilledStatus(slStatus) {
		return StateCompleted
	}
	if models.IsFilledStatus(tpStatus) {
		return StateCompleted
	}

	// Both legs exist and not filled.
	// If legs are in SUBMITTED status (broker API returned ordId but no WS confirmation yet),
	// return StateLegsSubmitted. If PENDING/OPEN, they're confirmed on exchange → ACTIVE.
	slConfirmed := isConfirmedOnExchange(slStatus)
	tpConfirmed := isConfirmedOnExchange(tpStatus) || tpStatus == ""
	if slConfirmed && tpConfirmed {
		return StateActive
	}
	return StateLegsSubmitted
}

// ════════════════════════════════════════════════════════════════════════════
// Getters (for trailing monitor and REST endpoints)
// ════════════════════════════════════════════════════════════════════════════

// GetActiveGroups returns all OCO groups in ACTIVE state.
// Used by the trailing SL monitor to know which groups to watch.
func (m *OCOManager) GetActiveGroups() []*OCOGroup {
	var active []*OCOGroup
	m.groups.Range(func(key, value any) bool {
		group := value.(*OCOGroup)
		if group.State == StateActive {
			active = append(active, group)
		}
		return true
	})
	return active
}

// GetGroupsByUser returns all non-terminal OCO groups for a user.
func (m *OCOManager) GetGroupsByUser(userID string) []*OCOGroup {
	var result []*OCOGroup
	m.groups.Range(func(key, value any) bool {
		group := value.(*OCOGroup)
		if group.UserID == userID && !group.State.IsTerminal() {
			result = append(result, group)
		}
		return true
	})
	return result
}

// GetGroup returns a single OCO group by ID.
func (m *OCOManager) GetGroup(groupID uuid.UUID) (*OCOGroup, bool) {
	val, ok := m.groups.Load(groupID)
	if !ok {
		return nil, false
	}
	return val.(*OCOGroup), true
}

// ActiveCount returns the number of active (non-terminal) OCO groups.
func (m *OCOManager) ActiveCount() int {
	count := 0
	m.groups.Range(func(key, value any) bool {
		group := value.(*OCOGroup)
		if !group.State.IsTerminal() {
			count++
		}
		return true
	})
	return count
}

// CancelGroup cancels all active orders in an OCO group.
func (m *OCOManager) CancelGroup(ctx context.Context, groupID uuid.UUID) error {
	val, ok := m.groups.Load(groupID)
	if !ok {
		return fmt.Errorf("OCO group %s not found", groupID)
	}
	group := val.(*OCOGroup)

	mu := m.GetGroupMu(groupID)
	mu.Lock()
	defer mu.Unlock()

	if group.State.IsTerminal() {
		return fmt.Errorf("OCO group %s already in terminal state: %s", groupID, group.State)
	}

	log.Printf("[oco] User-initiated cancel for group %s", groupID)

	// Cancel all non-terminal broker orders
	if group.State == StatePendingEntry && group.EntryBrokerID != "" {
		go m.cancelLeg(group, group.EntryBrokerID, "ENTRY", "User cancelled OCO")
	}
	if group.State == StateActive || group.State == StateLegsSubmitted {
		if group.SLBrokerID != "" {
			go m.cancelLeg(group, group.SLBrokerID, "SL", "User cancelled OCO")
		}
		if group.TPBrokerID != "" {
			go m.cancelLeg(group, group.TPBrokerID, "TP", "User cancelled OCO")
		}
	}

	group.State = StateCancelled
	group.UpdatedAt = time.Now()
	m.scheduleCleanup(group)

	return nil
}

// CancelAllGroupsByUser cancels every non-terminal OCO group for a user.
// Called on force-exit-all or when a manual position exit is detected.
func (m *OCOManager) CancelAllGroupsByUser(ctx context.Context, userID string) {
	groups := m.GetGroupsByUser(userID)
	if len(groups) == 0 {
		return
	}

	log.Printf("[oco] Cancelling all %d active OCO groups for user %s (force exit / manual exit)", len(groups), userID)
	for _, group := range groups {
		if err := m.CancelGroup(ctx, group.GroupID); err != nil {
			log.Printf("[oco] Failed to cancel group %s: %v", group.GroupID, err)
		}
	}
}

// CancelGroupsByStrategy cancels all non-terminal OCO groups for a user+strategy.
// Called when the user force-exits all positions for a specific strategy.
func (m *OCOManager) CancelGroupsByStrategy(ctx context.Context, userID, strategyID string) {
	defer ocoRecover("CancelGroupsByStrategy")
	var toCancel []uuid.UUID
	m.groups.Range(func(key, value any) bool {
		group := value.(*OCOGroup)
		if group.UserID == userID && group.StrategyID == strategyID && !group.State.IsTerminal() {
			toCancel = append(toCancel, group.GroupID)
		}
		return true
	})

	if len(toCancel) == 0 {
		return
	}

	log.Printf("[oco] Strategy exit: cancelling %d OCO groups for user %s strategy %s", len(toCancel), userID, strategyID)
	for _, gid := range toCancel {
		if err := m.CancelGroup(ctx, gid); err != nil {
			log.Printf("[oco] Failed to cancel group %s: %v", gid, err)
		}
	}
}

// CancelGroupsBySymbol cancels every non-terminal OCO group for (userID, symbol).
// Called when a manual exit is detected on the broker terminal — the underlying
// position is gone, so any open SL/TP legs (across all strategies trading this
// symbol) must be cancelled to prevent orphaned orders firing into a flat book.
//
// Per-strategy cancellation (e.g. strategy delete/deactivate) goes through
// CancelGroupsByStrategy and does NOT call this function.
func (m *OCOManager) CancelGroupsBySymbol(ctx context.Context, userID string, symbol string) {
	defer ocoRecover("CancelGroupsBySymbol")
	var toCancel []uuid.UUID
	m.groups.Range(func(key, value any) bool {
		group := value.(*OCOGroup)
		if group.UserID == userID && group.Symbol == symbol && !group.State.IsTerminal() {
			toCancel = append(toCancel, group.GroupID)
		}
		return true
	})

	if len(toCancel) == 0 {
		return
	}

	log.Printf("[oco] Manual exit detected: cancelling %d OCO group(s) for %s/%s",
		len(toCancel), userID, symbol)
	for _, gid := range toCancel {
		if err := m.CancelGroup(ctx, gid); err != nil {
			log.Printf("[oco] Failed to cancel group %s: %v", gid, err)
		}
	}
}

// IsKnownBrokerID returns true if the given broker order ID belongs to an
// active OCO group in memory. Used by the status service to avoid false
// manual-exit detection when the DB write hasn't completed yet.
func (m *OCOManager) IsKnownBrokerID(brokerID string) bool {
	_, ok := m.brokerIndex.Load(brokerID)
	return ok
}

// ════════════════════════════════════════════════════════════════════════════
// Adopt Existing Order Into OCO
// ════════════════════════════════════════════════════════════════════════════

// AdoptOrder wraps an already-placed broker order into an OCO group so that
// when the broker WS sends EXECUTED, handleEntryUpdate fires and places
// SL/TP legs from the actual fill price.
//
// Used when an order was placed via the default executor path and needs OCO
// management retroactively — either because trailing SL auth was unavailable
// at signal time, or for fixed SL/TP orders that go through Route 3.
//
// trailingSL=true enables the trailing SL monitor; false places a fixed SL-M
// that is never modified after placement.
func (m *OCOManager) AdoptOrder(
	order *models.Order,
	slPercent float64,
	tpPercent float64,
	trailingSL bool,
	trailingSLPct float64,
	auth *indiraClient.AuthContext,
) {
	if order.IndiraOrderID == nil || *order.IndiraOrderID == "" {
		log.Printf("[oco] AdoptOrder: no broker ID on order %s — skipping", order.OrderID)
		return
	}
	brokerID := *order.IndiraOrderID

	groupID := uuid.New()
	// NOTE: Do NOT default trailingSLPct to slPercent — see CreateOCOEntry comment.
	// CalculateTrailingSL already defaults to 0.1% when TrailingSLPct <= 0.

	group := &OCOGroup{
		GroupID:       groupID,
		UserID:        order.UserID,
		EntryOrderID:  order.OrderID,
		EntryBrokerID: brokerID,
		SLPercent:     slPercent,
		TPPercent:     tpPercent,
		TrailingSL:    trailingSL,
		TrailingSLPct: trailingSLPct,
		State:         StatePendingEntry,
		Symbol:        order.Symbol,
		Exchange:      string(order.Exchange),
		StockCode:     order.StockCode,
		Quantity:      order.Quantity,
		OrderSide:     string(order.OrderSide),
		Auth:          auth,
		ProductType:   "INTRADAY",
		Validity:      "DAY",
		StrategyID:    order.StrategyID,
		EventID:       order.EventID,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	// Record the entry limit price as the last-resort SL/TP reference (used
	// buffer-stripped only if the real fill price can't be resolved from WS/TB).
	if order.Price != nil && *order.Price > 0 {
		group.EntryLimitPrice = *order.Price
	}

	// Tag the order in DB so HandleBrokerUpdate can find it and reconstructGroup
	// can restore TrailingSL after a service restart.
	gid := groupID
	role := string(RoleEntry)
	slType := "FIXED"
	if trailingSL {
		slType = "TRAILING"
	}
	order.OCOGroupID = &gid
	order.OCORole = &role
	order.StopLossType = &slType
	if trailingSLPct > 0 {
		order.TrailingSLPct = &trailingSLPct
	}
	// Store SL/TP as PERCENT on the entry order (mirrors PlaceEntryWithOCO). These
	// are set only AFTER the broker placement, so the entry was never sent with a
	// triggerPrice, and they let reconstructGroup restore SLPercent/TPPercent after
	// a service restart. NOTE: these are percentages, not prices.
	slPct := slPercent
	tpPct := tpPercent
	order.StopLoss = &slPct
	order.TakeProfit = &tpPct
	go func() {
		defer ocoRecover("adoptOrderTag")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.repo.UpdateOCOTag(ctx, order.OrderID, groupID, role, slPercent, tpPercent); err != nil {
			log.Printf("[oco] AdoptOrder: failed to tag order %s with OCO group: %v", order.OrderID, err)
		}
	}()

	// Register in memory BEFORE any WS event can arrive
	m.groups.Store(groupID, group)
	m.brokerIndex.Store(brokerID, groupID)

	log.Printf("[oco] Adopted order %s (broker=%s) into OCO group %s: symbol=%s SL=%.1f%% TP=%.1f%% trailing=%v",
		order.OrderID, brokerID, groupID, order.Symbol, slPercent, tpPercent, trailingSL)

	// Ensure the broker order-WS subscription is live so the EXECUTED event that
	// places these SL/TP legs is consumed. The price-monitor → AdoptOrder path does
	// not otherwise (re)subscribe, so a user whose connection was idle-swept between
	// signal time and entry placement would never get protective legs. The
	// trade-book fill reconciler is the backstop if this subscription still misses.
	if m.wsEnsure != nil && auth != nil {
		m.wsEnsure(order.UserID, auth)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════════════════

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// isConfirmedOnExchange returns true if the order status indicates
// the order is live on the exchange (not just submitted to broker API).
func isConfirmedOnExchange(s models.OrderStatus) bool {
	switch strings.ToUpper(string(s)) {
	case "PENDING", "OPEN", "TRIGGER PENDING", "TRIGGER_PENDING",
		"AFTER MARKET ORDER REQ RECEIVED", "PARTIALLY_FILLED", "PARTIALLY TRADED":
		return true
	}
	return false
}

func safeString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
