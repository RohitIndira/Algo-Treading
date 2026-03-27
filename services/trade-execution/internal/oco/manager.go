// Package oco implements a custom One-Cancels-the-Other order management system.
//
// Architecture overview:
//
//	                   ┌─────────────────────┐
//	 TradeSignal ──→   │    OCO Manager       │
//	                   │  (in-memory registry │
//	                   │   + broker ID index) │
//	                   └──────────┬──────────┘
//	                              │
//	    ┌──────────┬──────────────┼───────────────────┐
//	    ▼          ▼              ▼                    ▼
//	 PlaceEntry  OnFill →     HandleBrokerUpdate   TrailingMonitor
//	 (SL order)  PlaceLegs   (cancel counterpart)  (ModifyOrder)
//
// All state is in-memory (sync.Map) for O(1) lookups from broker WS events.
// State changes are persisted to PostgreSQL asynchronously.
package oco

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

// maxEntryRetries is how many times we retry placing the OCO entry order on transient errors (e.g. 401).
const maxEntryRetries = 2

// maxLegPlacementRetries is how many times we retry placing SL/TP legs after entry fill.
const maxLegPlacementRetries = 3

// brokerOpTimeout is the max duration for any single broker API call (place, cancel, modify).
const brokerOpTimeout = 30 * time.Second

// dbRetryAttempts is how many times async DB updates are retried before giving up.
const dbRetryAttempts = 3

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
		repo:         repo,
		indiraClient: indiraClient,
	}
}

// SetWSBroadcaster wires the frontend WS push callback.
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

	// Calculate entry limit price: 0.5% buffer from trigger
	var entryLimitPrice float64
	if orderSide == "BUY" {
		entryLimitPrice = roundNSE(entryTriggerPrice * 1.005)
	} else {
		entryLimitPrice = roundNSE(entryTriggerPrice * 0.995)
	}

	if productType == "" {
		productType = "INTRADAY"
	}
	if trailingSLPct <= 0 {
		trailingSLPct = slPercent
	}

	// Create the OCO group
	group := &OCOGroup{
		GroupID:       groupID,
		UserID:        userID,
		EntryOrderID:  entryOrderID,
		SLPercent:     slPercent,
		TPPercent:     tpPercent,
		TrailingSL:    trailingSL,
		TrailingSLPct: trailingSLPct,
		State:         StatePendingEntry,
		Symbol:        symbol,
		Exchange:      exchange,
		StockCode:     stockCode,
		Quantity:      quantity,
		OrderSide:     orderSide,
		Auth:          auth,
		ProductType:   productType,
		Validity:      "DAY",
		StrategyID:    strategyID,
		EventID:       eventID,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	// Build the entry order model
	triggerPrice := roundNSE(entryTriggerPrice)
	entryOrder := &models.Order{
		OrderID:      entryOrderID,
		UserID:       userID,
		StrategyID:   strategyID,
		EventID:      eventID,
		StockCode:    stockCode,
		Exchange:     models.Exchange(exchange),
		Symbol:       symbol,
		OrderType:    models.OrderTypeStopLoss, // SL order
		OrderSide:    models.OrderSide(orderSide),
		Quantity:     quantity,
		Price:        &entryLimitPrice,
		StopLoss:     &triggerPrice, // trigger price goes here
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

	// Persist entry order to DB
	if err := m.repo.Create(ctx, entryOrder); err != nil {
		return nil, fmt.Errorf("failed to persist OCO entry order: %w", err)
	}

	// Place the entry SL order at broker with retry on 401 (session expired).
	var brokerID string
	var placeErr error
	currentAuth := auth
	for attempt := 0; attempt <= maxEntryRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)

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

		// Only retry on 401; other errors are not retryable here.
		if !isSessionExpiredError(placeErr) {
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

	// Persist broker ID synchronously — the status service WS handler looks up
	// orders by IndiraOrderID in the DB. If this write is async and the broker
	// WS fires EXECUTED before the write completes, GetByIndiraOrderID fails
	// and HandleBrokerUpdate is never called, so SL/TP legs are never placed.
	if err := m.repo.Update(ctx, entryOrder); err != nil {
		log.Printf("[oco] WARNING: failed to persist entry order broker ID for group %s: %v", groupID, err)
	}

	// Register in memory
	m.groups.Store(groupID, group)
	m.brokerIndex.Store(brokerID, groupID)

	log.Printf("[oco] Created OCO group %s: entry=%s broker=%s symbol=%s trigger=%.2f limit=%.2f SL=%.1f%% TP=%.1f%% trailing=%v",
		groupID, entryOrderID, brokerID, symbol, triggerPrice, entryLimitPrice, slPercent, tpPercent, trailingSL)

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
	}
}

// handleEntryUpdate processes broker status updates for the entry order.
func (m *OCOManager) handleEntryUpdate(ctx context.Context, group *OCOGroup, order *models.Order, status string) {
	switch status {
	case "EXECUTED", "TRADED":
		if group.State != StatePendingEntry {
			return // Already handled
		}

		// Capture fill price
		fillPrice := 0.0
		if order.FilledPrice != nil {
			fillPrice = *order.FilledPrice
		} else if order.Price != nil {
			fillPrice = *order.Price // fallback
		}
		if fillPrice <= 0 {
			log.Printf("[oco] WARNING: Entry fill price is 0 for group %s — cannot place legs", group.GroupID)
			group.State = StateFailed
			return
		}

		group.EntryFillPrice = fillPrice
		group.HighestPrice = fillPrice // initialize for trailing SL
		group.State = StatePlacingLegs
		group.UpdatedAt = time.Now()

		log.Printf("[oco] Entry FILLED for group %s at %.2f — placing SL+TP legs", group.GroupID, fillPrice)

		// Place SL and TP legs (in background goroutine so we don't block the WS handler)
		legCtx, legCancel := context.WithTimeout(context.Background(), brokerOpTimeout)
		go func() {
			defer legCancel()
			m.placeOCOLegs(legCtx, group)
		}()

	case "REJECTED", "A.REJECTED", "CANCELLED":
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

		// Calculate P&L
		if order.FilledPrice != nil {
			if group.OrderSide == "BUY" {
				group.PnL = (*order.FilledPrice - group.EntryFillPrice) * float64(group.Quantity)
			} else {
				group.PnL = (group.EntryFillPrice - *order.FilledPrice) * float64(group.Quantity)
			}
		}

		// Cancel TP leg
		go m.cancelLeg(group, group.TPBrokerID, "TP", "SL leg executed (OCO)")

	case "REJECTED", "A.REJECTED":
		log.Printf("[oco] SL leg REJECTED for group %s — cancelling TP and marking failed", group.GroupID)
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		go m.cancelLeg(group, group.TPBrokerID, "TP", "SL leg rejected (OCO cleanup)")
		m.scheduleCleanup(group)

	case "CANCELLED":
		// If we cancelled it ourselves (during OCO completion), this is expected.
		// If cancelled externally, we should also cancel the TP leg.
		if group.State == StateActive || group.State == StateLegsSubmitted {
			log.Printf("[oco] SL leg CANCELLED externally for group %s — cancelling TP", group.GroupID)
			group.State = StateCancelled
			group.UpdatedAt = time.Now()
			go m.cancelLeg(group, group.TPBrokerID, "TP", "SL leg cancelled externally (OCO)")
			m.scheduleCleanup(group)
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

		// Calculate P&L
		if order.FilledPrice != nil {
			if group.OrderSide == "BUY" {
				group.PnL = (*order.FilledPrice - group.EntryFillPrice) * float64(group.Quantity)
			} else {
				group.PnL = (group.EntryFillPrice - *order.FilledPrice) * float64(group.Quantity)
			}
		}

		// Cancel SL leg
		go m.cancelLeg(group, group.SLBrokerID, "SL", "TP leg executed (OCO)")

	case "REJECTED", "A.REJECTED":
		log.Printf("[oco] WARNING: TP leg REJECTED for group %s — SL leg remains active (user has SL protection but NO take-profit)", group.GroupID)
		// Don't cancel SL — user at least has stop-loss protection.
		// Broadcast so the frontend can notify the user.
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_tp_rejected", order)
		}

	case "CANCELLED":
		if group.State == StateActive || group.State == StateLegsSubmitted {
			log.Printf("[oco] TP leg CANCELLED externally for group %s — cancelling SL", group.GroupID)
			group.State = StateCancelled
			group.UpdatedAt = time.Now()
			go m.cancelLeg(group, group.SLBrokerID, "SL", "TP leg cancelled externally (OCO)")
			m.scheduleCleanup(group)
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

	slOK := group.SLLegConfirmed
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
// STEP 3: Place SL + TP Legs After Entry Fill
// ════════════════════════════════════════════════════════════════════════════

// placeOCOLegs places both the SL and TP legs after the entry order fills.
// Both legs are placed in parallel for minimum latency.
func (m *OCOManager) placeOCOLegs(ctx context.Context, group *OCOGroup) {
	fillPrice := group.EntryFillPrice
	exitSide := group.ExitSide()

	// Calculate prices
	slTrigger, slLimit := group.CalculateSLFromFill(fillPrice)
	tpLimit := group.CalculateTPFromFill(fillPrice)

	// Validate SL/TP price direction
	if group.OrderSide == "BUY" {
		if slTrigger >= fillPrice {
			log.Printf("[oco] CRITICAL: SL trigger %.2f >= fill %.2f for BUY group %s (SLPct=%.1f%%) — aborting legs",
				slTrigger, fillPrice, group.GroupID, group.SLPercent)
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			group.State = StateFailed
			group.UpdatedAt = time.Now()
			mu.Unlock()
			return
		}
		if tpLimit <= fillPrice {
			log.Printf("[oco] CRITICAL: TP limit %.2f <= fill %.2f for BUY group %s (TPPct=%.1f%%) — aborting legs",
				tpLimit, fillPrice, group.GroupID, group.TPPercent)
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			group.State = StateFailed
			group.UpdatedAt = time.Now()
			mu.Unlock()
			return
		}
	} else {
		if slTrigger <= fillPrice {
			log.Printf("[oco] CRITICAL: SL trigger %.2f <= fill %.2f for SELL group %s (SLPct=%.1f%%) — aborting legs",
				slTrigger, fillPrice, group.GroupID, group.SLPercent)
			mu := m.GetGroupMu(group.GroupID)
			mu.Lock()
			group.State = StateFailed
			group.UpdatedAt = time.Now()
			mu.Unlock()
			return
		}
		if tpLimit >= fillPrice {
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

	log.Printf("[oco] Placing legs for group %s: fillPrice=%.2f SL(trigger=%.2f limit=%.2f) TP(limit=%.2f) side=%s",
		group.GroupID, fillPrice, slTrigger, slLimit, tpLimit, exitSide)

	// Create order models for SL and TP legs
	slOrderID := uuid.New()
	tpOrderID := uuid.New()

	group.SLOrderID = slOrderID
	group.TPOrderID = tpOrderID
	group.SLTriggerPrice = slTrigger
	group.SLLimitPrice = slLimit
	group.TPLimitPrice = tpLimit

	slOrder := m.buildLegOrder(group, slOrderID, exitSide, models.OrderTypeStopLoss, &slLimit, &slTrigger, RoleSLLeg)
	tpOrder := m.buildLegOrder(group, tpOrderID, exitSide, models.OrderTypeLimit, &tpLimit, nil, RoleTPLeg)

	// Persist both leg orders to DB
	var dbWg sync.WaitGroup
	dbWg.Add(2)
	go func() {
		defer dbWg.Done()
		if err := m.repo.Create(ctx, slOrder); err != nil {
			log.Printf("[oco] Failed to persist SL leg order: %v", err)
		}
	}()
	go func() {
		defer dbWg.Done()
		if err := m.repo.Create(ctx, tpOrder); err != nil {
			log.Printf("[oco] Failed to persist TP leg order: %v", err)
		}
	}()
	dbWg.Wait()

	// Place both legs at broker in parallel
	var wg sync.WaitGroup
	var slBrokerID, tpBrokerID string
	var slErr, tpErr error

	wg.Add(2)

	go func() {
		defer wg.Done()
		slBrokerID, slErr = m.placeLegWithRetry(ctx, slOrder, group.Auth, "SL", group.GroupID)
	}()

	go func() {
		defer wg.Done()
		tpBrokerID, tpErr = m.placeLegWithRetry(ctx, tpOrder, group.Auth, "TP", group.GroupID)
	}()

	wg.Wait()

	// Lock the group for state updates
	mu := m.GetGroupMu(group.GroupID)
	mu.Lock()
	defer mu.Unlock()

	// Handle results
	if slErr != nil && tpErr != nil {
		// Both failed — critical failure. User has an open position with NO protection.
		log.Printf("[oco] CRITICAL: Both legs failed for group %s: SL=%v TP=%v — POSITION UNPROTECTED", group.GroupID, slErr, tpErr)
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		m.scheduleCleanup(group)
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_legs_failed", slOrder)
		}
		return
	}

	if slErr != nil {
		// SL failed but TP placed — user has no protection! Cancel TP and fail.
		log.Printf("[oco] SL leg failed for group %s (no protection) — cancelling TP", group.GroupID)
		group.TPBrokerID = tpBrokerID
		m.brokerIndex.Store(tpBrokerID, group.GroupID)
		group.State = StateFailed
		group.UpdatedAt = time.Now()
		go m.cancelLeg(group, tpBrokerID, "TP", "SL leg failed (no protection)")
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_legs_failed", slOrder)
		}
		return
	}

	if tpErr != nil {
		// TP failed but SL placed — user has protection via SL. Continue without TP.
		log.Printf("[oco] TP leg failed for group %s — SL submitted, awaiting WS confirmation (user protected)", group.GroupID)
		group.SLBrokerID = slBrokerID
		m.brokerIndex.Store(slBrokerID, group.GroupID)
		group.TPLegConfirmed = true // no TP leg to confirm
		group.State = StateLegsSubmitted
		group.UpdatedAt = time.Now()
		if m.wsBroadcaster != nil {
			m.wsBroadcaster(group.UserID, "oco_tp_rejected", slOrder)
		}
		return
	}

	// Both succeeded — submitted to broker, awaiting WS confirmation
	group.SLBrokerID = slBrokerID
	group.TPBrokerID = tpBrokerID
	group.State = StateLegsSubmitted
	group.UpdatedAt = time.Now()

	// Register broker IDs for O(1) WS lookup
	m.brokerIndex.Store(slBrokerID, group.GroupID)
	m.brokerIndex.Store(tpBrokerID, group.GroupID)

	log.Printf("[oco] Group %s LEGS_SUBMITTED: SL(broker=%s trigger=%.2f) TP(broker=%s limit=%.2f) — awaiting WS confirmation",
		group.GroupID, slBrokerID, slTrigger, tpBrokerID, tpLimit)

	// Broadcast to frontend
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
	return &models.Order{
		OrderID:      orderID,
		UserID:       group.UserID,
		StrategyID:   group.StrategyID,
		EventID:      group.EventID,
		StockCode:    group.StockCode,
		Exchange:     models.Exchange(group.Exchange),
		Symbol:       group.Symbol,
		OrderType:    orderType,
		OrderSide:    models.OrderSide(side),
		Quantity:     group.Quantity,
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
// Cancel + Cleanup
// ════════════════════════════════════════════════════════════════════════════

// cancelLeg cancels a single leg order at the broker with retry and timeout.
func (m *OCOManager) cancelLeg(group *OCOGroup, brokerID string, legName string, reason string) {
	if brokerID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), brokerOpTimeout)
	defer cancel()

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}

		err := m.indiraClient.CancelOrder(ctx, group.Exchange, brokerID, group.Symbol, group.Auth)
		if err != nil {
			log.Printf("[oco] Cancel %s leg attempt %d failed (group=%s broker=%s): %v",
				legName, attempt+1, group.GroupID, brokerID, err)
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
func (m *OCOManager) ModifySLLeg(ctx context.Context, group *OCOGroup, newTrigger, newLimit float64) error {
	mu := m.GetGroupMu(group.GroupID)
	mu.Lock()
	defer mu.Unlock()

	if group.State != StateActive || group.SLBrokerID == "" {
		return fmt.Errorf("group %s not in ACTIVE state or SL broker ID missing", group.GroupID)
	}

	// Build a modify order
	slOrder := &models.Order{
		OrderID:       group.SLOrderID,
		IndiraOrderID: &group.SLBrokerID,
		StockCode:     group.StockCode,
		Exchange:      models.Exchange(group.Exchange),
		Symbol:        group.Symbol,
		OrderType:     models.OrderTypeStopLoss,
		OrderSide:     models.OrderSide(group.ExitSide()),
		Quantity:      group.Quantity,
		Price:         &newLimit,
		StopLoss:      &newTrigger,
		Validity:      group.Validity,
		ProductType:   group.ProductType,
	}

	if err := m.indiraClient.ModifyOrder(ctx, slOrder, group.Auth); err != nil {
		return fmt.Errorf("modify SL leg failed: %w", err)
	}

	// Update in-memory state
	oldTrigger := group.SLTriggerPrice
	group.SLTriggerPrice = newTrigger
	group.SLLimitPrice = newLimit
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
			group.ProductType = o.ProductType
			group.Validity = o.Validity
			group.StrategyID = o.StrategyID
			group.EventID = o.EventID
			group.CreatedAt = o.CreatedAt
			if o.IndiraOrderID != nil {
				group.EntryBrokerID = *o.IndiraOrderID
			}
			if o.FilledPrice != nil {
				group.EntryFillPrice = *o.FilledPrice
				group.HighestPrice = *o.FilledPrice
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

	// If entry not filled yet
	if !models.IsFilledStatus(entryStatus) && !models.IsTerminalStatus(entryStatus) {
		return StatePendingEntry
	}

	// If entry is terminal but not filled
	if models.IsTerminalStatus(entryStatus) && !models.IsFilledStatus(entryStatus) {
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

// CancelGroupsBySymbol cancels all non-terminal OCO groups for a user+symbol.
// Called when a manual position exit is detected for a specific symbol.
func (m *OCOManager) CancelGroupsBySymbol(ctx context.Context, userID string, symbol string) {
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

	log.Printf("[oco] Manual exit detected: cancelling %d OCO groups for %s/%s", len(toCancel), userID, symbol)
	for _, gid := range toCancel {
		if err := m.CancelGroup(ctx, gid); err != nil {
			log.Printf("[oco] Failed to cancel group %s: %v", gid, err)
		}
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
