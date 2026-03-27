// Package statusservice provides real-time order status updates
// using the Indira WebSocket stream. It subscribes per-user and
// updates the order database and publishes Kafka notifications.
package statusservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	inexec "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// OCOHandler is called on every broker WS status update to check if the order
// belongs to an OCO group and act accordingly (place legs, cancel counterpart).
type OCOHandler interface {
	HandleBrokerUpdate(ctx context.Context, order *models.Order, brokerStatus string)
	// CancelGroupsBySymbol cancels all active OCO groups for a user+symbol.
	// Used when a manual position exit is detected (order not in our DB).
	CancelGroupsBySymbol(ctx context.Context, userID string, symbol string)
}

// OrderStatusService listens to a single shared WebSocket connection and
// routes order-status updates to the correct user by WSOrderStatus.UserID.
// All active users share one TCP connection to Indira instead of one per user.
type OrderStatusService struct {
	execClient    *inexec.ExecutionClient
	repo          repository.OrderRepository
	credsRepo     repository.CredentialsRepository
	publisher     *publisher.KafkaPublisher
	logger        *zap.Logger
	wsBroadcaster func(userID string, order *models.Order)
	ocoHandler    OCOHandler // OCO order management hook

	// Single shared WS client. Protected by wsMu for first-connect init only.
	wsMu             sync.Mutex
	wsClient         *indiraClient.WSClient // nil until first StartSubscription
	processorRunning bool                   // true while processUpdates goroutine is alive

	// subscriberAuths: userID → *indiraClient.AuthContext
	// Serves two purposes: (1) tracks who is subscribed, (2) enables re-subscription
	// after a reconnect using the stored auth context.
	subscriberAuths sync.Map
}

// SetWSBroadcaster wires a callback so live order status changes are pushed
// to the frontend immediately via /ws/live-orders.
func (s *OrderStatusService) SetWSBroadcaster(fn func(userID string, order *models.Order)) {
	s.wsBroadcaster = fn
}

// SetOCOHandler wires the OCO manager so every broker WS status update is
// checked for OCO group membership. This is the hook that enables:
//   - Entry fill → place SL+TP legs
//   - SL leg fill → cancel TP leg (and vice versa)
func (s *OrderStatusService) SetOCOHandler(handler OCOHandler) {
	s.ocoHandler = handler
}

// NewOrderStatusService creates a new order status service.
// credsRepo may be nil — if set, enables auto-refresh of expired broker
// credentials on the shared WebSocket connection (avoids persistent 401 loops).
func NewOrderStatusService(execClient *inexec.ExecutionClient, repo repository.OrderRepository, credsRepo repository.CredentialsRepository, pub *publisher.KafkaPublisher, logger *zap.Logger) *OrderStatusService {
	return &OrderStatusService{
		execClient: execClient,
		repo:       repo,
		credsRepo:  credsRepo,
		publisher:  pub,
		logger:     logger,
	}
}

// StartSubscription subscribes userID to the shared WS connection.
// The first call establishes the connection; subsequent calls send a
// WSConnectionRequest message on the existing connection. Idempotent.
func (s *OrderStatusService) StartSubscription(ctx context.Context, userID string, auth *indiraClient.AuthContext) error {
	// Always refresh stored auth (bearer token may have rotated).
	authCopy := *auth
	s.subscriberAuths.Store(userID, &authCopy)

	s.wsMu.Lock()
	defer s.wsMu.Unlock()

	if s.wsClient == nil {
		// First user — establish the shared connection.
		wsClient, err := s.execClient.GetSharedWSClient(ctx, auth)
		if err != nil {
			s.subscriberAuths.Delete(userID)
			return fmt.Errorf("failed to connect shared WS: %w", err)
		}
		s.wsClient = wsClient
		// Re-subscribe all stored users after any reconnect.
		s.wsClient.OnReconnected = s.resubscribeAll
		// On 401, reload credentials from DB so the WS can recover
		// without waiting for the user to place a new order.
		s.wsClient.OnAuthRefresh = s.refreshAuthFromDB
	}

	// Ensure the update processor goroutine is always running.
	// Uses context.Background() because it must live for the entire
	// service lifetime, not be tied to the caller's context.
	if !s.processorRunning {
		s.processorRunning = true
		go s.processUpdates(context.Background())
		s.logger.Info("Shared WS established", zap.String("first_user", userID))
		return nil
	}

	// Subsequent user — subscribe on the live connection.
	if err := s.wsClient.Subscribe(ctx, auth); err != nil {
		return fmt.Errorf("subscribe user %s on shared WS: %w", userID, err)
	}
	s.logger.Info("User subscribed on shared WS", zap.String("user_id", userID))
	return nil
}

// StopSubscription removes a user from the subscription map.
// The shared WS connection stays open for remaining users.
func (s *OrderStatusService) StopSubscription(userID string) {
	s.subscriberAuths.Delete(userID)
	s.logger.Info("Unsubscribed user from shared WS", zap.String("user_id", userID))
}

// refreshAuthFromDB reloads credentials from the DB for the given user.
// Called by WSClient.OnAuthRefresh when a 401 is detected during token fetch.
func (s *OrderStatusService) refreshAuthFromDB(userID string) (*indiraClient.AuthContext, error) {
	if s.credsRepo == nil {
		return nil, fmt.Errorf("no credentials repository configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	userId, appId, source, bearerToken, err := s.credsRepo.GetIndiraCredentials(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to load credentials for user %s: %w", userID, err)
	}
	newAuth := &indiraClient.AuthContext{
		UserId:      userId,
		AppId:       appId,
		Source:      source,
		BearerToken: bearerToken,
	}
	// Also update subscriberAuths so resubscribeAll uses the fresh token.
	s.subscriberAuths.Store(userID, newAuth)
	s.logger.Info("Refreshed auth from DB for WS reconnect", zap.String("user_id", userID))
	return newAuth, nil
}

// resubscribeAll is called by WSClient.OnReconnected after a reconnect.
// It re-sends WSConnectionRequest for every stored user so Indira resumes
// streaming their updates on the new session.
func (s *OrderStatusService) resubscribeAll() {
	metrics.BrokerWSReconnects.Inc()
	s.logger.Info("Shared WS reconnected — re-subscribing all users")
	s.subscriberAuths.Range(func(key, value any) bool {
		userID := key.(string)
		auth := value.(*indiraClient.AuthContext)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.wsClient.Subscribe(ctx, auth); err != nil {
				s.logger.Error("Re-subscribe after reconnect failed",
					zap.String("user_id", userID), zap.Error(err))
			}
		}()
		return true
	})
}

// processUpdates reads from the shared WS channel and dispatches each update
// to a goroutine so DB operations never block the read loop.
// Started exactly once when the shared connection is established.
func (s *OrderStatusService) processUpdates(ctx context.Context) {
	s.logger.Info("Shared WS update processor started")
	defer func() {
		s.wsMu.Lock()
		s.processorRunning = false
		s.wsMu.Unlock()
		s.logger.Warn("Shared WS update processor exited — will restart on next subscription")
	}()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("WS update processor shutting down")
			return
		case wsStatus, ok := <-s.wsClient.Updates:
			if !ok {
				// Updates channel is never closed by WSClient; this branch
				// is a safety net only.
				s.logger.Warn("Shared WS updates channel closed unexpectedly")
				return
			}
			go s.handleStatusUpdate(ctx, wsStatus)
		}
	}
}

func (s *OrderStatusService) handleStatusUpdate(ctx context.Context, wsStatus *indiraClient.WSOrderStatus) {
	updateStart := time.Now()
	defer func() {
		metrics.StatusUpdateProcessDuration.Observe(time.Since(updateStart).Seconds())
	}()

	// UniqueCode is normally the System-generated order ID ("NZVND00001J2" style)
	// OrderNumber = Exchange order number (0 when not yet matched)
	indiraID := wsStatus.UniqueCode
	if indiraID == "" {
		indiraID = wsStatus.OrderNumber
	}
	if indiraID == "" || indiraID == "0" {
		return
	}

	order, err := s.repo.GetByIndiraOrderID(ctx, indiraID)
	if err != nil {
		// Not our order (placed outside this system).
		// Check if this is a manual position exit — if so, cancel active OCO groups for this symbol.
		brokerStatusUpper := strings.ToUpper(strings.TrimSpace(wsStatus.OrderStatus))
		if (brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED") && wsStatus.Symbol != "" && s.ocoHandler != nil {
			s.handleManualExitDetection(ctx, wsStatus)
		}
		return
	}

	// ── Store raw broker data exactly as received ──────────────────────────
	// Serialize full WS payload to JSON for audit/debugging
	rawJSON, _ := json.Marshal(wsStatus)
	rawJSONStr := string(rawJSON)
	order.BrokerWSData = &rawJSONStr

	// Store raw broker status string without any mapping
	brokerStatus := wsStatus.OrderStatus
	order.BrokerStatus = &brokerStatus

	// Store exchange order number
	if wsStatus.OrderNumber != "" && wsStatus.OrderNumber != "0" {
		order.ExchangeOrderNumber = &wsStatus.OrderNumber
	}

	// ── Use broker status directly — no mapping ───────────────────────────
	// The broker sends statuses like PENDING, EXECUTED, CANCELLED etc.
	// We store and show exactly what the broker sends.
	brokerStatusUpper := strings.ToUpper(strings.TrimSpace(wsStatus.OrderStatus))
	newStatus := models.OrderStatus(brokerStatusUpper)
	previousStatus := order.Status

	// Record every WS event in execution_events for full audit trail
	s.repo.RecordExecutionEvent(ctx, order.OrderID, "BROKER_WS_UPDATE", map[string]interface{}{
		"broker_status":         wsStatus.OrderStatus,
		"previous_status":       string(previousStatus),
		"symbol":                wsStatus.Symbol,
		"order_number":          wsStatus.OrderNumber,
		"unique_code":           wsStatus.UniqueCode,
		"traded_qty":            wsStatus.TradedQTY,
		"traded_price":          wsStatus.TradedPrice,
		"order_price":           wsStatus.OrderPrice,
		"trigger_price":         wsStatus.TriggerPrice,
		"pending_qty":           wsStatus.PendingQty,
		"order_original_qty":    wsStatus.OrderOriginalQty,
		"product":               wsStatus.Product,
		"order_type":            wsStatus.OrderType,
		"buy_sell":              wsStatus.BuySell,
		"reason":                wsStatus.Reason,
		"order_entry_time":      wsStatus.OrderEntryTime,
		"last_modified":         wsStatus.LastModifiedTimeStamp,
		"order_seq":             wsStatus.OrderSequenceNumber,
		"message_seq":           wsStatus.MessageSequenceNumber,
		"decimal_locator":       wsStatus.DecimalLocator,
		"exchange":              wsStatus.Exchange,
		"misc":                  wsStatus.Misc,
		"initiated_by":          wsStatus.InitiatedBy,
		"modified_by":           wsStatus.ModifiedBy,
		"exchange_algo_id":      wsStatus.ExchangeAlgoID,
		"exchange_account_code": wsStatus.ExchangeAccountCode,
		"timestamp":             time.Now(),
	})

	// Metrics
	metrics.StatusUpdatesReceived.WithLabelValues(brokerStatusUpper).Inc()
	switch brokerStatusUpper {
	case "EXECUTED", "TRADED":
		metrics.FillsTotal.WithLabelValues("live").Inc()
	case "REJECTED", "A.REJECTED":
		metrics.RejectionsTotal.WithLabelValues("rejected").Inc()
	case "CANCELLED":
		metrics.RejectionsTotal.WithLabelValues("cancelled").Inc()
	}

	// Set status to exactly what broker sent
	order.Status = newStatus

	// Extract fill details using DecimalLocator for correct price
	dl := decimalLocator(string(wsStatus.DecimalLocator))

	// Always update traded qty and price from broker
	if qty, err := strconv.Atoi(string(wsStatus.TradedQTY)); err == nil {
		order.FilledQuantity = int32(qty)
	}
	if price, err := strconv.ParseFloat(wsStatus.TradedPrice, 64); err == nil && price > 0 {
		order.FilledPrice = &price
	}

	// Mark execution time for filled orders
	if brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED" {
		now := time.Now()
		order.ExecutedAt = &now
	}

	// Update order price from broker
	if wsStatus.OrderPrice != "" {
		if oprice, err := strconv.ParseFloat(wsStatus.OrderPrice, 64); err == nil && oprice > 0 {
			order.Price = &oprice
		}
	}

	// Store trigger price from broker (apply DecimalLocator)
	if wsStatus.TriggerPrice > 0 {
		tp := wsStatus.TriggerPrice / dl
		order.StopLoss = &tp
	}

	// Store rejection/cancellation reason
	if wsStatus.Reason != "" {
		reason := wsStatus.Reason
		order.RejectionReason = &reason
	}

	if err := s.repo.Update(ctx, order); err != nil {
		s.logger.Error("Failed to update order from WS",
			zap.Error(err),
			zap.String("order_id", order.OrderID.String()))
		return
	}

	s.logger.Info("Order updated from broker WS",
		zap.String("order_id", order.OrderID.String()),
		zap.String("prev", string(previousStatus)),
		zap.String("broker_status", brokerStatusUpper),
		zap.String("traded_qty", string(wsStatus.TradedQTY)),
		zap.String("traded_price", wsStatus.TradedPrice),
		zap.String("order_number", wsStatus.OrderNumber))

	// Push to all connected /ws/live-orders clients immediately
	if s.wsBroadcaster != nil {
		orderCopy := *order
		s.wsBroadcaster(orderCopy.UserID, &orderCopy)
	}

	// ── OCO hook: check if this order is part of an OCO group ────────────
	// On entry fill → place SL+TP legs. On leg fill → cancel counterpart.
	// Runs in its own goroutine so it never blocks the WS read loop.
	if s.ocoHandler != nil {
		orderCopy := *order
		go s.ocoHandler.HandleBrokerUpdate(context.Background(), &orderCopy, brokerStatusUpper)
	}

	s.publishNotification(ctx, order, wsStatus)
}

// handleManualExitDetection is called when we see an EXECUTED order that is
// NOT in our database — this means the user placed it manually from their
// broker app. If they have active OCO groups for that symbol, cancel them
// to prevent orphaned SL/TP legs from triggering unwanted positions.
func (s *OrderStatusService) handleManualExitDetection(ctx context.Context, wsStatus *indiraClient.WSOrderStatus) {
	// Extract user ID from the WS message
	userID := wsStatus.UCC // UCC is the client code (user ID)
	if userID == "" {
		return
	}

	symbol := wsStatus.Symbol
	s.logger.Info("Manual exit detected — cancelling OCO groups for symbol",
		zap.String("user_id", userID),
		zap.String("symbol", symbol),
		zap.String("unique_code", wsStatus.UniqueCode))

	go s.ocoHandler.CancelGroupsBySymbol(context.Background(), userID, symbol)
}

// flexToInt converts a FlexInt to int, returning 0 on failure.
func flexToInt(f indiraClient.FlexInt) int {
	v, _ := strconv.Atoi(string(f))
	return v
}

// decimalLocator returns the divisor encoded in the WS DecimalLocator field.
// E.g. "100" means raw prices must be divided by 100 to get the actual value.
// Returns 1 when the field is empty or unparseable (no-op divisor).
func decimalLocator(dl string) float64 {
	if dl == "" {
		return 1
	}
	if v, err := strconv.ParseFloat(dl, 64); err == nil && v > 0 {
		return v
	}
	return 1
}

func (s *OrderStatusService) publishNotification(ctx context.Context, order *models.Order, wsStatus *indiraClient.WSOrderStatus) {
	if s.publisher == nil {
		return
	}

	dl := decimalLocator(string(wsStatus.DecimalLocator))

	// Parse WS numeric fields
	tradedQty := 0
	if qty, err := strconv.Atoi(string(wsStatus.TradedQTY)); err == nil {
		tradedQty = qty
	}
	tradedPriceStr := ""
	if wsStatus.TradedPrice != "" && wsStatus.TradedPrice != "0" {
		if p, err := strconv.ParseFloat(wsStatus.TradedPrice, 64); err == nil && p > 0 {
			tradedPriceStr = fmt.Sprintf("₹%.2f", p)
		}
	}
	triggerPriceStr := ""
	if wsStatus.TriggerPrice > 0 {
		triggerPriceStr = fmt.Sprintf("₹%.2f", wsStatus.TriggerPrice/dl)
	}

	// Broker ref: prefer UniqueCode (system order ID), fall back to OrderNumber
	brokerRef := wsStatus.UniqueCode
	if brokerRef == "" {
		brokerRef = wsStatus.OrderNumber
	}

	// Build enriched ExecutionDetails from WS payload
	execDetails := &models.ExecutionDetails{
		ExecutedAt:      time.Now(),
		ExecutedPrice:   tradedPriceStr,
		BrokerRef:       brokerRef,
		TradedQty:       tradedQty,
		PendingQty:      flexToInt(wsStatus.PendingQty),
		OriginalQty:     wsStatus.OrderOriginalQty,
		ExchangeOrderNo: wsStatus.OrderNumber,
		OrderEntryTime:  wsStatus.OrderEntryTime,
		LastModifiedAt:  wsStatus.LastModifiedTimeStamp,
		Product:         wsStatus.Product,
		OrderValidity:   wsStatus.OrderValidity,
		Reason:          wsStatus.Reason,
		TriggerPrice:    triggerPriceStr,
	}

	update := &models.OrderUpdate{
		UpdateID:  uuid.New().String(),
		OrderID:   order.OrderID.String(),
		UserID:    order.UserID,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(1 * time.Hour),
		Status:    string(order.Status),
		OrderSummary: models.OrderSummary{
			Stock:     order.Symbol,
			Action:    string(order.OrderSide),
			Quantity:  order.Quantity,
			Exchange:  string(order.Exchange),
			OrderType: string(order.OrderType),
		},
		ExecutionDetails: execDetails,
	}
	if order.Price != nil {
		update.OrderSummary.Price = fmt.Sprintf("₹%.2f", *order.Price)
	} else {
		update.OrderSummary.Price = "MARKET"
	}

	// Use raw broker status for notification type
	brokerStatusUpper := strings.ToUpper(strings.TrimSpace(wsStatus.OrderStatus))
	switch brokerStatusUpper {
	case "EXECUTED", "TRADED":
		update.UpdateType = "EXECUTION_SUCCESS"
		update.Priority = "HIGH"
		update.Title = "Order Executed ✓"
		update.Message = fmt.Sprintf("Your %s order was executed", order.Symbol)
		if tradedPriceStr != "" {
			update.DetailedMessage = fmt.Sprintf(
				"Filled %d/%d share(s) of %s at %s on %s | Broker ref: %s | Entry: %s",
				tradedQty, wsStatus.OrderOriginalQty, order.Symbol, tradedPriceStr,
				order.Exchange, brokerRef, wsStatus.OrderEntryTime,
			)
			execDetails.ExecutedAt = time.Now()
		}
		update.StatusEmoji = "✅"
		update.StatusColor = "#00C851"

	case "PARTIALLY TRADED", "PARTIALLY EXECUTED":
		update.UpdateType = "PARTIAL_FILL"
		update.Priority = "MEDIUM"
		update.Title = "Order Partially Filled"
		update.Message = fmt.Sprintf("%d of %d shares filled for %s", tradedQty, wsStatus.OrderOriginalQty, order.Symbol)
		update.DetailedMessage = fmt.Sprintf(
			"Partially filled %d/%d at %s | Pending qty: %d | Broker ref: %s",
			tradedQty, wsStatus.OrderOriginalQty, tradedPriceStr, wsStatus.PendingQty, brokerRef,
		)
		update.StatusEmoji = "🔶"
		update.StatusColor = "#FFBB33"

	case "REJECTED", "A.REJECTED":
		update.UpdateType = "ORDER_REJECTED"
		update.Priority = "HIGH"
		update.Title = "Order Rejected ✗"
		update.Message = fmt.Sprintf("Your %s order was rejected", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Rejected by broker: %s | Ref: %s", wsStatus.Reason, brokerRef)
		update.StatusEmoji = "❌"
		update.StatusColor = "#FF4444"
		execDetails.Reason = wsStatus.Reason

	case "CANCELLED":
		update.UpdateType = "ORDER_CANCELLED"
		update.Priority = "MEDIUM"
		update.Title = "Order Cancelled"
		update.Message = fmt.Sprintf("Your %s order was cancelled", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Cancelled: %s | Ref: %s", wsStatus.Reason, brokerRef)
		update.StatusEmoji = "⚠️"
		update.StatusColor = "#FFBB33"
		execDetails.Reason = wsStatus.Reason

	case "PENDING":
		update.UpdateType = "ORDER_PENDING"
		update.Priority = "LOW"
		update.Title = "Order Pending"
		update.Message = fmt.Sprintf("Your %s order is pending", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Pending | Ref: %s | Entry: %s", brokerRef, wsStatus.OrderEntryTime)
		update.StatusEmoji = "⏳"
		update.StatusColor = "#33B5E5"

	default:
		// For any other broker status, still publish a generic notification
		update.UpdateType = "ORDER_STATUS_UPDATE"
		update.Priority = "LOW"
		update.Title = fmt.Sprintf("Order %s", brokerStatusUpper)
		update.Message = fmt.Sprintf("Your %s order status: %s", order.Symbol, brokerStatusUpper)
		update.StatusEmoji = "ℹ️"
		update.StatusColor = "#33B5E5"
	}

	if err := s.publisher.PublishOrderUpdate(ctx, update); err != nil {
		s.logger.Error("Failed to publish WS order update", zap.Error(err))
	} else {
		s.logger.Info("WS order-update published to Kafka",
			zap.String("update_type", update.UpdateType),
			zap.String("order_id", order.OrderID.String()),
			zap.String("broker_ref", brokerRef),
			zap.String("traded_price", tradedPriceStr),
			zap.Int("traded_qty", tradedQty),
			zap.Int("pending_qty", flexToInt(wsStatus.PendingQty)),
			zap.String("reason", wsStatus.Reason),
		)
	}
}

