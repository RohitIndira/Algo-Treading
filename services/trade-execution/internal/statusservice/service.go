// Package statusservice provides real-time order status updates
// using the Indira WebSocket stream. It subscribes per-user and
// updates the order database and publishes Kafka notifications.
package statusservice

import (
	"context"
	"fmt"
	"log"
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

// OrderStatusService listens to a single shared WebSocket connection and
// routes order-status updates to the correct user by WSOrderStatus.UserID.
// All active users share one TCP connection to Indira instead of one per user.
type OrderStatusService struct {
	execClient    *inexec.ExecutionClient
	repo          repository.OrderRepository
	publisher     *publisher.KafkaPublisher
	logger        *zap.Logger
	wsBroadcaster func(userID string, order *models.Order)

	// Single shared WS client. Protected by wsMu for first-connect init only.
	wsMu     sync.Mutex
	wsClient *indiraClient.WSClient // nil until first StartSubscription

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

// NewOrderStatusService creates a new order status service
func NewOrderStatusService(execClient *inexec.ExecutionClient, repo repository.OrderRepository, pub *publisher.KafkaPublisher, logger *zap.Logger) *OrderStatusService {
	return &OrderStatusService{
		execClient: execClient,
		repo:       repo,
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
		// Single goroutine fans out all updates from one channel.
		go s.processUpdates(ctx)
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
	// UniqueCode is normally the System-generated order ID ("NZVND00001J2" style)
	// OrderNumber = Exchange order number (0 when not yet matched)
	indiraID := wsStatus.UniqueCode
	if indiraID == "" {
		indiraID = wsStatus.OrderNumber
	}
	if indiraID == "" || indiraID == "0" {
		return
	}

	s.logger.Debug("WS order update",
		zap.String("id", indiraID),
		zap.String("status", wsStatus.OrderStatus),
		zap.String("symbol", wsStatus.Symbol))

	order, err := s.repo.GetByIndiraOrderID(ctx, indiraID)
	if err != nil {
		// Not our order (placed outside this system) – silently skip
		return
	}

	newStatus := mapIndiraStatus(wsStatus.OrderStatus)
	previousStatus := order.Status

	// Skip if nothing useful changed
	if order.Status == newStatus {
		return
	}

	metrics.StatusUpdatesReceived.WithLabelValues(string(newStatus)).Inc()

	// Track fills and rejections
	switch newStatus {
	case models.StatusFilled:
		metrics.FillsTotal.WithLabelValues("live").Inc()
	case models.StatusRejected:
		metrics.RejectionsTotal.WithLabelValues("rejected").Inc()
	case models.StatusCancelled:
		metrics.RejectionsTotal.WithLabelValues("cancelled").Inc()
	}

	order.Status = newStatus

	if newStatus == models.StatusFilled || newStatus == models.StatusPartiallyFilled {
		if qty, err := strconv.Atoi(wsStatus.TradedQTY); err == nil {
			order.FilledQuantity = int32(qty)
		}
		if price, err := strconv.ParseFloat(wsStatus.TradedPrice, 64); err == nil && price > 0 {
			order.FilledPrice = &price
		}
		now := time.Now()
		order.ExecutedAt = &now
	}

	if newStatus == models.StatusRejected || newStatus == models.StatusCancelled {
		reason := wsStatus.Reason
		order.RejectionReason = &reason
	}

	if err := s.repo.Update(ctx, order); err != nil {
		s.logger.Error("Failed to update order status from WS",
			zap.Error(err),
			zap.String("order_id", order.OrderID.String()))
		return
	}

	s.logger.Info("Order status updated from WS",
		zap.String("order_id", order.OrderID.String()),
		zap.String("prev", string(previousStatus)),
		zap.String("new", string(newStatus)))

	// Push the updated order to all connected /ws/live-orders clients immediately.
	if s.wsBroadcaster != nil {
		orderCopy := *order
		s.wsBroadcaster(orderCopy.UserID, &orderCopy)
	}

	s.publishNotification(ctx, order, wsStatus)
}

func (s *OrderStatusService) publishNotification(ctx context.Context, order *models.Order, wsStatus *indiraClient.WSOrderStatus) {
	if s.publisher == nil {
		return
	}

	// Parse WS numeric fields
	tradedQty := 0
	if qty, err := strconv.Atoi(wsStatus.TradedQTY); err == nil {
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
		triggerPriceStr = fmt.Sprintf("₹%.2f", wsStatus.TriggerPrice)
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
		PendingQty:      wsStatus.PendingQty,
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

	switch order.Status {
	case models.StatusFilled:
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

	case models.StatusPartiallyFilled:
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

	case models.StatusRejected:
		update.UpdateType = "ORDER_REJECTED"
		update.Priority = "HIGH"
		update.Title = "Order Rejected ✗"
		update.Message = fmt.Sprintf("Your %s order was rejected", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Rejected by broker: %s | Ref: %s", wsStatus.Reason, brokerRef)
		update.StatusEmoji = "❌"
		update.StatusColor = "#FF4444"
		execDetails.Reason = wsStatus.Reason

	case models.StatusCancelled:
		update.UpdateType = "ORDER_CANCELLED"
		update.Priority = "MEDIUM"
		update.Title = "Order Cancelled"
		update.Message = fmt.Sprintf("Your %s order was cancelled", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Cancelled: %s | Ref: %s", wsStatus.Reason, brokerRef)
		update.StatusEmoji = "⚠️"
		update.StatusColor = "#FFBB33"
		execDetails.Reason = wsStatus.Reason

	default:
		return // Don't flood with intermediate status notifications
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
			zap.Int("pending_qty", wsStatus.PendingQty),
			zap.String("reason", wsStatus.Reason),
		)
	}
}

// mapIndiraStatus converts a raw Indira WS status string into our internal OrderStatus.
// Based on actual observations:
//   - "A.REJECTED"       → REJECTED
//   - "ADMIN PENDING "   → PENDING
//   - "TRADED"           → FILLED
//   - "PARTIALLY TRADED" → PARTIALLY_FILLED
//   - "CANCELLED"        → CANCELLED
//   - "OPEN"             → SUBMITTED
func mapIndiraStatus(indiraStatus string) models.OrderStatus {
	s := strings.ToUpper(strings.TrimSpace(indiraStatus))
	switch {
	case strings.Contains(s, "REJECTED"):
		return models.StatusRejected
	case strings.Contains(s, "CANCELLED"):
		return models.StatusCancelled
	case strings.Contains(s, "PARTIALLY TRADED") || strings.Contains(s, "PARTIAL"):
		return models.StatusPartiallyFilled
	case s == "TRADED" || strings.Contains(s, "COMPLETE"):
		return models.StatusFilled
	case strings.Contains(s, "PENDING"), strings.Contains(s, "ADMIN PENDING"):
		return models.StatusPending
	case strings.Contains(s, "OPEN"), strings.Contains(s, "SUBMITTED"):
		return models.StatusSubmitted
	default:
		log.Printf("[statusservice] Unmapped Indira WS status: %q – defaulting to PENDING", s)
		return models.StatusPending
	}
}
