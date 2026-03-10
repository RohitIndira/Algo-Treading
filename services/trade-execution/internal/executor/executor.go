package executor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/statusservice"
	"github.com/google/uuid"
)

// OrderExecutor handles order execution logic
type OrderExecutor struct {
	repo          repository.OrderRepository
	credsCache    *CredentialsCache // in-memory credential store; avoids per-order DB hit
	indiraClient  *indira.ExecutionClient
	kafkaPub      *publisher.KafkaPublisher
	statusSvc     *statusservice.OrderStatusService
	paperExecutor *PaperOrderExecutor
	wsBroadcaster func(userID string, eventType string, order *models.Order)
	maxRetries    int
	retryDelay    time.Duration
}

// SetWSBroadcaster sets the callback for broadcasting new orders to the frontend WebSocket
func (e *OrderExecutor) SetWSBroadcaster(broadcaster func(userID string, eventType string, order *models.Order)) {
	e.wsBroadcaster = broadcaster
}

// SetPaperExecutor overrides the paper executor
func (e *OrderExecutor) SetPaperExecutor(paperExec *PaperOrderExecutor) {
	e.paperExecutor = paperExec
}

// NewOrderExecutor creates a new order executor.
// kafkaPub and statusSvc may be nil (graceful degradation).
// credsRepo is wrapped in a CredentialsCache so credentials are only fetched
// from the DB once per user and then served from memory on every subsequent order.
func NewOrderExecutor(
	repo repository.OrderRepository,
	credsRepo repository.CredentialsRepository,
	indiraClient *indira.ExecutionClient,
	kafkaPub *publisher.KafkaPublisher,
	statusSvc *statusservice.OrderStatusService,
	maxRetries int,
	retryDelay time.Duration,
) *OrderExecutor {
	return &OrderExecutor{
		repo:          repo,
		credsCache:    NewCredentialsCache(credsRepo),
		indiraClient:  indiraClient,
		kafkaPub:      kafkaPub,
		statusSvc:     statusSvc,
		paperExecutor: NewPaperOrderExecutor(repo, kafkaPub, nil),
		maxRetries:    maxRetries,
		retryDelay:    retryDelay,
	}
}

// ExecuteOrder processes and executes an order
// In services/trade-execution/internal/executor/executor.go

func (e *OrderExecutor) ExecuteOrder(ctx context.Context, order *models.Order) error {
	log.Printf("Executing order %s for user %s [trading_mode=%q is_paper=%v]",
		order.OrderID, order.UserID, order.TradingMode, order.IsPaperTrade)

	// Verify risk approval
	if !order.RiskApproved {
		log.Printf("Order %s not approved by risk management", order.OrderID)
		return e.rejectOrder(ctx, order, "Risk not approved")
	}

	// ── PAPER TRADING: bypass broker entirely ──────────────────────────────
	if order.IsPaperTrade {
		log.Printf("[paper] Routing order %s to paper executor (mode=%q)", order.OrderID, order.TradingMode)
		return e.paperExecutor.ExecutePaperOrder(ctx, order)
	}
	// ───────────────────────────────────────────────────────────────────────
	if order.TradingMode == "" {
		log.Printf("[WARN] Order %s has empty TradingMode — routing LIVE. Verify strategy TradingMode in rules-engine configstore.", order.OrderID)
	}

	// Update status to PENDING asynchronously to avoid blocking the hot path
	go func() {
		orderCopy := *order
		orderCopy.Status = models.StatusPending
		if err := e.repo.Update(context.Background(), &orderCopy); err != nil {
			log.Printf("Background DB Error: failed to update order %s status to PENDING: %v", orderCopy.OrderID, err)
		}
	}()

	// Get Indira credentials: prefer values carried on the order (from frontend),
	// but fall back to the DB credentials store for signals from the rules engine
	// that don't carry per-request auth data.
	var auth *indiraClient.AuthContext

	if order.UserID != "" && order.AppId != nil && order.Source != nil && order.BearerToken != nil {
		// Credentials were embedded in the signal — use them directly.
		auth = &indiraClient.AuthContext{
			UserId:      order.UserID,
			AppId:       *order.AppId,
			Source:      *order.Source,
			BearerToken: *order.BearerToken,
		}
	} else {
		// Fall back: load credentials from cache (DB on first miss) for this user.
		if e.credsCache == nil {
			return e.failOrder(ctx, order, "Missing Indira Securities authentication data and no credentials cache available")
		}
		userId, appId, source, bearerToken, err := e.credsCache.Get(ctx, order.UserID)
		if err != nil {
			return e.failOrder(ctx, order, "Missing Indira Securities authentication data: "+err.Error())
		}
		log.Printf("Loaded credentials for user %s (cache or DB)", order.UserID)
		auth = &indiraClient.AuthContext{
			UserId:      userId,
			AppId:       appId,
			Source:      source,
			BearerToken: bearerToken,
		}
	}

	// Execute order with retries + idempotency guard.
	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := e.retryDelay * time.Duration(attempt)
			log.Printf("Retrying order %s, attempt %d after %v", order.OrderID, attempt, delay)
			time.Sleep(delay)

			// Before retrying after a timeout, check if the order was already placed.
			// A timeout means the request may have reached the broker even though we got no response.
			// Placing again would create a duplicate position.
			if isTimeoutError(lastErr) {
				log.Printf("[idempotency] Timeout on attempt %d for order %s — checking broker order book before retry",
					attempt, order.OrderID)
				if foundID, ok := e.indiraClient.FindRecentOrder(
					ctx, auth, order.Symbol, string(order.OrderSide), int(order.Quantity),
				); ok {
					log.Printf("[idempotency] Order %s already exists at broker as %s — skipping retry",
						order.OrderID, foundID)
					order.IndiraOrderID = &foundID
					return e.handleSuccessfulPlacement(ctx, order, foundID, auth)
				}
				log.Printf("[idempotency] Order %s not found in broker book — safe to retry", order.OrderID)
			}
		}

		// Place order via Indira API
		orderID, err := e.indiraClient.PlaceOrder(ctx, order, auth)
		if err != nil {
			// Broker business rejections (e.g. EG003) are deterministic — retrying won't help.
			var brokerErr *indiraClient.BrokerBusinessError
			if errors.As(err, &brokerErr) {
				log.Printf("❌ Broker rejected order %s (no retry): %v", order.OrderID, brokerErr)
				return e.failOrder(ctx, order, brokerErr.Error())
			}
			lastErr = err
			order.RetryCount++
			log.Printf("Failed to place order %s (attempt %d): %v", order.OrderID, attempt+1, err)
			if attempt == e.maxRetries {
				log.Printf("❌ BROKER CONNECTION ERROR - Max retries exhausted. Check:")
				log.Printf("  1. Is INDIRA_BASE_URL environment variable set?")
				log.Printf("  2. Is the broker backend running at that URL?")
				log.Printf("  3. Is the network connection working?")
			}
			continue
		}

		// Order placed successfully - store the Indira order ID
		order.IndiraOrderID = &orderID
		return e.handleSuccessfulPlacement(ctx, order, orderID, auth)
	}

	// All retries exhausted
	log.Printf("Max retries exhausted for order %s", order.OrderID)
	return e.failOrder(ctx, order, fmt.Sprintf("Max retries exceeded: %v", lastErr))
}

func (e *OrderExecutor) handleSuccessfulPlacement(ctx context.Context, order *models.Order, indiraOrderID string, auth *indiraClient.AuthContext) error {
	// Update local memory
	now := time.Now()
	order.Status = models.StatusSubmitted
	order.SubmittedAt = &now
	order.IndiraOrderID = &indiraOrderID

	// Perform DB updates + publish order-update in a background goroutine
	go func() {
		bgCtx := context.Background()
		orderCopy := *order

		if err := e.repo.Update(bgCtx, &orderCopy); err != nil {
			log.Printf("Background DB Error: failed to update order %s after placement: %v", orderCopy.OrderID, err)
			// Continue — still want to publish Kafka event and start WS
		}

		// Record execution event in DB
		e.repo.RecordExecutionEvent(bgCtx, orderCopy.OrderID, "SUBMITTED", map[string]interface{}{
			"indira_order_id": indiraOrderID,
			"timestamp":       now,
		})

		// Publish ORDER_SUBMITTED to Kafka order-updates topic
		e.publishOrderUpdate(bgCtx, &orderCopy, "ORDER_SUBMITTED", "MEDIUM",
			"Order Submitted",
			fmt.Sprintf("Order for %s submitted to broker (ref: %s)", orderCopy.Symbol, indiraOrderID))
	}()

	// Start backend-side WebSocket subscription to Indira for real-time order status.
	// This is idempotent — no-op if already subscribed for this user.
	if e.statusSvc != nil && auth != nil {
		go func() {
			if err := e.statusSvc.StartSubscription(context.Background(), order.UserID, auth); err != nil {
				log.Printf("[executor] WS subscription for user %s failed (non-fatal): %v", order.UserID, err)
			}
		}()
	} else if e.statusSvc == nil {
		log.Printf("[executor] statusSvc not configured — skipping WS subscription for order %s", order.OrderID)
	} else {
		log.Printf("[executor] Missing auth on order %s — cannot start WS subscription", order.OrderID)
	}

	if e.wsBroadcaster != nil {
		e.wsBroadcaster(order.UserID, "new_order", order)
	}

	log.Printf("Order %s submitted successfully with indira_order_id: %s", order.OrderID, indiraOrderID)
	return nil
}

func (e *OrderExecutor) rejectOrder(ctx context.Context, order *models.Order, reason string) error {
	log.Printf("Rejecting order %s: %s", order.OrderID, reason)

	order.Status = models.StatusRejected
	order.RejectionReason = &reason

	if err := e.repo.Update(ctx, order); err != nil {
		log.Printf("[executor] failed to update rejected order %s in DB: %v", order.OrderID, err)
	}

	e.repo.RecordExecutionEvent(ctx, order.OrderID, "REJECTED", map[string]interface{}{
		"reason":    reason,
		"timestamp": time.Now(),
	})

	// Publish ORDER_REJECTED to Kafka
	e.publishOrderUpdate(ctx, order, "ORDER_REJECTED", "HIGH",
		"Order Rejected ✗",
		fmt.Sprintf("Order for %s was rejected: %s", order.Symbol, reason))

	return fmt.Errorf("order rejected: %s", reason)
}

func (e *OrderExecutor) failOrder(ctx context.Context, order *models.Order, errorMsg string) error {
	log.Printf("Failing order %s: %s", order.OrderID, errorMsg)

	order.Status = models.StatusFailed
	order.ErrorMessage = &errorMsg

	if err := e.repo.Update(ctx, order); err != nil {
		log.Printf("[executor] failed to update failed order %s in DB: %v", order.OrderID, err)
	}

	e.repo.RecordExecutionEvent(ctx, order.OrderID, "FAILED", map[string]interface{}{
		"error":     errorMsg,
		"timestamp": time.Now(),
	})

	// Publish ORDER_FAILED to Kafka
	e.publishOrderUpdate(ctx, order, "ORDER_FAILED", "HIGH",
		"Order Failed ✗",
		fmt.Sprintf("Order for %s failed: %s", order.Symbol, errorMsg))

	return fmt.Errorf("order failed: %s", errorMsg)
}

// publishOrderUpdate publishes a lightweight order-update notification to the Kafka order-updates topic.
// Safe to call even if kafkaPub is nil (degraded mode — logs and skips).
func (e *OrderExecutor) publishOrderUpdate(ctx context.Context, order *models.Order, updateType, priority, title, message string) {
	if e.kafkaPub == nil {
		log.Printf("[executor] kafkaPub not configured — skipping order-update for %s (%s)", order.OrderID, updateType)
		return
	}
	now := time.Now()
	update := &models.OrderUpdate{
		UpdateID:  uuid.New().String(),
		OrderID:   order.OrderID.String(),
		UserID:    order.UserID,
		CreatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
		UpdateType: updateType,
		Priority:   priority,
		Title:      title,
		Message:    message,
		Status:     string(order.Status),
		OrderSummary: models.OrderSummary{
			Stock:     order.Symbol,
			Action:    string(order.OrderSide),
			Quantity:  order.Quantity,
			Exchange:  string(order.Exchange),
			OrderType: string(order.OrderType),
		},
		NotificationChannels: models.NotificationChannels{
			Push:  true,
			Email: false,
			InApp: true,
		},
	}
	if order.Price != nil {
		update.OrderSummary.Price = fmt.Sprintf("₹%.2f", *order.Price)
	} else {
		update.OrderSummary.Price = "MARKET"
	}
	if err := e.kafkaPub.PublishOrderUpdate(ctx, update); err != nil {
		log.Printf("[executor] failed to publish order-update (%s) for order %s: %v", updateType, order.OrderID, err)
	} else {
		log.Printf("[executor] ✓ Published order-update: type=%s order=%s user=%s symbol=%s",
			updateType, order.OrderID, order.UserID, order.Symbol)
	}
}

// isTimeoutError returns true if err represents a network timeout or context deadline exceeded.
// On timeout, the broker may have received the order even though we got no response —
// so we must check the order book before retrying to avoid duplicates.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// CancelOrder cancels an order
func (e *OrderExecutor) CancelOrder(ctx context.Context, order *models.Order, reason string) error {
	log.Printf("Cancelling order %s: %s", order.OrderID, reason)

	// Check if order can be cancelled
	if order.Status == models.StatusFilled || order.Status == models.StatusCancelled {
		return fmt.Errorf("order cannot be cancelled (status: %s)", order.Status)
	}

	// If order has been submitted to Indira, cancel it there too
	if order.IndiraOrderID != nil {
		// Need AuthContext to cancel
		if order.BearerToken != nil && order.AppId != nil && order.Source != nil {
			auth := &indiraClient.AuthContext{
				UserId:      order.UserID,
				BearerToken: *order.BearerToken,
				AppId:       *order.AppId,
				Source:      *order.Source,
			}

			err := e.indiraClient.CancelOrder(ctx, string(order.Exchange), *order.IndiraOrderID, order.Symbol, auth)
			if err != nil {
				log.Printf("Failed to cancel order on Indira: %v", err)
				// Continue with local cancellation even if Indira fails
			}
		} else {
			log.Printf("Warning: Cannot cancel order on Indira - missing auth data")
		}
	}

	// Update local status
	order.Status = models.StatusCancelled
	order.RejectionReason = &reason

	if err := e.repo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update cancelled order: %w", err)
	}

	// Record cancellation event
	e.repo.RecordExecutionEvent(ctx, order.OrderID, "CANCELLED", map[string]interface{}{
		"reason":    reason,
		"timestamp": time.Now(),
	})

	return nil
}
