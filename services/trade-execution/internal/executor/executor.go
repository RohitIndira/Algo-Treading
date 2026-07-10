package executor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/orderstatus"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// OrderExecutor handles order execution logic
type OrderExecutor struct {
	repo          repository.OrderRepository
	credsCache    *CredentialsCache // in-memory credential store; avoids per-order DB hit
	indiraClient  *indira.ExecutionClient
	kafkaPub      *publisher.KafkaPublisher
	statusSvc     *orderstatus.OrderStatusService
	paperExecutor *PaperOrderExecutor
	wsBroadcaster func(userID string, eventType string, order *models.Order)
	logger        *zap.Logger
	maxRetries    int
	retryDelay    time.Duration
}

// CredentialsCache returns the internal credentials cache for pre-warming on startup.
func (e *OrderExecutor) CredentialsCache() *CredentialsCache {
	return e.credsCache
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
	statusSvc *orderstatus.OrderStatusService,
	logger *zap.Logger,
	maxRetries int,
	retryDelay time.Duration,
) *OrderExecutor {
	return &OrderExecutor{
		repo:          repo,
		credsCache:    NewCredentialsCache(credsRepo),
		indiraClient:  indiraClient,
		kafkaPub:      kafkaPub,
		statusSvc:     statusSvc,
		logger:        logger.Named("executor"),
		paperExecutor: NewPaperOrderExecutor(repo, kafkaPub, nil),
		maxRetries:    maxRetries,
		retryDelay:    retryDelay,
	}
}

// ExecuteOrder processes and executes an order
// In services/trade-execution/internal/executor/executor.go

func (e *OrderExecutor) ExecuteOrder(ctx context.Context, order *models.Order) error {
	start := time.Now()
	mode := "live"
	if order.IsPaperTrade {
		mode = "paper"
	}
	defer func() {
		metrics.OrderLatency.WithLabelValues(mode).Observe(time.Since(start).Seconds())
	}()

	e.logger.Info("exec_start",
		zap.String("oid", order.OrderID.String()),
		zap.String("uid", order.UserID),
		zap.String("sym", order.Symbol),
		zap.String("mode", mode))

	// Verify risk approval
	if !order.RiskApproved {
		metrics.OrdersTotal.WithLabelValues("rejected", mode).Inc()
		return e.rejectOrder(ctx, order, "Risk not approved")
	}

	// ── PAPER TRADING: bypass broker entirely ──────────────────────────────
	if order.IsPaperTrade {
		err := e.paperExecutor.ExecutePaperOrder(ctx, order)
		if err != nil {
			metrics.OrdersTotal.WithLabelValues("failed", mode).Inc()
		} else {
			metrics.OrdersTotal.WithLabelValues("submitted", mode).Inc()
		}
		return err
	}

	// Mark as PENDING in memory (DB will be updated to SUBMITTED after PlaceOrder succeeds)
	order.Status = models.StatusPending

	// Get Indira credentials
	credStart := time.Now()
	var auth *indiraClient.AuthContext
	var credSource string

	if order.UserID != "" && order.AppId != nil && order.Source != nil && order.BearerToken != nil {
		credSource = "signal"
		auth = &indiraClient.AuthContext{
			UserId:      order.UserID,
			AppId:       *order.AppId,
			Source:      *order.Source,
			BearerToken: *order.BearerToken,
		}
	} else {
		if e.credsCache == nil {
			return e.failOrder(ctx, order, "Missing Indira Securities authentication data and no credentials cache available")
		}
		userId, appId, source, bearerToken, err := e.credsCache.Get(ctx, order.UserID)
		if err != nil {
			return e.failOrder(ctx, order, "Missing Indira Securities authentication data: "+err.Error())
		}
		credSource = "cache"
		auth = &indiraClient.AuthContext{
			UserId:      userId,
			AppId:       appId,
			Source:      source,
			BearerToken: bearerToken,
		}
	}
	credMs := float64(time.Since(credStart).Microseconds()) / 1000.0
	metrics.CredentialLookupDuration.WithLabelValues(credSource).Observe(time.Since(credStart).Seconds())

	// Execute order with retries + idempotency guard.
	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			metrics.BrokerRetries.Inc()
			delay := e.retryDelay * time.Duration(attempt)
			e.logger.Warn("exec_retry",
				zap.String("oid", order.OrderID.String()),
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}

			// On 401 / session-expired, reload credentials from DB.
			if isSessionExpiredError(lastErr) && e.credsCache != nil {
				e.credsCache.Invalidate(order.UserID)
				userId, appId, source, bearerToken, err := e.credsCache.Get(ctx, order.UserID)
				if err != nil {
					e.logger.Error("auth_reload_failed", zap.String("uid", order.UserID), zap.Error(err))
				} else if bearerToken != auth.BearerToken {
					auth = &indiraClient.AuthContext{
						UserId: userId, AppId: appId, Source: source, BearerToken: bearerToken,
					}
				}
			}

			// Before retrying after a timeout, check broker order book to prevent duplicates.
			if isTimeoutError(lastErr) {
				if foundID, ok := e.indiraClient.FindRecentOrder(
					ctx, auth, order.Symbol, string(order.OrderSide), int(order.Quantity),
				); ok {
					e.logger.Info("exec_idempotent_hit",
						zap.String("oid", order.OrderID.String()),
						zap.String("broker_id", foundID))
					order.IndiraOrderID = &foundID
					return e.handleSuccessfulPlacement(ctx, order, foundID, auth)
				}
			}
		}

		// Place order via Indira API — measure single call duration
		apiStart := time.Now()
		orderID, err := e.indiraClient.PlaceOrder(ctx, order, auth)
		apiMs := float64(time.Since(apiStart).Microseconds()) / 1000.0
		metrics.BrokerAPICallDuration.Observe(time.Since(apiStart).Seconds())

		if err != nil {
			var brokerErr *indiraClient.BrokerBusinessError
			if errors.As(err, &brokerErr) {
				e.logger.Error("exec_broker_rejected",
					zap.String("oid", order.OrderID.String()),
					zap.Error(brokerErr))
				metrics.BrokerErrors.WithLabelValues("business").Inc()
				metrics.OrdersTotal.WithLabelValues("failed", "live").Inc()
				return e.failOrder(ctx, order, brokerErr.Error())
			}
			// Classify error for metrics
			if isTimeoutError(err) {
				metrics.BrokerErrors.WithLabelValues("timeout").Inc()
			} else if isSessionExpiredError(err) {
				metrics.BrokerErrors.WithLabelValues("auth").Inc()
			} else {
				metrics.BrokerErrors.WithLabelValues("network").Inc()
			}
			lastErr = err
			order.RetryCount++
			e.logger.Error("exec_place_failed",
				zap.String("oid", order.OrderID.String()),
				zap.Int("attempt", attempt+1),
				zap.Error(err))
			continue
		}

		// Order placed successfully
		order.IndiraOrderID = &orderID
		metrics.OrdersTotal.WithLabelValues("submitted", "live").Inc()
		e.logger.Info("exec_submitted",
			zap.String("oid", order.OrderID.String()),
			zap.String("broker_id", orderID),
			zap.String("sym", order.Symbol),
			zap.String("uid", order.UserID),
			zap.Float64("cred_ms", credMs),
			zap.Float64("broker_api_ms", apiMs),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0))
		return e.handleSuccessfulPlacement(ctx, order, orderID, auth)
	}

	// All retries exhausted
	metrics.OrdersTotal.WithLabelValues("failed", "live").Inc()
	e.logger.Error("exec_retries_exhausted",
		zap.String("oid", order.OrderID.String()),
		zap.Error(lastErr))
	return e.failOrder(ctx, order, fmt.Sprintf("Max retries exceeded: %v", lastErr))
}

func (e *OrderExecutor) handleSuccessfulPlacement(ctx context.Context, order *models.Order, indiraOrderID string, auth *indiraClient.AuthContext) error {
	now := time.Now()
	order.Status = models.StatusSubmitted
	order.SubmittedAt = &now
	order.IndiraOrderID = &indiraOrderID

	// For square-off/exit orders, save the DB record synchronously BEFORE
	// returning. This ensures the indira_order_id is in the DB when the
	// broker WS fires the EXECUTED callback — preventing the status service
	// from misidentifying it as a "manual exit" and cancelling unrelated
	// OCO groups by symbol.
	if order.IsSquareOffOrder {
		orderCopy := *order
		if err := e.repo.Update(ctx, &orderCopy); err != nil {
			e.logger.Error("sync_db_update_failed", zap.String("oid", orderCopy.OrderID.String()), zap.Error(err))
		}
		go func() {
			bgCtx := context.Background()
			oc := orderCopy
			e.repo.RecordExecutionEvent(bgCtx, oc.OrderID, "SUBMITTED", map[string]interface{}{
				"indira_order_id": indiraOrderID,
				"timestamp":       now,
			})
			e.publishOrderUpdate(bgCtx, &oc, "ORDER_SUBMITTED", "MEDIUM",
				"Order Submitted",
				fmt.Sprintf("Order for %s submitted to broker (ref: %s)", oc.Symbol, indiraOrderID))
		}()
	} else {
		// Normal orders: background DB updates + publish concurrently for speed.
		go func() {
			bgCtx := context.Background()
			orderCopy := *order

			var wg sync.WaitGroup
			wg.Add(3)

			go func() {
				defer wg.Done()
				if err := e.repo.Update(bgCtx, &orderCopy); err != nil {
					e.logger.Error("bg_db_update_failed", zap.String("oid", orderCopy.OrderID.String()), zap.Error(err))
				}
			}()

			go func() {
				defer wg.Done()
				e.repo.RecordExecutionEvent(bgCtx, orderCopy.OrderID, "SUBMITTED", map[string]interface{}{
					"indira_order_id": indiraOrderID,
					"timestamp":       now,
				})
			}()

			go func() {
				defer wg.Done()
				e.publishOrderUpdate(bgCtx, &orderCopy, "ORDER_SUBMITTED", "MEDIUM",
					"Order Submitted",
					fmt.Sprintf("Order for %s submitted to broker (ref: %s)", orderCopy.Symbol, indiraOrderID))
			}()

			wg.Wait()
		}()
	}

	// Start broker WS subscription (idempotent).
	if e.statusSvc != nil && auth != nil {
		go func() {
			if err := e.statusSvc.StartSubscription(context.Background(), order.UserID, auth); err != nil {
				e.logger.Warn("ws_sub_failed", zap.String("uid", order.UserID), zap.Error(err))
			}
		}()
	}

	if e.wsBroadcaster != nil {
		e.wsBroadcaster(order.UserID, "new_order", order)
	}

	return nil
}

func (e *OrderExecutor) rejectOrder(ctx context.Context, order *models.Order, reason string) error {
	e.logger.Warn("exec_rejected",
		zap.String("oid", order.OrderID.String()),
		zap.String("sym", order.Symbol),
		zap.String("reason", reason))

	order.Status = models.StatusRejected
	order.RejectionReason = &reason

	if err := e.repo.Update(ctx, order); err != nil {
		e.logger.Error("db_update_failed", zap.String("oid", order.OrderID.String()), zap.Error(err))
	}

	e.repo.RecordExecutionEvent(ctx, order.OrderID, "REJECTED", map[string]interface{}{
		"reason":    reason,
		"timestamp": time.Now(),
	})

	e.publishOrderUpdate(ctx, order, "ORDER_REJECTED", "HIGH",
		"Order Rejected",
		fmt.Sprintf("Order for %s was rejected: %s", order.Symbol, reason))

	return fmt.Errorf("order rejected: %s", reason)
}

func (e *OrderExecutor) failOrder(ctx context.Context, order *models.Order, errorMsg string) error {
	e.logger.Error("exec_failed",
		zap.String("oid", order.OrderID.String()),
		zap.String("sym", order.Symbol),
		zap.String("err", errorMsg))

	order.Status = models.StatusFailed
	order.ErrorMessage = &errorMsg

	if err := e.repo.Update(ctx, order); err != nil {
		e.logger.Error("db_update_failed", zap.String("oid", order.OrderID.String()), zap.Error(err))
	}

	e.repo.RecordExecutionEvent(ctx, order.OrderID, "FAILED", map[string]interface{}{
		"error":     errorMsg,
		"timestamp": time.Now(),
	})

	e.publishOrderUpdate(ctx, order, "ORDER_FAILED", "HIGH",
		"Order Failed",
		fmt.Sprintf("Order for %s failed: %s", order.Symbol, errorMsg))

	return fmt.Errorf("order failed: %s", errorMsg)
}

// publishOrderUpdate publishes a lightweight order-update notification to the Kafka order-updates topic.
func (e *OrderExecutor) publishOrderUpdate(ctx context.Context, order *models.Order, updateType, priority, title, message string) {
	if e.kafkaPub == nil {
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
		e.logger.Error("kafka_publish_failed",
			zap.String("oid", order.OrderID.String()),
			zap.String("type", updateType),
			zap.Error(err))
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

// isSessionExpiredError returns true if the error indicates a 401 / session-expired
// response from the broker. This means the cached bearer token is stale and the
// credentials cache should be invalidated so a fresh token is loaded from the DB.
func isSessionExpiredError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "HTTP error 401") || strings.Contains(msg, "Session expired")
}

// CancelOrder cancels an order
func (e *OrderExecutor) CancelOrder(ctx context.Context, order *models.Order, reason string) error {
	if models.IsTerminalStatus(order.Status) {
		return fmt.Errorf("order cannot be cancelled (status: %s)", order.Status)
	}

	// If order has been submitted to Indira, cancel it there too
	if order.IndiraOrderID != nil {
		if order.BearerToken != nil && order.AppId != nil && order.Source != nil {
			auth := &indiraClient.AuthContext{
				UserId:      order.UserID,
				BearerToken: *order.BearerToken,
				AppId:       *order.AppId,
				Source:      *order.Source,
			}
			if err := e.indiraClient.CancelOrder(ctx, string(order.Exchange), *order.IndiraOrderID, order.Symbol, auth); err != nil {
				e.logger.Warn("broker_cancel_failed", zap.String("oid", order.OrderID.String()), zap.Error(err))
			}
		} else {
			e.logger.Warn("cancel_no_auth", zap.String("oid", order.OrderID.String()))
		}
	}

	order.Status = models.StatusCancelled
	order.RejectionReason = &reason

	if err := e.repo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update cancelled order: %w", err)
	}

	e.repo.RecordExecutionEvent(ctx, order.OrderID, "CANCELLED", map[string]interface{}{
		"reason":    reason,
		"timestamp": time.Now(),
	})

	e.logger.Info("exec_cancelled", zap.String("oid", order.OrderID.String()), zap.String("reason", reason))
	return nil
}
