package executor

import (
	"context"
	"fmt"
	"log"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

// OrderExecutor handles order execution logic
type OrderExecutor struct {
	repo         repository.OrderRepository
	credsRepo    repository.CredentialsRepository
	indiraClient *indira.ExecutionClient
	maxRetries   int
	retryDelay   time.Duration
}

// NewOrderExecutor creates a new order executor
func NewOrderExecutor(repo repository.OrderRepository, credsRepo repository.CredentialsRepository, indiraClient *indira.ExecutionClient, maxRetries int, retryDelay time.Duration) *OrderExecutor {
	return &OrderExecutor{
		repo:         repo,
		credsRepo:    credsRepo,
		indiraClient: indiraClient,
		maxRetries:   maxRetries,
		retryDelay:   retryDelay,
	}
}

// ExecuteOrder processes and executes an order
// In services/trade-execution/internal/executor/executor.go

func (e *OrderExecutor) ExecuteOrder(ctx context.Context, order *models.Order) error {
	log.Printf("Executing order %s for user %s", order.OrderID, order.UserID)

	// Verify risk approval
	if !order.RiskApproved {
		log.Printf("Order %s not approved by risk management", order.OrderID)
		return e.rejectOrder(ctx, order, "Risk not approved")
	}

	// Update status to PENDING
	order.Status = models.StatusPending
	if err := e.repo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update order status to PENDING: %w", err)
	}

	// Get Indira credentials from the order (passed from frontend)
	// Note: UserID is a non-pointer string on the Order model
	if order.UserID == "" || order.AppId == nil || order.Source == nil || order.BearerToken == nil {
		return e.failOrder(ctx, order, "Missing Indira Securities authentication data")
	}

	auth := &indiraClient.AuthContext{
		UserId:      order.UserID,
		AppId:       *order.AppId,
		Source:      *order.Source,
		BearerToken: *order.BearerToken,
	}

	// Execute order with retries
	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := e.retryDelay * time.Duration(attempt)
			log.Printf("Retrying order %s, attempt %d after %v", order.OrderID, attempt, delay)
			time.Sleep(delay)
		}

		// Place order via Indira API
		orderID, err := e.indiraClient.PlaceOrder(ctx, order, auth)
		if err != nil {
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
		return e.handleSuccessfulPlacement(ctx, order, orderID)
	}

	// All retries exhausted
	log.Printf("Max retries exhausted for order %s", order.OrderID)
	return e.failOrder(ctx, order, fmt.Sprintf("Max retries exceeded: %v", lastErr))
}

func (e *OrderExecutor) handleSuccessfulPlacement(ctx context.Context, order *models.Order, indiraOrderID string) error {
	now := time.Now()
	order.Status = models.StatusSubmitted
	order.SubmittedAt = &now

	// Store the Indira order ID returned from API
	order.IndiraOrderID = &indiraOrderID

	if err := e.repo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update order after placement: %w", err)
	}

	// Record execution event
	e.repo.RecordExecutionEvent(ctx, order.OrderID, "SUBMITTED", map[string]interface{}{
		"indira_order_id": indiraOrderID,
		"timestamp":       now,
	})

	log.Printf("Order %s submitted successfully with indira_order_id: %s", order.OrderID, indiraOrderID)
	return nil
}

func (e *OrderExecutor) rejectOrder(ctx context.Context, order *models.Order, reason string) error {
	log.Printf("Rejecting order %s: %s", order.OrderID, reason)

	order.Status = models.StatusRejected
	order.RejectionReason = &reason

	if err := e.repo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update rejected order: %w", err)
	}

	// Record rejection event
	e.repo.RecordExecutionEvent(ctx, order.OrderID, "REJECTED", map[string]interface{}{
		"reason":    reason,
		"timestamp": time.Now(),
	})

	return fmt.Errorf("order rejected: %s", reason)
}

func (e *OrderExecutor) failOrder(ctx context.Context, order *models.Order, errorMsg string) error {
	log.Printf("Failing order %s: %s", order.OrderID, errorMsg)

	order.Status = models.StatusFailed
	order.ErrorMessage = &errorMsg

	if err := e.repo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update failed order: %w", err)
	}

	// Record failure event
	e.repo.RecordExecutionEvent(ctx, order.OrderID, "FAILED", map[string]interface{}{
		"error":     errorMsg,
		"timestamp": time.Now(),
	})

	return fmt.Errorf("order failed: %s", errorMsg)
}

// PollOrderStatus polls Indira for order status updates
func (e *OrderExecutor) PollOrderStatus(ctx context.Context, order *models.Order) error {
	if order.IndiraOrderID == nil {
		return fmt.Errorf("no Indira order ID for order %s", order.OrderID)
	}

	log.Printf("Polling status for order %s (indira_order_id: %s)", order.OrderID, *order.IndiraOrderID)

	// Need AuthContext to poll status
	if order.BearerToken == nil || order.AppId == nil || order.Source == nil {
		return fmt.Errorf("missing authentication data for status polling")
	}

	auth := &indiraClient.AuthContext{
		UserId:      order.UserID,
		BearerToken: *order.BearerToken,
		AppId:       *order.AppId,
		Source:      *order.Source,
	}

	_, err := e.indiraClient.GetOrderStatus(ctx, *order.IndiraOrderID, auth)
	if err != nil {
		return fmt.Errorf("failed to get order status: %w", err)
	}

	// TODO: Parse response and update order based on status
	// This depends on the actual Indira API response structure

	log.Printf("Order %s status updated from Indira API", order.OrderID)
	return e.repo.Update(ctx, order)
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
