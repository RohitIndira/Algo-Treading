package executor

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/odin"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

// OrderExecutor handles order execution logic
type OrderExecutor struct {
	repo       repository.OrderRepository
	odinClient *odin.ExecutionClient
	maxRetries int
	retryDelay time.Duration
}

// NewOrderExecutor creates a new order executor
func NewOrderExecutor(repo repository.OrderRepository, odinClient *odin.ExecutionClient, maxRetries int, retryDelay time.Duration) *OrderExecutor {
	return &OrderExecutor{
		repo:       repo,
		odinClient: odinClient,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

// ExecuteOrder processes and executes an order
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

	// Execute order with retries
	var lastErr error
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := e.retryDelay * time.Duration(attempt)
			log.Printf("Retrying order %s, attempt %d after %v", order.OrderID, attempt, delay)
			time.Sleep(delay)
		}

		// Place order via Odin
		orderID, err := e.odinClient.PlaceOrder(ctx, order, order.UserID)
		if err != nil {
			lastErr = err
			order.RetryCount++
			log.Printf("Failed to place order %s: %v", order.OrderID, err)
			continue
		}

		// Order placed successfully - store the Odin order ID
		order.OdinOrderID = &orderID
		return e.handleSuccessfulPlacement(ctx, order, orderID)
	}

	// All retries exhausted
	log.Printf("Max retries exhausted for order %s", order.OrderID)
	return e.failOrder(ctx, order, fmt.Sprintf("Max retries exceeded: %v", lastErr))
}

func (e *OrderExecutor) handleSuccessfulPlacement(ctx context.Context, order *models.Order, odinOrderID string) error {
	now := time.Now()
	order.Status = models.StatusSubmitted
	order.SubmittedAt = &now

	// Store the Odin order ID returned from API
	order.OdinOrderID = &odinOrderID

	if err := e.repo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update order after placement: %w", err)
	}

	// Record execution event
	e.repo.RecordExecutionEvent(ctx, order.OrderID, "SUBMITTED", map[string]interface{}{
		"odin_order_id": odinOrderID,
		"timestamp":     now,
	})

	log.Printf("Order %s submitted successfully with odin_order_id: %s", order.OrderID, odinOrderID)
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

	return nil
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

	return nil
}

// PollOrderStatus polls Odin for order status updates
func (e *OrderExecutor) PollOrderStatus(ctx context.Context, order *models.Order) error {
	if order.OdinOrderID == nil {
		return fmt.Errorf("no Odin order ID for order %s", order.OrderID)
	}

	log.Printf("Polling status for order %s (odin_order_id: %s)", order.OrderID, *order.OdinOrderID)

	_, err := e.odinClient.GetOrderStatus(ctx, string(order.Exchange), *order.OdinOrderID, order.UserID)
	if err != nil {
		return fmt.Errorf("failed to get order status: %w", err)
	}

	// TODO: Parse response and update order based on status
	// This depends on the actual Odin API response structure

	log.Printf("Order %s status updated from Odin API", order.OrderID)
	return e.repo.Update(ctx, order)
}

// CancelOrder cancels an order
func (e *OrderExecutor) CancelOrder(ctx context.Context, order *models.Order, reason string) error {
	log.Printf("Cancelling order %s: %s", order.OrderID, reason)

	// Check if order can be cancelled
	if order.Status == models.StatusFilled || order.Status == models.StatusCancelled {
		return fmt.Errorf("order cannot be cancelled (status: %s)", order.Status)
	}

	// If order has been submitted to Odin, cancel it there too
	if order.OdinOrderID != nil {
		err := e.odinClient.CancelOrder(ctx, string(order.Exchange), *order.OdinOrderID, order.UserID)
		if err != nil {
			log.Printf("Failed to cancel order on Odin: %v", err)
			// Continue with local cancellation even if Odin fails
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
