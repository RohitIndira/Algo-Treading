package scheduler

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

// TrailingStopLossMonitor monitors and adjusts trailing stop losses
type TrailingStopLossMonitor struct {
	orderRepo     repository.OrderRepository
	indiraClient  *indira.ExecutionClient
	credsRepo     repository.CredentialsRepository
	checkInterval time.Duration
	stopChan      chan struct{}
}

// TrailingStopLossState tracks the state of a trailing stop loss
type TrailingStopLossState struct {
	OrderID         string
	HighestPrice    float64 // Highest price reached since entry
	CurrentStopLoss float64 // Current stop loss price
	TrailingPct     float64 // Trailing percentage
	LastUpdated     time.Time
	StopLossOrderID string // ID of the stop loss order placed
}

// NewTrailingStopLossMonitor creates a new trailing stop loss monitor
func NewTrailingStopLossMonitor(
	orderRepo repository.OrderRepository,
	indiraClient *indira.ExecutionClient,
	credsRepo repository.CredentialsRepository,
	checkInterval time.Duration,
) *TrailingStopLossMonitor {
	if checkInterval == 0 {
		checkInterval = 30 * time.Second // Default: check every 30 seconds
	}

	return &TrailingStopLossMonitor{
		orderRepo:     orderRepo,
		indiraClient:  indiraClient,
		credsRepo:     credsRepo,
		checkInterval: checkInterval,
		stopChan:      make(chan struct{}),
	}
}

// Start starts the trailing stop loss monitor
func (m *TrailingStopLossMonitor) Start(ctx context.Context) error {
	log.Printf("Starting Trailing Stop Loss Monitor (Check interval: %v)", m.checkInterval)

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Trailing Stop Loss Monitor stopped")
			return nil

		case <-m.stopChan:
			log.Println("Trailing Stop Loss Monitor stopped")
			return nil

		case <-ticker.C:
			if err := m.monitorTrailingStopLosses(ctx); err != nil {
				log.Printf("Error monitoring trailing stop losses: %v", err)
			}
		}
	}
}

// Stop stops the monitor
func (m *TrailingStopLossMonitor) Stop() {
	close(m.stopChan)
}

// monitorTrailingStopLosses checks and updates all trailing stop losses
func (m *TrailingStopLossMonitor) monitorTrailingStopLosses(ctx context.Context) error {
	// Get all filled orders with trailing stop loss enabled
	orders, err := m.getOrdersWithTrailingStopLoss(ctx)
	if err != nil {
		return fmt.Errorf("failed to get orders with trailing SL: %w", err)
	}

	if len(orders) == 0 {
		return nil
	}

	log.Printf("Monitoring %d orders with trailing stop loss", len(orders))

	for _, order := range orders {
		if err := m.updateTrailingStopLoss(ctx, order); err != nil {
			log.Printf("Failed to update trailing SL for order %s: %v", order.OrderID, err)
		}
	}

	return nil
}

// getOrdersWithTrailingStopLoss retrieves orders that have trailing stop loss enabled
func (m *TrailingStopLossMonitor) getOrdersWithTrailingStopLoss(ctx context.Context) ([]*models.Order, error) {
	// Fetch all orders with status = FILLED or PARTIALLY_FILLED, stop_loss_type = TRAILING, trailing_sl_pct > 0
	return m.orderRepo.GetTrailingStopLossOrders(ctx)
}

// updateTrailingStopLoss updates the trailing stop loss for an order
func (m *TrailingStopLossMonitor) updateTrailingStopLoss(ctx context.Context, order *models.Order) error {
	// Get current market price from Indira API
	currentPrice, err := m.getCurrentPrice(ctx, order)
	if err != nil {
		return fmt.Errorf("failed to get current price: %w", err)
	}

	// Calculate new stop loss based on highest price
	var newStopLoss float64
	var shouldUpdate bool

	if order.OrderSide == models.OrderSideBuy {
		// For BUY orders, trail upwards
		// If current price is higher than entry, move stop loss up
		if currentPrice > *order.FilledPrice {
			newStopLoss = currentPrice * (1 - (*order.StopLoss / 100))

			// Only update if new stop loss is higher than current
			if order.StopLoss != nil && newStopLoss > *order.StopLoss {
				shouldUpdate = true
			} else if order.StopLoss == nil {
				shouldUpdate = true
			}
		}
	} else {
		// For SELL orders, trail downwards
		// If current price is lower than entry, move stop loss down
		if currentPrice < *order.FilledPrice {
			newStopLoss = currentPrice * (1 + (*order.StopLoss / 100))

			// Only update if new stop loss is lower than current
			if order.StopLoss != nil && newStopLoss < *order.StopLoss {
				shouldUpdate = true
			} else if order.StopLoss == nil {
				shouldUpdate = true
			}
		}
	}

	if !shouldUpdate {
		return nil
	}

	log.Printf("Updating trailing SL for order %s: %s -> %s (Current Price: %s)",
		order.OrderID,
		formatPrice(order.StopLoss),
		formatPrice(&newStopLoss),
		formatPrice(&currentPrice))

	// Update stop loss in database
	order.StopLoss = &newStopLoss
	order.UpdatedAt = time.Now()

	if err := m.orderRepo.Update(ctx, order); err != nil {
		return fmt.Errorf("failed to update order in database: %w", err)
	}

	// If a stop loss order exists, modify it
	if order.IndiraOrderID != nil {
		if err := m.modifyStopLossOrder(ctx, order, newStopLoss); err != nil {
			log.Printf("Warning: Failed to modify stop loss order: %v", err)
			// Don't return error - database is updated, modification can be retried
		}
	}

	return nil
}

// getCurrentPrice gets the current market price for an order's symbol
func (m *TrailingStopLossMonitor) getCurrentPrice(ctx context.Context, order *models.Order) (float64, error) {
	// Build auth context from order
	if order.BearerToken == nil || order.AppId == nil || order.Source == nil {
		return 0, fmt.Errorf("missing authentication data for order %s", order.OrderID)
	}

	// TODO: Implement GetQuote or similar method in Indira client to get current price
	// When implemented, use this auth context:
	// auth := &indiraClient.AuthContext{
	// 	UserId:      order.UserID,
	// 	BearerToken: *order.BearerToken,
	// 	AppId:       *order.AppId,
	// 	Source:      *order.Source,
	// }
	// positions, err := m.indiraClient.GetPositions(ctx, auth)

	// For now, return filled price as placeholder
	if order.FilledPrice != nil {
		return *order.FilledPrice, nil
	}

	return 0, fmt.Errorf("no price available for order %s", order.OrderID)
}

// modifyStopLossOrder modifies the stop loss order at the broker
func (m *TrailingStopLossMonitor) modifyStopLossOrder(ctx context.Context, order *models.Order, newStopLoss float64) error {
	// Build auth context
	if order.BearerToken == nil || order.AppId == nil || order.Source == nil {
		return fmt.Errorf("missing authentication data")
	}

	auth := &indiraClient.AuthContext{
		UserId:      order.UserID,
		BearerToken: *order.BearerToken,
		AppId:       *order.AppId,
		Source:      *order.Source,
	}

	// Update order with new stop loss
	order.StopLoss = &newStopLoss

	// Modify order via Indira API
	if err := m.indiraClient.ModifyOrder(ctx, order, auth); err != nil {
		return fmt.Errorf("failed to modify order: %w", err)
	}

	log.Printf("Successfully modified stop loss order for %s", order.OrderID)
	return nil
}

// formatPrice formats a price pointer for logging
func formatPrice(price *float64) string {
	if price == nil {
		return "N/A"
	}
	return fmt.Sprintf("%.2f", *price)
}
