package executor

import (
	"context"
	"log"
	"strings"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/paper"
)

// PaperTradeHandler handles paper trade execution and position management
type PaperTradeHandler struct {
	positionManager *paper.PositionManager
}

// NewPaperTradeHandler creates a new paper trade handler
func NewPaperTradeHandler(positionManager *paper.PositionManager) *PaperTradeHandler {
	return &PaperTradeHandler{
		positionManager: positionManager,
	}
}

// ProcessPaperOrder processes a paper trade order and creates/updates positions
func (pth *PaperTradeHandler) ProcessPaperOrder(ctx context.Context, order *models.Order) error {
	// Only handle paper orders
	if strings.ToUpper(order.TradingMode) != "PAPER" {
		return nil
	}

	// Only process filled orders
	if order.Status != models.StatusFilled {
		log.Printf("Skipping non-filled paper order: %s (Status: %s)", order.OrderID, order.Status)
		return nil
	}

	switch order.OrderSide {
	case models.OrderSideBuy:
		// Create new position for BUY orders
		if err := pth.positionManager.CreatePosition(ctx, order); err != nil {
			log.Printf("Failed to create paper position for order %s: %v", order.OrderID, err)
			return err
		}
		log.Printf("✓ Paper BUY order processed: %s (Symbol: %s, Qty: %d)",
			order.OrderID, order.Symbol, order.Quantity)

	case models.OrderSideSell:
		// SELL orders close positions - handled by position manager's SL/TP monitoring
		log.Printf("✓ Paper SELL order processed: %s (Symbol: %s, Qty: %d)",
			order.OrderID, order.Symbol, order.Quantity)
	}

	return nil
}

// ProcessExecutionResult processes execution results for paper trades
func (pth *PaperTradeHandler) ProcessExecutionResult(ctx context.Context, result *models.ExecutionResult) error {
	// This is called when a simulated execution result is published
	// We can use this to verify positions are properly tracked

	log.Printf("Paper execution result received: OrderID=%s, Status=%s, Price=%.2f",
		result.OrderID, result.Status, result.ExecutedPrice)

	return nil
}

// GetUserPositions retrieves all positions for a user
func (pth *PaperTradeHandler) GetUserPositions(ctx context.Context, userID string, includeClosed bool) ([]*models.PaperPosition, error) {
	return pth.positionManager.GetUserPositions(ctx, userID, includeClosed)
}

// GetUserPnL gets total unrealized and realized PnL for a user
func (pth *PaperTradeHandler) GetUserPnL(ctx context.Context, userID, strategyID string) (unrealizedPnL, realizedPnL float64, err error) {
	return pth.positionManager.GetUserPnL(ctx, userID, strategyID)
}

// UpdatePrices updates current prices and PnL for all user positions
func (pth *PaperTradeHandler) UpdatePrices(ctx context.Context, userID string) error {
	return pth.positionManager.UpdatePositionPrices(ctx, userID)
}
