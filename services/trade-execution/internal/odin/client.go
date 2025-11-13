package odin

import (
	"context"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/pkg/odin"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
)

// ExecutionClient wraps Odin API for trade execution
type ExecutionClient struct {
	client *odin.Client
}

// NewExecutionClient creates a new execution client
func NewExecutionClient(baseURL string) *ExecutionClient {
	return &ExecutionClient{
		client: odin.NewClient(baseURL),
	}
}

// PlaceOrder places order via Odin API
func (c *ExecutionClient) PlaceOrder(ctx context.Context, order *models.Order, userID string) (string, error) {
	// Convert internal order model to Odin API request
	orderReq := c.convertToOdinRequest(order)

	// Call Odin API
	orderID, err := c.client.PlaceOrder(ctx, userID, &orderReq)
	if err != nil {
		return "", fmt.Errorf("failed to place order: %w", err)
	}

	return orderID, nil
}

// GetOrderStatus queries order status from Odin
func (c *ExecutionClient) GetOrderStatus(ctx context.Context, exchange, orderID, userID string) ([]map[string]interface{}, error) {
	// Use GetOrderHistory to get order details
	return c.client.GetOrderHistory(ctx, userID, orderID)
}

// CancelOrder cancels an order
func (c *ExecutionClient) CancelOrder(ctx context.Context, exchange, orderID, userID string) error {
	cancelReq := odin.CancelOrderRequest{
		Exchange: exchange,
		OrderID:  orderID,
	}

	return c.client.CancelOrder(ctx, userID, &cancelReq)
}

// ModifyOrder modifies an order
func (c *ExecutionClient) ModifyOrder(ctx context.Context, modifyReq odin.ModifyOrderRequest, userID string) error {
	return c.client.ModifyOrder(ctx, userID, &modifyReq)
}

func (c *ExecutionClient) convertToOdinRequest(order *models.Order) odin.OrderRequest {
	req := odin.OrderRequest{
		ScripInfo: odin.ScripInfo{
			Exchange:   string(order.Exchange),
			ScripToken: int(order.StockCode),
			Symbol:     order.Symbol,
			Series:     "EQ", // Default to equity
		},
		TransactionType:   c.mapOrderSide(order.OrderSide),
		ProductType:       "INTRADAY", // Default to intraday
		OrderType:         c.mapOrderType(order.OrderType),
		Quantity:          int(order.Quantity),
		Validity:          order.Validity,
		DisclosedQuantity: 0,
		IsAMO:             false,
	}

	if order.Price != nil {
		req.Price = *order.Price
	}

	if order.StopLoss != nil {
		req.TriggerPrice = *order.StopLoss
	}

	return req
}

func (c *ExecutionClient) mapOrderType(orderType models.OrderType) string {
	switch orderType {
	case models.OrderTypeMarket:
		return "MKT"
	case models.OrderTypeLimit:
		return "RL"
	case models.OrderTypeStopLoss:
		return "SL"
	default:
		return "MKT"
	}
}

func (c *ExecutionClient) mapOrderSide(orderSide models.OrderSide) string {
	switch orderSide {
	case models.OrderSideBuy:
		return "BUY"
	case models.OrderSideSell:
		return "SELL"
	default:
		return "BUY"
	}
}
