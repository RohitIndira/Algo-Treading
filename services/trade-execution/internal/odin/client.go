package odin

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/RohitIndira/Algo-Treading/pkg/odin"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
)

// ExecutionClient wraps Odin API for trade execution
type ExecutionClient struct {
	client       *odin.Client
	loggedInUser string
	loginMutex   sync.Mutex
}

// NewExecutionClient creates a new execution client
func NewExecutionClient(baseURL string) *ExecutionClient {
	return &ExecutionClient{
		client: odin.NewClient(baseURL),
	}
}

// ensureLoginWithCredentials ensures user is logged in with provided credentials
func (c *ExecutionClient) ensureLoginWithCredentials(ctx context.Context, odinUserID, password, totpSecret string) error {
	c.loginMutex.Lock()
	defer c.loginMutex.Unlock()

	// If already logged in for this user, skip
	if c.loggedInUser == odinUserID {
		return nil
	}

	// Validate credentials
	if odinUserID == "" || password == "" || totpSecret == "" {
		return fmt.Errorf("missing Odin credentials for user")
	}

	// Login
	loginReq := &odin.LoginRequest{
		UserID:     odinUserID,
		Password:   password,
		TOTPSecret: totpSecret,
	}

	log.Printf("Logging in user %s to Odin API...", odinUserID)
	err := c.client.Login(ctx, loginReq)
	if err != nil {
		return fmt.Errorf("failed to login: %w", err)
	}

	c.loggedInUser = odinUserID
	log.Printf("✓ Successfully logged in user %s", odinUserID)
	return nil
}

// PlaceOrderWithCredentials places order via Odin API using provided credentials
func (c *ExecutionClient) PlaceOrderWithCredentials(ctx context.Context, order *models.Order, odinUserID, password, totpSecret string) (string, error) {
	// Ensure user is logged in first with their credentials
	if err := c.ensureLoginWithCredentials(ctx, odinUserID, password, totpSecret); err != nil {
		return "", fmt.Errorf("login failed: %w", err)
	}

	// Convert internal order model to Odin API request
	orderReq := c.convertToOdinRequest(order)
	log.Printf("Placing order for user %s: %+v", odinUserID, orderReq)

	// Call Odin API with the logged-in user's ID
	orderID, err := c.client.PlaceOrder(ctx, odinUserID, &orderReq)
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
	// Convert exchange format: "EXCHANGE_NSE" -> "NSE_EQ", "NSE" -> "NSE_EQ"
	exchange := c.formatExchange(string(order.Exchange))

	req := odin.OrderRequest{
		ScripInfo: odin.ScripInfo{
			Exchange:   exchange,
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
		TriggerPrice:      0, // Default to 0
	}

	// Set price for limit orders
	if order.Price != nil {
		req.Price = *order.Price
	}

	// Only set trigger price for stop loss orders (SL/SL-MKT)
	if order.OrderType == models.OrderTypeStopLoss && order.StopLoss != nil {
		req.TriggerPrice = *order.StopLoss
	}

	return req
}

func (c *ExecutionClient) formatExchange(exchange string) string {
	// Handle various exchange formats and convert to broker format
	// "EXCHANGE_NSE" -> "NSE_EQ"
	// "NSE" -> "NSE_EQ"
	// "EXCHANGE_BSE" -> "BSE_EQ"
	// "BSE" -> "BSE_EQ"

	// Strip "EXCHANGE_" prefix if present
	if len(exchange) > 9 && exchange[:9] == "EXCHANGE_" {
		exchange = exchange[9:]
	}

	// Append "_EQ" for equity segment
	return exchange + "_EQ"
}

func (c *ExecutionClient) mapOrderType(orderType models.OrderType) string {
	switch orderType {
	case models.OrderTypeMarket:
		return "RL-MKT" // Regular market order
	case models.OrderTypeLimit:
		return "RL" // Regular limit order
	case models.OrderTypeStopLoss:
		return "SL-MKT" // Stop loss market order
	default:
		return "RL-MKT" // Default to market order
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
