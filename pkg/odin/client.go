package odin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client represents an Odin API client
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// Config holds client configuration
type Config struct {
	BaseURL string
	Timeout time.Duration
}

// NewClient creates a new Odin API client
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// NewClientWithConfig creates a client with custom configuration
func NewClientWithConfig(config Config) *Client {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &Client{
		baseURL: config.BaseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// ============ Request/Response Types ============

// LoginRequest represents login credentials
type LoginRequest struct {
	UserID     string `json:"user_id"`
	Password   string `json:"password"`
	TOTPSecret string `json:"totp_secret"`
}

// LoginResponse represents login response
type LoginResponse struct {
	Success bool                   `json:"success"`
	Data    map[string]interface{} `json:"data"`
	Message string                 `json:"message"`
}

// StandardResponse represents standard API response
type StandardResponse struct {
	Success bool                   `json:"success"`
	Data    interface{}            `json:"data,omitempty"`
	Error   map[string]interface{} `json:"error,omitempty"`
	Message string                 `json:"message"`
}

// ScripInfo represents security information
type ScripInfo struct {
	Exchange    string `json:"exchange"`
	ScripToken  int    `json:"scrip_token"`
	Symbol      string `json:"symbol"`
	Series      string `json:"series"`
	ExpiryDate  string `json:"expiry_date,omitempty"`
	StrikePrice string `json:"strike_price,omitempty"`
	OptionType  string `json:"option_type,omitempty"`
}

// OrderRequest represents an order placement request
type OrderRequest struct {
	ScripInfo         ScripInfo `json:"scrip_info"`
	TransactionType   string    `json:"transaction_type"`
	ProductType       string    `json:"product_type"`
	OrderType         string    `json:"order_type"`
	Quantity          int       `json:"quantity"`
	Price             float64   `json:"price"`
	TriggerPrice      float64   `json:"trigger_price"`
	DisclosedQuantity int       `json:"disclosed_quantity"`
	Validity          string    `json:"validity"`
	ValidityDays      int       `json:"validity_days"`
	IsAMO             bool      `json:"is_amo"`
	OrderIdentifier   string    `json:"order_identifier"`
	PartCode          string    `json:"part_code"`
	AlgoID            string    `json:"algo_id"`
	StrategyID        string    `json:"strategy_id"`
	VenderCode        string    `json:"vender_code"`
}

// ModifyOrderRequest represents order modification request
type ModifyOrderRequest struct {
	Exchange          string   `json:"exchange"`
	OrderID           string   `json:"order_id"`
	Quantity          *int     `json:"quantity,omitempty"`
	Price             *float64 `json:"price,omitempty"`
	TriggerPrice      *float64 `json:"trigger_price,omitempty"`
	OrderType         *string  `json:"order_type,omitempty"`
	DisclosedQuantity *int     `json:"disclosed_quantity,omitempty"`
}

// CancelOrderRequest represents order cancellation request
type CancelOrderRequest struct {
	Exchange string `json:"exchange"`
	OrderID  string `json:"order_id"`
}

// Balance represents account balance
type Balance struct {
	AvailableBalance float64 `json:"available_balance"`
	UsedMargin       float64 `json:"used_margin"`
	Collateral       float64 `json:"collateral"`
}

// Position represents a trading position
type Position struct {
	Exchange        string  `json:"exchange"`
	Symbol          string  `json:"symbol"`
	Token           int     `json:"token"`
	ProductType     string  `json:"product_type"`
	Quantity        int     `json:"quantity"`
	AveragePrice    float64 `json:"average_price"`
	CurrentPrice    float64 `json:"current_price"`
	PnL             float64 `json:"pnl"`
	PnLPercentage   float64 `json:"pnl_percentage"`
	DayBuyQuantity  int     `json:"day_buy_quantity"`
	DaySellQuantity int     `json:"day_sell_quantity"`
	CFBuyQuantity   int     `json:"cf_buy_quantity"`
	CFSellQuantity  int     `json:"cf_sell_quantity"`
	NetQuantity     int     `json:"net_quantity"`
}

// Holding represents a delivery holding
type Holding struct {
	ISIN               string  `json:"isin"`
	Symbol             string  `json:"symbol"`
	Exchange           string  `json:"exchange"`
	Quantity           int     `json:"quantity"`
	AveragePrice       float64 `json:"average_price"`
	CurrentPrice       float64 `json:"current_price"`
	PnL                float64 `json:"pnl"`
	PnLPercentage      float64 `json:"pnl_percentage"`
	CollateralQuantity int     `json:"collateral_quantity"`
	CollateralType     string  `json:"collateral_type"`
	T1Quantity         int     `json:"t1_quantity"`
	Haircut            float64 `json:"haircut"`
}

// Order represents an order
type Order struct {
	OrderID         string  `json:"order_id"`
	Exchange        string  `json:"exchange"`
	Symbol          string  `json:"symbol"`
	TransactionType string  `json:"transaction_type"`
	Quantity        int     `json:"quantity"`
	Price           float64 `json:"price"`
	FilledQuantity  int     `json:"filled_quantity"`
	Status          string  `json:"status"`
	OrderTime       string  `json:"order_time"`
	ProductType     string  `json:"product_type"`
}

// ============ Helper Methods ============

func (c *Client) doRequest(ctx context.Context, method, path, userID string, body interface{}) (*StandardResponse, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if userID != "" {
		req.Header.Set("X-User-ID", userID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var stdResp StandardResponse
	if err := json.Unmarshal(responseBody, &stdResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !stdResp.Success {
		return &stdResp, fmt.Errorf("API error: %s", stdResp.Message)
	}

	return &stdResp, nil
}

// ============ Authentication Methods ============

// Login authenticates a user
func (c *Client) Login(ctx context.Context, req *LoginRequest) error {
	resp, err := c.doRequest(ctx, "POST", "/auth/login", "", req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("login failed: %s", resp.Message)
	}

	return nil
}

// GetBalance retrieves account balance
func (c *Client) GetBalance(ctx context.Context, userID string) (*Balance, error) {
	resp, err := c.doRequest(ctx, "GET", "/auth/balance", userID, nil)
	if err != nil {
		return nil, err
	}

	var balance Balance
	dataBytes, err := json.Marshal(resp.Data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(dataBytes, &balance); err != nil {
		return nil, err
	}

	return &balance, nil
}

// ValidateSession checks if session is valid
func (c *Client) ValidateSession(ctx context.Context, userID string) error {
	_, err := c.doRequest(ctx, "PUT", "/auth/session/validate", userID, nil)
	return err
}

// Logout logs out a user
func (c *Client) Logout(ctx context.Context, userID string) error {
	_, err := c.doRequest(ctx, "DELETE", "/auth/logout", userID, nil)
	return err
}

// ============ Order Management Methods ============

// PlaceOrder places a new order
func (c *Client) PlaceOrder(ctx context.Context, userID string, req *OrderRequest) (string, error) {
	resp, err := c.doRequest(ctx, "POST", "/orders/place", userID, req)
	if err != nil {
		return "", err
	}

	orderID, ok := resp.Data.(string)
	if !ok {
		return "", fmt.Errorf("invalid order ID in response")
	}

	return orderID, nil
}

// ModifyOrder modifies an existing order
func (c *Client) ModifyOrder(ctx context.Context, userID string, req *ModifyOrderRequest) error {
	_, err := c.doRequest(ctx, "PUT", "/orders/modify", userID, req)
	return err
}

// CancelOrder cancels an order
func (c *Client) CancelOrder(ctx context.Context, userID string, req *CancelOrderRequest) error {
	_, err := c.doRequest(ctx, "DELETE", "/orders/cancel", userID, req)
	return err
}

// GetOrderBook retrieves order book
func (c *Client) GetOrderBook(ctx context.Context, userID string, offset, limit int, orderStatus string) ([]Order, error) {
	path := fmt.Sprintf("/orders/book?offset=%d&limit=%d", offset, limit)
	if orderStatus != "" {
		path += fmt.Sprintf("&order_status=%s", orderStatus)
	}

	resp, err := c.doRequest(ctx, "GET", path, userID, nil)
	if err != nil {
		return nil, err
	}

	var orders []Order
	dataBytes, err := json.Marshal(resp.Data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(dataBytes, &orders); err != nil {
		return nil, err
	}

	return orders, nil
}

// GetTradeBook retrieves trade book
func (c *Client) GetTradeBook(ctx context.Context, userID string, offset, limit int) ([]Order, error) {
	path := fmt.Sprintf("/orders/trades?offset=%d&limit=%d", offset, limit)

	resp, err := c.doRequest(ctx, "GET", path, userID, nil)
	if err != nil {
		return nil, err
	}

	var trades []Order
	dataBytes, err := json.Marshal(resp.Data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(dataBytes, &trades); err != nil {
		return nil, err
	}

	return trades, nil
}

// GetOrderHistory retrieves order history
func (c *Client) GetOrderHistory(ctx context.Context, userID, orderID string) ([]map[string]interface{}, error) {
	path := fmt.Sprintf("/orders/%s/history", orderID)

	resp, err := c.doRequest(ctx, "GET", path, userID, nil)
	if err != nil {
		return nil, err
	}

	var history []map[string]interface{}
	dataBytes, err := json.Marshal(resp.Data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(dataBytes, &history); err != nil {
		return nil, err
	}

	return history, nil
}

// ============ Portfolio Management Methods ============

// GetPositions retrieves trading positions
func (c *Client) GetPositions(ctx context.Context, userID, positionType string) ([]Position, error) {
	path := fmt.Sprintf("/portfolio/positions?position_type=%s", positionType)

	resp, err := c.doRequest(ctx, "GET", path, userID, nil)
	if err != nil {
		return nil, err
	}

	var positions []Position
	dataBytes, err := json.Marshal(resp.Data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(dataBytes, &positions); err != nil {
		return nil, err
	}

	return positions, nil
}

// GetHoldings retrieves delivery holdings
func (c *Client) GetHoldings(ctx context.Context, userID string) ([]Holding, error) {
	resp, err := c.doRequest(ctx, "GET", "/portfolio/holdings", userID, nil)
	if err != nil {
		return nil, err
	}

	var holdings []Holding
	dataBytes, err := json.Marshal(resp.Data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(dataBytes, &holdings); err != nil {
		return nil, err
	}

	return holdings, nil
}

// ConvertPosition converts position type
func (c *Client) ConvertPosition(ctx context.Context, userID string, exchange string, token int, fromProduct, toProduct string, quantity int, transactionType string) error {
	path := fmt.Sprintf("/portfolio/positions/convert?exchange=%s&token=%d&from_product=%s&to_product=%s&quantity=%d&transaction_type=%s",
		exchange, token, fromProduct, toProduct, quantity, transactionType)

	_, err := c.doRequest(ctx, "PUT", path, userID, nil)
	return err
}

// ============ Health Check ============

// HealthCheck checks service health
func (c *Client) HealthCheck(ctx context.Context) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/health", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var health map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return nil, err
	}

	return health, nil
}
