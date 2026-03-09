package indira

import (
	"context"
	"fmt"
	"log"
	"strings"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
)

// ExecutionClient wraps Indira API for trade execution
// This client is stateless and thread-safe for concurrent use by multiple users
type ExecutionClient struct {
	client    *indiraClient.Client
	wsManager *indiraClient.WSManager
}

// NewExecutionClient creates a new execution client for Indira Securities
// The client is stateless and can handle multiple users concurrently
func NewExecutionClient() *ExecutionClient {
	client := indiraClient.NewDefaultClient()
	wsManager := indiraClient.NewWSManager(client)

	return &ExecutionClient{
		client:    client,
		wsManager: wsManager,
	}
}

// PlaceOrder places order via Indira API
// auth parameter should be provided by frontend (bearer token, appId, source)
func (c *ExecutionClient) PlaceOrder(ctx context.Context, order *models.Order, auth *indiraClient.AuthContext) (string, error) {
	// Convert internal order model to Indira API request
	orderReq, err := c.convertToIndiraRequest(order)
	if err != nil {
		return "", fmt.Errorf("failed to convert order: %w", err)
	}

	log.Printf("Placing order for user %s: Symbol=%s",auth.UserId, orderReq.Symbol)
	// Call Indira API
	resp, err := c.client.PlaceOrder(ctx, auth, orderReq)
	if err != nil {
		return "", fmt.Errorf("failed to place order: %w", err)
	}

	// Return order ID (try both fields)
	orderID := resp.OrderId
	if orderID == "" {
		orderID = resp.OrdId
	}
	if orderID == "" {
		return "", fmt.Errorf("broker accepted request but returned no order ID (message: %s)", resp.Message)
	}

	log.Printf("✓ Order placed successfully: OrderID=%s", orderID)
	return orderID, nil
}

// GetOrderStatus queries order status from Indira
func (c *ExecutionClient) GetOrderStatus(ctx context.Context, orderID string, auth *indiraClient.AuthContext) (map[string]interface{}, error) {
	// Get order trail which includes status history
	trail, err := c.client.GetOrderTrail(ctx, auth, &indiraClient.OrderTrailRequest{
		OrdId:      orderID,
		Instrument: "STK", // Default to stock, might need to be dynamic
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get order status: %w", err)
	}

	if len(trail) == 0 {
		return nil, fmt.Errorf("no order trail found for order %s", orderID)
	}

	// Return the most recent status (last item in trail)
	return trail[len(trail)-1], nil
}

// SubscribeOrderStatus starts or gets the WebSocket connection for a user and returns a channel pouring real-time order updates
// This is the fastest method to get order statuses. The framework will automatically handle reconnects and heartbeats.
func (c *ExecutionClient) SubscribeOrderStatus(ctx context.Context, auth *indiraClient.AuthContext) (<-chan *indiraClient.WSOrderStatus, error) {
	if auth == nil || auth.UserId == "" {
		return nil, fmt.Errorf("auth context and UserId required for websocket subscription")
	}

	wsClient, err := c.wsManager.GetOrCreateClient(ctx, auth)
	if err != nil {
		return nil, fmt.Errorf("failed to get websocket client: %w", err)
	}

	// wsClient.Updates is a buffered channel filled asynchronously by the background goroutines
	return wsClient.Updates, nil
}

// CancelOrder cancels an order
func (c *ExecutionClient) CancelOrder(ctx context.Context, exchange, orderID, symbol string, auth *indiraClient.AuthContext) error {
	cancelReq := &indiraClient.CancelOrderRequest{
		Exc:    exchange,
		OrdId:  orderID,
		Symbol: symbol, // Indira requires symbol for cancellation
	}

	log.Printf("Cancelling order: OrderID=%s, Exchange=%s, User=%s", orderID, exchange, auth.UserId)

	err := c.client.CancelOrder(ctx, auth, cancelReq)
	if err != nil {
		return fmt.Errorf("failed to cancel order: %w", err)
	}

	log.Printf("✓ Order cancelled successfully: OrderID=%s", orderID)
	return nil
}

// ModifyOrder modifies an order
func (c *ExecutionClient) ModifyOrder(ctx context.Context, order *models.Order, auth *indiraClient.AuthContext) error {
	// Convert to Indira modify request
	modifyReq, err := c.convertToIndiraModifyRequest(order)
	if err != nil {
		return fmt.Errorf("failed to convert modify request: %w", err)
	}

	log.Printf("Modifying order: OrderID=%s, Symbol=%s, User=%s", modifyReq.OrdId, modifyReq.Symbol, auth.UserId)

	err = c.client.ModifyOrder(ctx, auth, modifyReq)
	if err != nil {
		return fmt.Errorf("failed to modify order: %w", err)
	}

	log.Printf("✓ Order modified successfully: OrderID=%s", modifyReq.OrdId)
	return nil
}

// GetPositions retrieves current positions
func (c *ExecutionClient) GetPositions(ctx context.Context, auth *indiraClient.AuthContext) ([]indiraClient.Position, error) {
	return c.client.GetPositions(ctx, auth)
}

// GetHoldings retrieves current holdings
func (c *ExecutionClient) GetHoldings(ctx context.Context, auth *indiraClient.AuthContext) ([]indiraClient.Holding, error) {
	return c.client.GetHoldings(ctx, auth)
}

// GetOrderBook retrieves the full order book from the broker.
func (c *ExecutionClient) GetOrderBook(ctx context.Context, auth *indiraClient.AuthContext) ([]indiraClient.OrderBook, error) {
	return c.client.GetOrderBook(ctx, auth)
}

// FindRecentOrder checks the broker order book for an order matching symbol, side (BUY/SELL), and qty.
// Used for idempotency: on network timeout we don't know if the order was placed; this checks before retrying.
// Returns the broker order ID and true if a match is found.
func (c *ExecutionClient) FindRecentOrder(ctx context.Context, auth *indiraClient.AuthContext, symbol, side string, qty int) (string, bool) {
	orders, err := c.client.GetOrderBook(ctx, auth)
	if err != nil {
		log.Printf("[idempotency] order book fetch failed (cannot check duplicate): %v", err)
		return "", false
	}

	sideUpper := strings.ToUpper(side)
	symUpper := strings.ToUpper(symbol)
	for _, o := range orders {
		if !strings.EqualFold(o.OrdAction, sideUpper) || o.Qty != qty {
			continue
		}
		// Broker symbol may be "STK_TCS_EQ_NSE_11536"; our symbol is "TCS".
		oSymUpper := strings.ToUpper(o.Symbol)
		if oSymUpper == symUpper || strings.Contains(oSymUpper, symUpper) {
			log.Printf("[idempotency] Found matching order in broker book: %s (symbol=%s side=%s qty=%d)",
				o.OrdId, o.Symbol, o.OrdAction, o.Qty)
			return o.OrdId, true
		}
	}
	return "", false
}

// ============ Internal Conversion Methods ============

func (c *ExecutionClient) convertToIndiraRequest(order *models.Order) (*indiraClient.PlaceOrderRequest, error) {
	// Build Indira symbol format
	symbolBuilder := indiraClient.NewSymbolBuilder()
	symbolBuilder.Symbol = order.Symbol
	symbolBuilder.Token = fmt.Sprintf("%d", order.StockCode)
	symbolBuilder.Exchange = indiraClient.CleanExchangeName(string(order.Exchange))

	// Determine instrument and series
	symbolBuilder.Instrument = indiraClient.DetermineInstrument(symbolBuilder.Exchange, order.Symbol)
	symbolBuilder.Series = indiraClient.DetermineSeries(symbolBuilder.Exchange, symbolBuilder.Instrument)

	indiraSymbol := symbolBuilder.BuildSymbol()

	// Build order request
	req := &indiraClient.PlaceOrderRequest{
		Symbol:       indiraSymbol,
		ExcToken:     symbolBuilder.Token,
		Exc:          symbolBuilder.Exchange,
		OrdAction:    indiraClient.MapOrderSide(string(order.OrderSide)),
		OrdValidity:  indiraClient.MapValidity(order.Validity),
		OrdType:      indiraClient.MapOrderType(string(order.OrderType)),
		PrdType:      indiraClient.MapProductType(order.ProductType),
		Qty:          int(order.Quantity),
		DisQty:       0,
		LotSize:      1,
		Instrument:   symbolBuilder.Instrument,
		Amo:          false,
		LimitPrice:   0,
		TriggerPrice: 0,
	}

	isBracket := order.ProductType == "BRACKET" || order.ProductType == "BRACKET_ORDER" || order.ProductType == "BO"

	if isBracket {
		// Bracket Order (BO) price rules per Indira API spec:
		// - MARKET BO: limitPrice=0, triggerPrice=user SL price, boTgtPrice=user TP price, boStpLoss=0
		// - LIMIT BO:  limitPrice=entry limit, triggerPrice=user SL price, boTgtPrice=user TP price, boStpLoss=0
		// All prices use Price2DP so they serialize to exactly 2 decimal places → avoids EG003.
		if order.OrderType == models.OrderTypeMarket {
			req.LimitPrice = 0
		} else if order.Price != nil {
			req.LimitPrice = indiraClient.Price2DP(indiraClient.RoundToTick(*order.Price))
		}

		var zero indiraClient.Price2DP = 0.0
		req.BoStpLoss = &zero // always 0 for BO — SL is carried via triggerPrice

		if order.StopLoss != nil {
			req.TriggerPrice = indiraClient.Price2DP(indiraClient.RoundToTick(*order.StopLoss))
		}
		if order.TakeProfit != nil {
			tgt := indiraClient.Price2DP(indiraClient.RoundToTick(*order.TakeProfit))
			req.BoTgtPrice = &tgt
		} else {
			req.BoTgtPrice = &zero
		}
	} else {
		// Non-bracket orders: limitPrice for limit/SL orders; broker ignores it for Market.
		// Price2DP ensures the JSON value is "5911.30" not "5911.3", preventing EG003.
		if order.Price != nil {
			req.LimitPrice = indiraClient.Price2DP(indiraClient.RoundToTick(*order.Price))
		}
		// Trigger price for stop loss orders
		if (order.OrderType == models.OrderTypeStopLoss ||
			order.OrderType == models.OrderTypeStopLossMarket) &&
			order.StopLoss != nil {
			req.TriggerPrice = indiraClient.Price2DP(indiraClient.RoundToTick(*order.StopLoss))
		}
		var zero indiraClient.Price2DP = 0.0
		req.BoTgtPrice = &zero
		req.BoStpLoss = &zero
	}

	return req, nil
}

func (c *ExecutionClient) convertToIndiraModifyRequest(order *models.Order) (*indiraClient.ModifyOrderRequest, error) {
	// Build Indira symbol format
	symbolBuilder := indiraClient.NewSymbolBuilder()
	symbolBuilder.Symbol = order.Symbol
	symbolBuilder.Token = fmt.Sprintf("%d", order.StockCode)
	symbolBuilder.Exchange = indiraClient.CleanExchangeName(string(order.Exchange))
	symbolBuilder.Instrument = indiraClient.DetermineInstrument(symbolBuilder.Exchange, order.Symbol)
	symbolBuilder.Series = indiraClient.DetermineSeries(symbolBuilder.Exchange, symbolBuilder.Instrument)

	indiraSymbol := symbolBuilder.BuildSymbol()

	// Get order ID from IndiraOrderID field
	orderID := ""
	if order.IndiraOrderID != nil {
		orderID = *order.IndiraOrderID
	} else {
		return nil, fmt.Errorf("order ID not found for modification")
	}

	// Build modify request
	req := &indiraClient.ModifyOrderRequest{
		OrdId:         orderID,
		Symbol:        indiraSymbol,
		OrdAction:     indiraClient.MapOrderSide(string(order.OrderSide)),
		OrdValidity:   indiraClient.MapValidity(order.Validity),
		ExchangeToken: symbolBuilder.Token,
		Exc:           symbolBuilder.Exchange,
		Qty:           int(order.Quantity),
		TradedQty:     0, // Will be filled by API
		LimitPrice:    0,
		TriggerPrice:  0,
		OrdType:       indiraClient.MapOrderType(string(order.OrderType)),
		PrdType:       indiraClient.MapProductType(order.ProductType),
		Instrument:    symbolBuilder.Instrument,
		LotSize:       1,
		DisQty:        0,
		OffMktFlag:    false,
	}

	// Set price for limit orders
	if order.Price != nil {
		req.LimitPrice = *order.Price
	}

	// Set trigger price for stop loss orders
	if (order.OrderType == models.OrderTypeStopLoss ||
		order.OrderType == models.OrderTypeStopLossMarket) &&
		order.StopLoss != nil {
		req.TriggerPrice = *order.StopLoss
	}

	return req, nil
}
