package indira

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
)

// TickSizeLookup retrieves the tick size for an instrument from Redis market data.
type TickSizeLookup interface {
	GetTickSize(ctx context.Context, exchange string, token int64) (float64, error)
}

// DPRLookup retrieves the Daily Price Range (circuit limits) from Redis market data.
type DPRLookup interface {
	// GetDPR returns (lower, upper) DPR bounds.
	GetDPR(ctx context.Context, exchange string, token int64) (lower float64, upper float64, err error)
}

// LTPLookup retrieves the last-traded price for an instrument from Redis market data.
type LTPLookup interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

// ExecutionClient wraps Indira API for trade execution
// This client is stateless and thread-safe for concurrent use by multiple users
type ExecutionClient struct {
	client    *indiraClient.Client
	wsManager *indiraClient.WSManager

	// tickSizeLookup fetches per-instrument tick size from Redis market data.
	// Nil-safe: falls back to hardcoded NSE defaults if unavailable.
	tickSizeLookup TickSizeLookup

	// dprLookup fetches Daily Price Range (circuit limits) from Redis.
	// Nil-safe: if unavailable, no DPR clamping is applied.
	dprLookup DPRLookup

	// ltpLookup fetches current LTP from Redis.
	// Used to set BUY limitPrice to LTP+0.5% for faster fills.
	ltpLookup LTPLookup
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

// SetTickSizeLookup sets the tick size lookup (e.g. RedisPriceClient).
func (c *ExecutionClient) SetTickSizeLookup(tsl TickSizeLookup) {
	c.tickSizeLookup = tsl
}

// SetDPRLookup sets the DPR lookup (e.g. RedisPriceClient).
func (c *ExecutionClient) SetDPRLookup(dpr DPRLookup) {
	c.dprLookup = dpr
}

// SetLTPLookup sets the LTP lookup (e.g. RedisPriceClient).
func (c *ExecutionClient) SetLTPLookup(ltp LTPLookup) {
	c.ltpLookup = ltp
}

// resolveTickSize fetches the tick size for an instrument from Redis.
// Returns 0 on any error (caller falls back to hardcoded defaults).
func (c *ExecutionClient) resolveTickSize(exchange string, stockCode int64) float64 {
	if c.tickSizeLookup == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ts, err := c.tickSizeLookup.GetTickSize(ctx, exchange, stockCode)
	if err != nil {
		log.Printf("[tick-size] Redis lookup failed for %s:%d, using default: %v", exchange, stockCode, err)
		return 0
	}
	return ts
}

// resolveDPR fetches the Daily Price Range for an instrument from Redis.
// Returns (0, 0) on any error (caller skips DPR clamping).
func (c *ExecutionClient) resolveDPR(exchange string, stockCode int64) (lower, upper float64) {
	if c.dprLookup == nil {
		return 0, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	lo, hi, err := c.dprLookup.GetDPR(ctx, exchange, stockCode)
	if err != nil {
		log.Printf("[dpr] Redis lookup failed for %s:%d, skipping DPR clamp: %v", exchange, stockCode, err)
		return 0, 0
	}
	return lo, hi
}

// resolveLTP fetches the last-traded price for an instrument from Redis.
// Returns 0 on any error (caller falls back to signal price).
func (c *ExecutionClient) resolveLTP(exchange string, stockCode int64) (float64, error) {
	if c.ltpLookup == nil {
		return 0, fmt.Errorf("ltp lookup not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	return c.ltpLookup.GetLTP(ctx, exchange, stockCode)
}

// clampDPR clamps a price to within the Daily Price Range [lower, upper].
// If lower and upper are both 0 (unavailable), the price is returned unchanged.
func clampDPR(price, lower, upper float64) float64 {
	if lower <= 0 && upper <= 0 {
		return price
	}
	if lower > 0 && price < lower {
		return lower
	}
	if upper > 0 && price > upper {
		return upper
	}
	return price
}

// roundPrice rounds a price using the instrument's tick size from Redis,
// falling back to hardcoded NSE defaults if unavailable.
func (c *ExecutionClient) roundPrice(price float64, tickSize float64) float64 {
	return indiraClient.RoundToTickSize(price, tickSize)
}

// PlaceOrder places order via Indira API
// auth parameter should be provided by frontend (bearer token, appId, source)
func (c *ExecutionClient) PlaceOrder(ctx context.Context, order *models.Order, auth *indiraClient.AuthContext) (string, error) {
	// Convert internal order model to Indira API request
	orderReq, err := c.convertToIndiraRequest(order)
	if err != nil {
		return "", fmt.Errorf("failed to convert order: %w", err)
	}

	if reqJSON, jsonErr := json.Marshal(orderReq); jsonErr == nil {
		log.Printf("Placing order for user %s: Symbol=%s payload=%s", auth.UserId, orderReq.Symbol, string(reqJSON))
	} else {
		log.Printf("Placing order for user %s: Symbol=%s", auth.UserId, orderReq.Symbol)
	}
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

	log.Printf("✓ Order submitted to broker: OrderID=%s (awaiting WS confirmation)", orderID)
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

// NewUserWSClient creates a fresh, unconnected WebSocket client bound to a
// single user's auth context. The caller (statusservice) owns the returned
// client's full lifecycle: wiring callbacks, calling Connect, and Close.
//
// One connection per user — the broker binds a WebSocket to a single
// authenticated user, so order statuses for multiple users cannot share a
// connection. statusservice keeps one of these per subscribed user.
func (c *ExecutionClient) NewUserWSClient(auth *indiraClient.AuthContext) *indiraClient.WSClient {
	return indiraClient.NewWSClient(c.client, auth)
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

// GetTradeBook retrieves trade-book entries for the given broker order IDs. The
// trade book is the authoritative source for fill qty, price and — critically —
// fill TIME: the order WebSocket omits TradedQTY and every timestamp on EXECUTED
// messages, so exit/entry times can only be sourced reliably from here.
func (c *ExecutionClient) GetTradeBook(ctx context.Context, auth *indiraClient.AuthContext, orderIds ...string) ([]indiraClient.TradeBook, error) {
	return c.client.GetTradeBook(ctx, auth, orderIds...)
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

	// Resolve tick size and DPR concurrently — these are independent Redis lookups.
	var tickSize float64
	var dprLower, dprUpper float64
	var lookupWg sync.WaitGroup
	lookupWg.Add(2)
	go func() {
		defer lookupWg.Done()
		tickSize = c.resolveTickSize(symbolBuilder.Exchange, order.StockCode)
	}()
	go func() {
		defer lookupWg.Done()
		dprLower, dprUpper = c.resolveDPR(symbolBuilder.Exchange, order.StockCode)
	}()
	lookupWg.Wait()

	round := func(price float64) float64 {
		return c.roundPrice(price, tickSize)
	}
	clamp := func(price float64) float64 {
		return clampDPR(price, dprLower, dprUpper)
	}
	// Convenience: round then clamp to DPR range.
	roundClamp := func(price float64) float64 {
		return clamp(round(price))
	}

	if dprLower > 0 && dprUpper > 0 {
		log.Printf("[dpr] %s:%d DPR range [%.2f, %.2f]", symbolBuilder.Exchange, order.StockCode, dprLower, dprUpper)
	}

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
		// ── Immediate Bracket Order (Case 1: within_range / MARKET) ─────────
		if order.OrderType == models.OrderTypeMarket {
			req.LimitPrice = 0
		} else if order.Price != nil {
			req.LimitPrice = indiraClient.Price2DP(roundClamp(*order.Price))
		}

		var zero indiraClient.Price2DP = 0.0
		req.BoStpLoss = &zero // always 0 for BO — SL is carried via triggerPrice

		if order.StopLoss != nil {
			req.TriggerPrice = indiraClient.Price2DP(roundClamp(*order.StopLoss))
		}
		if order.TakeProfit != nil {
			tgt := indiraClient.Price2DP(roundClamp(*order.TakeProfit))
			req.BoTgtPrice = &tgt
		} else {
			req.BoTgtPrice = &zero
		}
	} else {
		// Non-bracket orders.
		// convertSignalToOrder always wraps zero-values as &0.0 (never nil), so use > 0
		// to distinguish "configured" from "not set".
		//
		// BUY orders (any type): place as Limit at LTP+0.5% for immediate fill.
		//   - MARKET BUY → converted to Limit with limitPrice = LTP*1.005
		//   - SL BUY (trigger>0) → trigger kept, limitPrice = LTP*1.005
		//   - LTP unavailable → SL BUY falls back to trigger*1.005; plain BUY uses signal price
		// SELL orders: signal prices used unchanged.
		hasTrigger := order.StopLoss != nil && *order.StopLoss > 0
		hasPrice := order.Price != nil && *order.Price > 0

		if hasTrigger {
			req.TriggerPrice = indiraClient.Price2DP(roundClamp(*order.StopLoss))
			if req.OrdType != "SL-M" {
				req.OrdType = "SL"
			}
		}

		if order.OrderSide == models.OrderSideBuy {
			ltp, ltpErr := c.resolveLTP(symbolBuilder.Exchange, order.StockCode)
			if ltpErr == nil && ltp > 0 {
				limitPrice := roundClamp(ltp * 1.005)
				req.LimitPrice = indiraClient.Price2DP(limitPrice)
				if !hasTrigger {
					req.OrdType = "Limit"
				}
				log.Printf("[ltp-limit] BUY %s: limitPrice=%.2f (LTP=%.2f +0.5%%)", order.Symbol, limitPrice, ltp)
			} else if hasTrigger {
				limitPrice := roundClamp(*order.StopLoss * 1.005)
				req.LimitPrice = indiraClient.Price2DP(limitPrice)
				log.Printf("[sl-limit] BUY %s: trigger=%.2f limitPrice=%.2f (+0.5%%, LTP unavailable)", order.Symbol, *order.StopLoss, limitPrice)
			} else if hasPrice {
				req.LimitPrice = indiraClient.Price2DP(roundClamp(*order.Price))
			}
		} else if hasPrice {
			req.LimitPrice = indiraClient.Price2DP(roundClamp(*order.Price))
		}
		// Do NOT set BoStpLoss / BoTgtPrice for plain (non-bracket) orders.
		// The broker interprets any order carrying these fields (even as 0) as a
		// native bracket/OCO order and requires ordType "RL" or "RL-MKT" for the
		// main leg (EG003). Our OCO is custom-managed, so the entry is a plain
		// SL/Limit order — no bracket fields should be sent.
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

	// Resolve tick size and DPR concurrently — independent Redis lookups.
	var tickSize float64
	var dprLower, dprUpper float64
	var modLookupWg sync.WaitGroup
	modLookupWg.Add(2)
	go func() {
		defer modLookupWg.Done()
		tickSize = c.resolveTickSize(symbolBuilder.Exchange, order.StockCode)
	}()
	go func() {
		defer modLookupWg.Done()
		dprLower, dprUpper = c.resolveDPR(symbolBuilder.Exchange, order.StockCode)
	}()
	modLookupWg.Wait()

	round := func(price float64) float64 {
		return c.roundPrice(price, tickSize)
	}
	roundClamp := func(price float64) float64 {
		return clampDPR(round(price), dprLower, dprUpper)
	}

	// Set price for limit orders
	if order.Price != nil {
		req.LimitPrice = roundClamp(*order.Price)
	}

	// Set trigger price for stop loss orders
	if (order.OrderType == models.OrderTypeStopLoss ||
		order.OrderType == models.OrderTypeStopLossMarket) &&
		order.StopLoss != nil {
		req.TriggerPrice = roundClamp(*order.StopLoss)
	}

	return req, nil
}
