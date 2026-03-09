package indira

import (
	"encoding/json"
	"fmt"
)

// StandardResponse represents the standard API response format.
//
// Indira broker endpoints commonly return:
//
//	{"infoID":"0","infoMsg":"SUCCESS","timestamp":...,"data":{...}}
//
// Some other endpoints may return:
//
//	{"success":true,"data":...}
//	{"status":"error","message":"..."}
//
// This struct is intentionally permissive and captures the union.
type StandardResponse struct {
	// Broker-style
	InfoID    string `json:"infoID,omitempty"`
	InfoMsg   string `json:"infoMsg,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`

	// Generic-style
	Success bool            `json:"success,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   *ErrorInfo      `json:"error,omitempty"`
	Message string          `json:"message,omitempty"`
	Status  string          `json:"status,omitempty"`
	// Indira business-error fields (e.g. EG003 "Price Not in multiple of PriceTick")
	InfoID  string `json:"infoID,omitempty"`
	InfoMsg string `json:"infoMsg,omitempty"`
}

// ErrorInfo represents error information
type ErrorInfo struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// BrokerBusinessError is a deterministic broker rejection (e.g. EG003).
// It signals callers NOT to retry — the request will fail identically every time.
type BrokerBusinessError struct {
	Code    string
	Message string
}

func (e *BrokerBusinessError) Error() string {
	return fmt.Sprintf("broker rejected order [%s]: %s", e.Code, e.Message)
}

// ============ Order Types ============

// PlaceOrderRequest represents a request to place an order.
// Price fields use Price2DP so they always serialize to exactly 2 decimal places,
// preventing Indira EG003 "Price Not in multiple of PriceTick" caused by float64
// representation (e.g. 5911.3 stored as 5911.2999... → broker gets 591129 paise ≠ multiple of 5).
type PlaceOrderRequest struct {
	Symbol       string    `json:"symbol"`       // e.g., "STK_TCS_EQ_NSE_11536"
	ExcToken     string    `json:"excToken"`     // Exchange token, e.g., "11536"
	Exc          string    `json:"exc"`          // Exchange, e.g., "NSE"
	OrdAction    string    `json:"ordAction"`    // "BUY" or "SELL"
	OrdValidity  string    `json:"ordValidity"`  // "DAY", "IOC"
	OrdType      string    `json:"ordType"`      // "Market", "Limit", "SL", "SL-M"
	PrdType      string    `json:"prdType"`      // "INTRADAY", "DELIVERY", "CASH"
	LimitPrice   Price2DP  `json:"limitPrice"`   // Limit price — emits "5911.30" not "5911.3"
	TriggerPrice Price2DP  `json:"triggerPrice"` // Trigger price — always 2 decimal places
	Qty          int       `json:"qty"`          // Quantity
	DisQty       int       `json:"disQty"`       // Disclosed quantity
	LotSize      int       `json:"lotSize"`      // Lot size
	Instrument   string    `json:"instrument"`   // "STK", "OPT", "FUT", "IDX"
	Amo          bool      `json:"amo"`          // After Market Order
	BoStpLoss    *Price2DP `json:"boStpLoss"`    // Bracket order stop loss
	BoTgtPrice   *Price2DP `json:"boTgtPrice"`   // Bracket order target price
}

// PlaceOrderResponse represents the response from placing an order
type PlaceOrderResponse struct {
	OrderId string `json:"orderId,omitempty"`
	OrdId   string `json:"ordId,omitempty"`
	Message string `json:"message,omitempty"`
	Status  string `json:"status,omitempty"`

	// Broker-style fields (populated when available)
	OrdStatus string `json:"ordStatus,omitempty"`
	RejReason string `json:"rejReason,omitempty"`
}

// ModifyOrderRequest represents a request to modify an order
type ModifyOrderRequest struct {
	OrdId         string  `json:"ordId"`         // Order ID
	Symbol        string  `json:"symbol"`        // Symbol
	OrdAction     string  `json:"ordAction"`     // "BUY" or "SELL"
	OrdValidity   string  `json:"ordValidity"`   // "DAY", "IOC"
	ExchangeToken string  `json:"exchangeToken"` // Exchange token
	Exc           string  `json:"exc"`           // Exchange
	Qty           int     `json:"qty"`           // New quantity
	TradedQty     int     `json:"tradedQty"`     // Already traded quantity
	LimitPrice    float64 `json:"limitPrice"`    // New limit price
	TriggerPrice  float64 `json:"triggerPrice"`  // New trigger price
	OrdType       string  `json:"ordType"`       // "Market", "Limit", "SL", "SL-M"
	PrdType       string  `json:"prdType"`       // Product type
	Instrument    string  `json:"instrument"`    // Instrument type
	LotSize       int     `json:"lotSize"`       // Lot size
	DisQty        int     `json:"disQty"`        // Disclosed quantity
	OffMktFlag    bool    `json:"offMktFlag"`    // Off market flag
}

// CancelOrderRequest represents a request to cancel an order
type CancelOrderRequest struct {
	Symbol string `json:"symbol"` // Symbol
	Exc    string `json:"exc"`    // Exchange
	OrdId  string `json:"ordId"`  // Order ID
}

// BrokerageChargesRequest represents a request to calculate brokerage charges
type BrokerageChargesRequest struct {
	Symbol       string `json:"symbol"`       // Symbol
	ExcToken     string `json:"excToken"`     // Exchange token
	PrdType      string `json:"prdType"`      // Product type
	Instrument   string `json:"instrument"`   // Instrument type
	Exc          string `json:"exc"`          // Exchange
	OrderAction  string `json:"orderAction"`  // "BUY" or "SELL"
	Qty          string `json:"qty"`          // Quantity (string)
	Price        string `json:"price"`        // Price (string)
	TriggerPrice string `json:"triggerPrice"` // Trigger price (string)
}

// ============ Portfolio Types ============

// OrderBook represents an order in the order book
type OrderBook struct {
	OrdId        string  `json:"ordId,omitempty"`
	Symbol       string  `json:"symbol,omitempty"`
	Exc          string  `json:"exc,omitempty"`
	OrdAction    string  `json:"ordAction,omitempty"`
	Qty          int     `json:"qty,omitempty"`
	TradedQty    int     `json:"tradedQty,omitempty"`
	LimitPrice   float64 `json:"limitPrice,omitempty"`
	TriggerPrice float64 `json:"triggerPrice,omitempty"`
	OrdType      string  `json:"ordType,omitempty"`
	PrdType      string  `json:"prdType,omitempty"`
	Status       string  `json:"status,omitempty"`
	OrdTime      string  `json:"ordTime,omitempty"`
	ExcToken     string  `json:"excToken,omitempty"`
	Instrument   string  `json:"instrument,omitempty"`
}

// OrderTrailRequest represents a request to get order trail
type OrderTrailRequest struct {
	OrdId      string `json:"ordId"`      // Order ID
	Instrument string `json:"instrument"` // Instrument type
}

// TradeBook represents a trade in the trade book
type TradeBook struct {
	TradeId    string  `json:"tradeId,omitempty"`
	OrdId      string  `json:"ordId,omitempty"`
	Symbol     string  `json:"symbol,omitempty"`
	Exc        string  `json:"exc,omitempty"`
	OrdAction  string  `json:"ordAction,omitempty"`
	Qty        int     `json:"qty,omitempty"`
	Price      float64 `json:"price,omitempty"`
	TradeTime  string  `json:"tradeTime,omitempty"`
	PrdType    string  `json:"prdType,omitempty"`
	ExcToken   string  `json:"excToken,omitempty"`
	Instrument string  `json:"instrument,omitempty"`
}

// Holding represents a holding in the portfolio
type Holding struct {
	Symbol         string  `json:"symbol,omitempty"`
	Exc            string  `json:"exc,omitempty"`
	ExcToken       string  `json:"excToken,omitempty"`
	Qty            int     `json:"qty,omitempty"`
	AvgPrice       float64 `json:"avgPrice,omitempty"`
	CurrentPrice   float64 `json:"currentPrice,omitempty"`
	PnL            float64 `json:"pnl,omitempty"`
	PnLPercentage  float64 `json:"pnlPercentage,omitempty"`
	Instrument     string  `json:"instrument,omitempty"`
	ISIN           string  `json:"isin,omitempty"`
	T1Qty          int     `json:"t1Qty,omitempty"`
	CollateralQty  int     `json:"collateralQty,omitempty"`
	CollateralType string  `json:"collateralType,omitempty"`
}

// Position represents a position in the position book
type Position struct {
	Symbol        string  `json:"symbol,omitempty"`
	Exc           string  `json:"exc,omitempty"`
	ExcToken      string  `json:"excToken,omitempty"`
	PrdType       string  `json:"prdType,omitempty"`
	NetQty        int     `json:"netQty,omitempty"`
	BuyQty        int     `json:"buyQty,omitempty"`
	SellQty       int     `json:"sellQty,omitempty"`
	BuyAvgPrice   float64 `json:"buyAvgPrice,omitempty"`
	SellAvgPrice  float64 `json:"sellAvgPrice,omitempty"`
	CurrentPrice  float64 `json:"currentPrice,omitempty"`
	PnL           float64 `json:"pnl,omitempty"`
	PnLPercentage float64 `json:"pnlPercentage,omitempty"`
	Instrument    string  `json:"instrument,omitempty"`
	DayBuyQty     int     `json:"dayBuyQty,omitempty"`
	DaySellQty    int     `json:"daySellQty,omitempty"`
	CFBuyQty      int     `json:"cfBuyQty,omitempty"`
	CFSellQty     int     `json:"cfSellQty,omitempty"`
}

// ConvertPositionRequest represents a request to convert position
type ConvertPositionRequest struct {
	OrdAction  string `json:"ordAction"`  // "BUY" or "SELL"
	PrdType    string `json:"prdType"`    // Current product type
	ToPrdType  string `json:"toPrdType"`  // Target product type
	Qty        int    `json:"qty"`        // Quantity to convert
	Symbol     string `json:"symbol"`     // Symbol
	ExcToken   string `json:"excToken"`   // Exchange token
	Exc        string `json:"exc"`        // Exchange
	LotSize    int    `json:"lotSize"`    // Lot size
	Instrument string `json:"instrument"` // Instrument type
	Type       string `json:"type"`       // "DAY1", "NET"
}

// ============ User & Account Types ============

// AccountInfo represents user account information
type AccountInfo struct {
	UserId    string   `json:"userId,omitempty"`
	UserName  string   `json:"userName,omitempty"`
	Email     string   `json:"email,omitempty"`
	Mobile    string   `json:"mobile,omitempty"`
	Pan       string   `json:"pan,omitempty"`
	ClientId  string   `json:"clientId,omitempty"`
	Exchanges []string `json:"exchanges,omitempty"`
	Products  []string `json:"products,omitempty"`
}

// FundLimit represents fund limit information
type FundLimit struct {
	AvailableCash   float64 `json:"availableCash,omitempty"`
	AvailableMargin float64 `json:"availableMargin,omitempty"`
	UsedMargin      float64 `json:"usedMargin,omitempty"`
	Collateral      float64 `json:"collateral,omitempty"`
	PayinAmount     float64 `json:"payinAmount,omitempty"`
	PayoutAmount    float64 `json:"payoutAmount,omitempty"`
	TotalBalance    float64 `json:"totalBalance,omitempty"`
}
