package indira

import (
	"encoding/json"
	"fmt"
)

// StandardResponse represents the standard API response format
type StandardResponse struct {
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
	LimitPrice   Price2DP  `json:"limitPrice"`             // Limit price — emits "5911.30" not "5911.3"
	TriggerPrice Price2DP  `json:"triggerPrice,omitempty"` // Trigger price — omitted when 0 (Limit/Market orders); broker rejects triggerPrice:0 on non-SL orders
	Qty          int       `json:"qty"`          // Quantity
	DisQty       int       `json:"disQty"`       // Disclosed quantity
	LotSize      int       `json:"lotSize"`      // Lot size
	Instrument   string    `json:"instrument"`   // "STK", "OPT", "FUT", "IDX"
	Amo          bool      `json:"amo"`          // After Market Order
	BoStpLoss    *Price2DP `json:"boStpLoss,omitempty"`    // Bracket order stop loss
	BoTgtPrice   *Price2DP `json:"boTgtPrice,omitempty"`   // Bracket order target price
}

// PlaceOrderResponse represents the response from placing an order
type PlaceOrderResponse struct {
	OrderId   string `json:"orderId,omitempty"`
	OrdId     string `json:"ordId,omitempty"`
	OrdStatus string `json:"ordStatus,omitempty"` // "Executed", "Pending", "Rejected"
	RejReason string `json:"rejReason,omitempty"` // rejection reason if rejected
	Message   string `json:"message,omitempty"`
	Status    string `json:"status,omitempty"`
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
// OrderBookSymbol is the nested symbol-descriptor object that Indira returns
// inside every order-book entry. Matches the schema in Indira OPEN API doc v1.0
// (pages 13-14). Most fields are nullable or irrelevant for order-book use;
// kept here so unmarshal doesn't fail on unexpected fields and the Symbol +
// ExcTkn + ISIN can be read downstream if needed.
type OrderBookSymbol struct {
	Symbol        string  `json:"symbol,omitempty"`        // e.g. "STK_KINGFA_EQ_NSE_18944"
	DispSym       string  `json:"dispSym,omitempty"`       // e.g. "KINGFA"
	Instrument    string  `json:"instrument,omitempty"`    // STK/IDX/FUT/OPT
	BaseSym       string  `json:"baseSym,omitempty"`       // e.g. "KINGFA"
	CompanyName   string  `json:"companyName,omitempty"`
	ISIN          string  `json:"isin,omitempty"`
	Exc           string  `json:"exc,omitempty"`           // NSE/BSE/NFO/BFO/MCX/NCDEX/CDS/BCD
	ExcTkn        int     `json:"excTkn,omitempty"`        // numeric exchange token
	Series        string  `json:"series,omitempty"`        // EQ/BE/SM
	TickSize      float64 `json:"tickSize,omitempty"`
	StreamSym     string  `json:"streamSym,omitempty"`
	Multiplier    string  `json:"multiplier,omitempty"`
	TradingSymbol string  `json:"tradingSymbol,omitempty"`
	FNO           bool    `json:"fno,omitempty"`
	MTF           bool    `json:"mtf,omitempty"`
	// Many other nullable fields (lotSize, expiryDate, strikePrice, optionType,
	// segment, freezeQty, otherExc, isWeeklyExpiry) omitted — add if needed.
}

// OrderBook mirrors one entry in the Indira order-book response
// (GET /portfolio-services/api/order/v1/order-book → data.orders[]).
// Field names match the API doc exactly — notably `price` (not `limitPrice`)
// for the limit price, and `symbol` as a nested object.
type OrderBook struct {
	OrdId        string          `json:"ordId,omitempty"`
	ExchOrdId    int64           `json:"exchOrdId,omitempty"`
	Symbol       OrderBookSymbol `json:"symbol,omitempty"`
	Status       string          `json:"status,omitempty"`       // Requested/Executed/Cancelled/Rejected/Pending
	OrdAction    string          `json:"ordAction,omitempty"`    // BUY/SELL
	OrdType      string          `json:"ordType,omitempty"`      // Market/Limit/SL-M/Stop-loss
	PrdType      string          `json:"prdType,omitempty"`      // DELIVERY/INTRADAY
	OrdValidity  string          `json:"ordValidity,omitempty"`  // NONE/DAY/GTD/IOC/GTC
	ModifiedBy   string          `json:"modifiedBy,omitempty"`   // MOBILE-Ex/WEB/API/DESKTOP/SYSTEM
	Price        float64         `json:"price,omitempty"`        // LIMIT price (NOT limitPrice — API doc names it `price`)
	TriggerPrice float64         `json:"triggerPrice,omitempty"` // SL trigger
	RejReason    string          `json:"rejReason,omitempty"`
	OrdDate      string          `json:"ordDate,omitempty"`   // dd-MMM-yyyy hh24:mm:ss
	ExcOrdTime   string          `json:"excOrdTime,omitempty"`
	Qty          int             `json:"qty,omitempty"`
	DisQty       int             `json:"disQty,omitempty"`
	TradedQty    int             `json:"tradedQty,omitempty"`
	RemainQty    int             `json:"remainQty,omitempty"`
	CancelledQty int             `json:"cancelledQty,omitempty"`
	UndAsset     string          `json:"undAsset,omitempty"`
	AMO          bool            `json:"amo,omitempty"`
	Modifiable   bool            `json:"modifiable,omitempty"`
	Cancellable  bool            `json:"cancellable,omitempty"`

	// Legacy field kept for back-compat. `limitPrice` isn't in the doc's
	// order-book response — some older callers may still read it. Populated
	// from Price in a UnmarshalJSON helper if you want; for now unused by the
	// reconciler.
	LimitPrice float64 `json:"limitPrice,omitempty"`
}

// OrderTrailRequest represents a request to get order trail
type OrderTrailRequest struct {
	OrdId      string `json:"ordId"`      // Order ID
	Instrument string `json:"instrument"` // Instrument type
}

// TradeBook represents a trade in the trade book.
// Field names match the Indira broker API response (data.trades[]).
type TradeBook struct {
	OrdId      string      `json:"ordId,omitempty"`
	ExchTrdId  interface{} `json:"exchTrdId,omitempty"`
	ExchOrdId  string      `json:"exchOrdId,omitempty"`
	PrdType    string      `json:"prdType,omitempty"`
	OrdAction  string      `json:"ordAction,omitempty"`
	OrdType    string      `json:"ordType,omitempty"`
	// TradedPrice is the actual exchange fill price (authoritative).
	TradedPrice float64 `json:"tradedPrice,omitempty"`
	TradeTime   string  `json:"tradeTime,omitempty"` // e.g. "2026-03-30 11:57:08"
	TradedQty   int     `json:"tradedQty,omitempty"`
	RemainQty   int     `json:"remainQty,omitempty"`
	Qty         int     `json:"qty,omitempty"`
	Symbol      interface{} `json:"symbol,omitempty"` // complex object from broker
	UndAsset    string  `json:"undAsset,omitempty"`
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
// PositionSymbol is the nested "symbol" object inside each Indira position.
type PositionSymbol struct {
	Symbol      string `json:"symbol,omitempty"`      // e.g. "STK_MOTHERSON_EQ_NSE_4204"
	DispSym     string `json:"dispSym,omitempty"`     // e.g. "MOTHERSON"
	BaseSym     string `json:"baseSym,omitempty"`     // e.g. "MOTHERSON"
	Instrument  string `json:"instrument,omitempty"`  // e.g. "STK"
	Exc         string `json:"exc,omitempty"`         // e.g. "NSE"
	ExcTkn      int    `json:"excTkn,omitempty"`      // e.g. 4204
	TradingSym  string `json:"tradingSymbol,omitempty"` // e.g. "MOTHERSON-EQ"
	Series      string `json:"series,omitempty"`
	TickSize    string `json:"tickSize,omitempty"`
	Multiplier  string `json:"multiplier,omitempty"`
	CompanyName string `json:"companyName,omitempty"`
	ISIN        string `json:"isin,omitempty"`
	Pdc         string `json:"pdc,omitempty"` // previous day close
}

type Position struct {
	Symbol        PositionSymbol `json:"symbol"`
	PrdType       string         `json:"prdType,omitempty"`
	Type          string         `json:"type,omitempty"` // "DAILY" or "EXPIRY"
	NetQty        int            `json:"netQty"`
	BuyQty        int            `json:"buyQty"`
	SellQty       int            `json:"sellQty"`
	BuyAvgPrice   float64        `json:"buyAvgPrice"`
	SellAvgPrice  float64        `json:"sellAvgPrice"`
	LTP           float64        `json:"ltp"`           // last traded price
	NetPnL        float64        `json:"netPnl"`        // net P&L
	PnLPerc       float64        `json:"pnlPerc"`       // P&L percentage
	AvgPrice      float64        `json:"avgPrice"`
	BuyAmt        float64        `json:"buyAmt"`
	SellAmt       float64        `json:"sellAmt"`
	OrdAction     string         `json:"ordAction,omitempty"` // "BUY" or "SELL"
	DayBuyQty     int            `json:"dayBuyQty"`
	DaySellQty    int            `json:"daySellQty"`
	CFBuyQty      int            `json:"cfBuyQty"`
	CFSellQty     int            `json:"cfSellQty"`
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
