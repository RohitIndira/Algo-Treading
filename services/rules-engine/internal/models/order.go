package models

import (
	"time"

	"github.com/google/uuid"
)

// OrderRequest represents an order request to be published to RabbitMQ
type OrderRequest struct {
	OrderID      string    `json:"order_id"`
	UserID       string    `json:"user_id"`
	StrategyID   string    `json:"strategy_id"`
	StrategyName string    `json:"strategy_name"`
	EventID      string    `json:"event_id"`
	StockCode    int64     `json:"stock_code"`
	Token        int64     `json:"token"` // Trading token (same as stock_code for compatibility)
	Symbol       string    `json:"symbol"`
	Exchange     string    `json:"exchange"`
	OrderType    string    `json:"order_type"` // MARKET, LIMIT
	Quantity     int32     `json:"quantity"`
	Price        float64   `json:"price"`
	StopLoss     float64   `json:"stop_loss"`
	TakeProfit   float64   `json:"take_profit"`
	Timestamp    time.Time `json:"timestamp"`
	MatchScore   float64   `json:"match_score"`
	ImpactScore  int32     `json:"impact_score"`
	Sentiment    string    `json:"sentiment"`
	NewsCategory string    `json:"news_category"`
	RiskApproved bool      `json:"risk_approved"` // Temporary: Set to true until risk-management integration
	RiskScore    float64   `json:"risk_score"`
	RetryCount   int       `json:"retry_count"`
	OrderSide    string    `json:"order_side"` // BUY, SELL
	Validity     string    `json:"validity"`   // DAY, IOC, etc.

	// Frontend authentication data (from user credentials)
	BearerToken string `json:"bearer_token"` // JWT bearer token
	AppId       string `json:"app_id"`       // Application ID
	Source      string `json:"source"`       // Source platform (IOS, AND, WEB)

	// Stop loss configuration
	StopLossType  string  `json:"stop_loss_type"`  // "FIXED" or "TRAILING"
	TrailingSLPct float64 `json:"trailing_sl_pct"` // Trailing stop loss percentage

	// Product type
	ProductType string `json:"product_type"` // INTRADAY, DELIVERY, CASH

	// Trading mode
	TradingMode string `json:"trading_mode"` // PAPER or LIVE

	// PctChangeStatus records why this order was generated as MARKET vs LIMIT.
	// "within_range" → immediate MARKET order (price change already in strategy range).
	// "below_min"    → pending LIMIT order; Price is the computed target entry price.
	// ""             → no pct-change filter was active (treated as within_range).
	PctChangeStatus string `json:"pct_change_status"`

	// CurrentPctChange is the stock's percentage change at the time the order was created.
	CurrentPctChange float64 `json:"current_pct_change,omitempty"`

	// MaxMonitorPrice is the price level corresponding to max_pct_change.
	// The PriceMonitor must NOT trigger the order if LTP exceeds this level.
	// 0 means no upper bound.
	MaxMonitorPrice float64 `json:"max_monitor_price,omitempty"`
}

// RuleMatch represents a successful rule match
type RuleMatch struct {
	UserID            string    `json:"user_id"`
	StrategyID        string    `json:"strategy_id"`
	StrategyName      string    `json:"strategy_name"`
	Strategy          *Strategy `json:"strategy"` // Full strategy including trade_config from Kafka
	MatchScore        float64   `json:"match_score"`
	MatchedConditions []string  `json:"matched_conditions"`
	FailedConditions  []string  `json:"failed_conditions"`
	ApprovedByRisk    bool      `json:"approved_by_risk"`
	OrderRequestID    string    `json:"order_request_id"`
	Timestamp         time.Time `json:"timestamp"`
	EventID           string    `json:"event_id"`
	// PctChangeStatus is forwarded from EvaluationResult so the handler can
	// distinguish an immediate MARKET order (within_range / "") from a pending
	// LIMIT order (below_min).
	PctChangeStatus string `json:"pct_change_status"`
}

// NewOrderRequest creates a new order request from a match and event
func NewOrderRequest(match *RuleMatch, event *MarketEvent, strategy *Strategy) *OrderRequest {
	orderID := uuid.New().String()

	// Calculate stop loss and take profit
	price := event.MarketData.LastTradedPrice
	stopLoss := price * (1 - strategy.TradeConfig.StopLossPct/100)
	takeProfit := price * (1 + strategy.TradeConfig.TakeProfitPct/100)

	// Default product type to INTRADAY if not specified.
	// When OrderType is BRACKET, ensure ProductType is also BRACKET so the
	// downstream convertToIndiraRequest correctly builds bracket order legs.
	productType := strategy.TradeConfig.ProductType
	if strategy.TradeConfig.OrderType == "BRACKET" {
		productType = "BRACKET"
	} else if productType == "" {
		productType = "INTRADAY"
	}

	// Default stop loss type to FIXED if not specified
	stopLossType := strategy.TradeConfig.StopLossType
	if stopLossType == "" {
		stopLossType = "FIXED"
	}

	return &OrderRequest{
		OrderID:      orderID,
		UserID:       strategy.UserID,
		StrategyID:   strategy.StrategyID,
		StrategyName: strategy.StrategyName,
		EventID:      event.EventID,
		StockCode:    event.StockData.StockCode,
		Token:        event.StockData.StockCode, // Token is same as stock_code
		Symbol:       event.StockData.Symbol,
		Exchange:     event.StockData.Exchange,
		OrderType:    strategy.TradeConfig.OrderType,
		OrderSide:    "BUY", // Default to BUY
		Quantity:     strategy.TradeConfig.Quantity,
		Price:        price,
		StopLoss:     stopLoss,
		TakeProfit:   takeProfit,
		Validity:     "DAY", // Default to DAY order
		Timestamp:    time.Now(),
		MatchScore:   match.MatchScore,
		ImpactScore:  event.Analysis.ImpactScore,
		Sentiment:    event.Analysis.Sentiment,
		NewsCategory: event.NewsData.Category,
		RiskApproved: false, // Will be set by risk management service
		RiskScore:    0.0,   // Will be set by risk management service
		RetryCount:   0,

		// Authentication data from strategy
		BearerToken: strategy.BearerToken,
		AppId:       strategy.AppId,
		Source:      strategy.Source,

		// Stop loss configuration
		StopLossType:  stopLossType,
		TrailingSLPct: strategy.TradeConfig.TrailingSLPct,

		// Product type
		ProductType: productType,

		// Trading mode
		TradingMode: strategy.TradingMode,
	}
}

// Validate validates an order request
func (o *OrderRequest) Validate() error {
	if o.OrderID == "" {
		return ErrInvalidOrderID
	}
	if o.UserID == "" {
		return ErrInvalidUserID
	}
	if o.StrategyID == "" {
		return ErrInvalidStrategyID
	}
	if o.StockCode <= 0 {
		return ErrInvalidStockCode
	}
	if o.Exchange == "" {
		return ErrInvalidExchange
	}
	if o.Quantity <= 0 {
		return ErrInvalidQuantity
	}
	if o.Price <= 0 {
		return ErrInvalidPrice
	}
	if o.OrderType != "MARKET" && o.OrderType != "LIMIT" && o.OrderType != "BRACKET" && o.OrderType != "STOP_LOSS" {
		return ErrInvalidOrderType
	}
	return nil
}
