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

	// Stop loss / take profit configuration
	StopLossType   string  `json:"stop_loss_type"`   // "FIXED" | "TRAILING" | "MULTI_LEVEL"
	TakeProfitType string  `json:"take_profit_type"` // "FIXED" | "MULTI_LEVEL"
	StopLossPct    float64 `json:"stop_loss_pct"`    // Original SL percentage from strategy
	TakeProfitPct  float64 `json:"take_profit_pct"`  // Original TP percentage from strategy
	TrailingSLPct  float64 `json:"trailing_sl_pct"`  // Trailing stop loss percentage

	// Multi-level exit levels (non-nil only when respective type == "MULTI_LEVEL")
	MultiLevelSL []MultiLevelExitLevel `json:"multi_level_sl,omitempty"`
	MultiLevelTP []MultiLevelExitLevel `json:"multi_level_tp,omitempty"`

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

	// AutoSquareOffTime is the user-configured time (HH:MM IST) at which all
	// open positions for this user should be automatically closed.
	// Empty string = use the global default in trade-execution (15:05 IST live, 15:00 paper).
	AutoSquareOffTime string `json:"auto_square_off_time,omitempty"`

	// SignalSource identifies the pipeline that generated this order.
	// "BACKFILL_AMN" for after-market news backfill orders; empty for live signals.
	SignalSource string `json:"signal_source,omitempty"`

	// MaxTradesPerStrategy is the strategy's daily trade cap, forwarded so the
	// trade-execution price monitor can enforce the same hard ceiling when a
	// below_min watch finally triggers (a monitored watch only becomes a real
	// trade at that point). 0 = no limit. See pkg/tradecap.
	MaxTradesPerStrategy int32 `json:"max_trades_per_strategy,omitempty"`
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

// NewOrderRequest creates a new order request template from a match and event.
//
// Price, StopLoss, and TakeProfit are set to zero here. The handler is
// responsible for resolving the final entry price (with tick-size rounding
// from Redis) and computing SL/TP from the exact buying price. This ensures:
//   - SL/TP always match the user's configured percentages exactly (modulo tick rounding)
//   - All rounding uses the actual tick_size from the exchange feed, not a hardcoded value
//   - The entry price accounts for spread buffers and case-specific adjustments
func NewOrderRequest(match *RuleMatch, event *MarketEvent, strategy *Strategy) *OrderRequest {
	orderID := uuid.New().String()

	// Resolve product type based on OrderType and StopLossType:
	//   - Fixed SL + BRACKET OrderType  → BRACKET (broker-native bracket order)
	//   - Trailing SL                   → INTRADAY (custom OCO manages SL/TP legs)
	//   - Everything else               → user-configured or INTRADAY default
	productType := strategy.TradeConfig.ProductType
	if strategy.TradeConfig.OrderType == "BRACKET" {
		if strategy.TradeConfig.StopLossType == "TRAILING" {
			productType = "INTRADAY"
		} else {
			productType = "BRACKET"
		}
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
		Price:        0, // Resolved by handler with tick-size rounding
		StopLoss:     0, // Computed by handler from final entry price
		TakeProfit:   0, // Computed by handler from final entry price
		Validity:     "DAY",
		Timestamp:    time.Now(),
		MatchScore:   match.MatchScore,
		ImpactScore:  event.Analysis.ImpactScore,
		Sentiment:    event.Analysis.Sentiment,
		NewsCategory: event.NewsData.Category,
		RiskApproved: false,
		RiskScore:    0.0,
		RetryCount:   0,

		// Authentication data from strategy
		BearerToken: strategy.BearerToken,
		AppId:       strategy.AppId,
		Source:      strategy.Source,

		// Stop loss / take profit configuration
		StopLossType:   stopLossType,
		TakeProfitType: strategy.TradeConfig.TakeProfitType,
		StopLossPct:    strategy.TradeConfig.StopLossPct,
		TakeProfitPct:  strategy.TradeConfig.TakeProfitPct,
		TrailingSLPct:  strategy.TradeConfig.TrailingSLPct,
		MultiLevelSL:   strategy.TradeConfig.MultiLevelSL,
		MultiLevelTP:   strategy.TradeConfig.MultiLevelTP,

		// Product type
		ProductType: productType,

		// Trading mode
		TradingMode: strategy.TradingMode,

		// Per-user auto square-off override from strategy risk limits.
		// Only forwarded when the user has explicitly enabled it.
		AutoSquareOffTime: func() string {
			if strategy.RiskLimits.EnableAutoSquareOff {
				return strategy.RiskLimits.AutoSquareOffTime
			}
			return ""
		}(),

		// Daily trade cap, carried so trade-execution can enforce it at watch-trigger time.
		MaxTradesPerStrategy: strategy.RiskLimits.MaxTradesPerStrategy,
	}
}

// Signal kinds recorded on trade_signals.signal_kind.
const (
	// SignalKindImmediate is a real trade placed now (within_range / default).
	SignalKindImmediate = "IMMEDIATE"
	// SignalKindMonitoring is a below_min watch parked in the price monitor —
	// not a trade (and not counted against the cap) until its target triggers.
	SignalKindMonitoring = "MONITORING"
)

// SignalKind classifies this order for the durable trade counter. A below_min
// order is a price-monitor watch; everything else is an immediate trade.
func (o *OrderRequest) SignalKind() string {
	if o.PctChangeStatus == "below_min" {
		return SignalKindMonitoring
	}
	return SignalKindImmediate
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
