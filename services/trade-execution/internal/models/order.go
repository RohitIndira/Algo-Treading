package models

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// OrderStatus represents order lifecycle status
type OrderStatus string

const (
	StatusReceived        OrderStatus = "RECEIVED"
	StatusValidated       OrderStatus = "VALIDATED"
	StatusPending         OrderStatus = "PENDING"
	StatusSubmitted       OrderStatus = "SUBMITTED"
	StatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	StatusFilled          OrderStatus = "FILLED"
	StatusRejected        OrderStatus = "REJECTED"
	StatusCancelled       OrderStatus = "CANCELLED"
	StatusFailed          OrderStatus = "FAILED"
)

// IsFilledStatus returns true if the status represents a filled order
// (includes both internal and broker status strings).
func IsFilledStatus(s OrderStatus) bool {
	switch strings.ToUpper(string(s)) {
	case "FILLED", "EXECUTED", "TRADED":
		return true
	}
	return false
}

// IsPartiallyFilledStatus returns true if the status represents a partially filled order.
func IsPartiallyFilledStatus(s OrderStatus) bool {
	switch strings.ToUpper(string(s)) {
	case "PARTIALLY_FILLED", "PARTIALLY TRADED", "PARTIALLY EXECUTED":
		return true
	}
	return false
}

// IsTerminalStatus returns true if the order is in a terminal state
// and cannot be modified or cancelled.
func IsTerminalStatus(s OrderStatus) bool {
	switch strings.ToUpper(string(s)) {
	case "FILLED", "EXECUTED", "TRADED", "CANCELLED", "REJECTED", "A.REJECTED", "FAILED", "EXPIRED":
		return true
	}
	return false
}

// OrderType represents order type
type OrderType string

const (
	OrderTypeMarket         OrderType = "MARKET"
	OrderTypeLimit          OrderType = "LIMIT"
	OrderTypeStopLoss       OrderType = "STOP_LOSS"
	OrderTypeStopLossMarket OrderType = "SL-M" // Stop Loss Market
)

// OrderSide represents buy or sell
type OrderSide string

const (
	OrderSideBuy  OrderSide = "BUY"
	OrderSideSell OrderSide = "SELL"
)

// Exchange represents stock exchange
type Exchange string

const (
	ExchangeNSE Exchange = "NSE"
	ExchangeBSE Exchange = "BSE"
)

// Order represents a trading order
type Order struct {
	OrderID      uuid.UUID `json:"order_id" db:"order_id"`
	UserID       string    `json:"user_id" db:"user_id"`
	StrategyID   string    `json:"strategy_id" db:"strategy_id"`
	StrategyName string    `json:"strategy_name" db:"strategy_name"`
	EventID      uuid.UUID `json:"event_id" db:"event_id"`

	// Stock information
	StockCode int64    `json:"stock_code" db:"stock_code"`
	Exchange  Exchange `json:"exchange" db:"exchange"`
	Symbol    string   `json:"symbol" db:"symbol"`

	// Order details
	OrderType OrderType `json:"order_type" db:"order_type"`
	OrderSide OrderSide `json:"order_side" db:"order_side"`
	Quantity  int32     `json:"quantity" db:"quantity"`
	Price     *float64  `json:"price,omitempty" db:"price"`

	// Stop loss and take profit
	StopLoss   *float64 `json:"stop_loss,omitempty" db:"stop_loss"`
	TakeProfit *float64 `json:"take_profit,omitempty" db:"take_profit"`

	// Order validity
	Validity    string `json:"validity" db:"validity"`
	ProductType string `json:"product_type" db:"product_type"` // INTRADAY, DELIVERY, CASH

	// Additional order parameters
	TargetPrice *float64 `json:"target_price,omitempty" db:"target_price"` // For bracket orders

	// Order status
	Status OrderStatus `json:"status" db:"status"`

	// Broker API integration (Indira Securities / Odin)
	IndiraOrderID  *string `json:"indira_order_id,omitempty" db:"indira_order_id"` // Indira API order ID
	IndiraResponse *string `json:"indira_response,omitempty" db:"indira_response"` // Indira API response
	OdinOrderID    *string `json:"odin_order_id,omitempty" db:"odin_order_id"`     // Odin API order ID
	OdinResponse   *string `json:"odin_response,omitempty" db:"odin_response"`     // Odin API response

	// Raw broker WebSocket data — stored exactly as received, no mapping/transformation
	BrokerStatus       *string `json:"broker_status,omitempty" db:"broker_status"`               // Raw status from broker WS (e.g. "EXECUTED", "PENDING", "CANCELLED")
	BrokerWSData       *string `json:"broker_ws_data,omitempty" db:"broker_ws_data"`              // Full raw JSON from broker WS
	ExchangeOrderNumber *string `json:"exchange_order_number,omitempty" db:"exchange_order_number"` // Exchange order number (OrderNumber from WS)

	// Frontend auth data (passed from frontend for Indira API calls)
	BearerToken *string `json:"bearer_token,omitempty" db:"bearer_token"` // JWT bearer token from frontend
	AppId       *string `json:"app_id,omitempty" db:"app_id"`             // Application ID from frontend
	Source      *string `json:"source,omitempty" db:"source"`             // Source platform (IOS, AND, WEB)

	// Stop loss configuration
	StopLossType  *string  `json:"stop_loss_type,omitempty" db:"stop_loss_type"`   // FIXED or TRAILING
	TrailingSLPct *float64 `json:"trailing_sl_pct,omitempty" db:"trailing_sl_pct"` // Trailing SL percentage
	HighestPrice  *float64 `json:"highest_price,omitempty" db:"highest_price"`     // Highest price for trailing SL

	// Auto square-off flag
	IsSquareOffOrder bool `json:"is_square_off_order" db:"is_square_off_order"` // Flag for auto square-off orders

	// AutoSquareOffTime is a per-user override for when all positions are closed
	// (format "HH:MM" IST, e.g. "14:30"). Applies to both paper and live modes.
	// NULL / empty means "use the global default" (15:05 live, 15:00 paper).
	AutoSquareOffTime *string `json:"auto_square_off_time,omitempty" db:"auto_square_off_time"`

	// Paper trading fields
	IsPaperTrade    bool       `json:"is_paper_trade" db:"is_paper_trade"`               // True for paper trade orders
	TradingMode     string     `json:"trading_mode" db:"trading_mode"`                    // PAPER or LIVE
	PaperExitPrice  *float64   `json:"paper_exit_price,omitempty" db:"paper_exit_price"` // Exit price for paper positions
	PaperPnL        *float64   `json:"paper_pnl,omitempty" db:"paper_pnl"`               // Final P&L for paper positions
	// PaperExitTime is the exact timestamp when the paper position was closed.
	// entry time is ExecutedAt (set when the order fills).
	PaperExitTime   *time.Time `json:"paper_exit_time,omitempty" db:"paper_exit_time"`

	// Live trading exit fields
	LiveExitPrice *float64   `json:"live_exit_price,omitempty" db:"live_exit_price"` // Exit price for live positions (force-exit)
	LivePnL       *float64   `json:"live_pnl,omitempty" db:"live_pnl"`               // Final P&L for live positions (force-exit)
	// LiveExitTime is the exact timestamp when the live position was closed.
	// Entry time is ExecutedAt (set from broker OrderEntryTime/OrderTimeStamp on fill).
	LiveExitTime  *time.Time `json:"live_exit_time,omitempty" db:"live_exit_time"`

	// CurrentPctChange is the stock's percentage change at the time the order was created.
	CurrentPctChange float64 `json:"current_pct_change,omitempty" db:"current_pct_change"`

	// MaxMonitorPrice is the price ceiling for the PriceMonitor.
	// If LTP exceeds this level, the monitor must NOT trigger the order.
	// 0 means no upper bound.
	MaxMonitorPrice *float64 `json:"max_monitor_price,omitempty" db:"max_monitor_price"`

	// OCO (One-Cancels-the-Other) group tracking
	OCOGroupID    *uuid.UUID `json:"oco_group_id,omitempty" db:"oco_group_id"`       // Links all orders in one OCO group
	OCORole       *string    `json:"oco_role,omitempty" db:"oco_role"`               // ENTRY, SL_LEG, or TP_LEG
	ParentOrderID *uuid.UUID `json:"parent_order_id,omitempty" db:"parent_order_id"` // Child legs point to entry order

	// Signal deduplication
	SignalID *uuid.UUID `json:"signal_id,omitempty" db:"signal_id"` // Originating trade signal UUID (for idempotency)

	// Execution details
	FilledQuantity int32    `json:"filled_quantity" db:"filled_quantity"`
	FilledPrice    *float64 `json:"filled_price,omitempty" db:"filled_price"`
	Commission     *float64 `json:"commission,omitempty" db:"commission"`
	TotalCost      *float64 `json:"total_cost,omitempty" db:"total_cost"`

	// Risk approval
	RiskApproved bool     `json:"risk_approved" db:"risk_approved"`
	RiskScore    *float64 `json:"risk_score,omitempty" db:"risk_score"`

	// Timestamps
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at" db:"updated_at"`
	SubmittedAt *time.Time `json:"submitted_at,omitempty" db:"submitted_at"`
	ExecutedAt  *time.Time `json:"executed_at,omitempty" db:"executed_at"`

	// Error handling
	ErrorMessage    *string `json:"error_message,omitempty" db:"error_message"`
	RejectionReason *string `json:"rejection_reason,omitempty" db:"rejection_reason"`
	RetryCount      int32   `json:"retry_count" db:"retry_count"`
}

// OrderRequest from RabbitMQ
type OrderRequest struct {
	RequestID    string   `json:"request_id"`
	UserID       string   `json:"user_id"`
	StrategyID   string   `json:"strategy_id"`
	StrategyName string   `json:"strategy_name"`
	EventID      string   `json:"event_id"`
	StockCode  int64    `json:"stock_code"`
	Exchange   string   `json:"exchange"`
	Symbol     string   `json:"symbol"`
	OrderType  string   `json:"order_type"`
	OrderSide  string   `json:"order_side"`
	Quantity   int32    `json:"quantity"`
	Price      *float64 `json:"price,omitempty"`
	StopLoss   *float64 `json:"stop_loss,omitempty"`
	TakeProfit *float64 `json:"take_profit,omitempty"`
	Validity   string   `json:"validity"`

	RiskApproved bool      `json:"risk_approved"`
	RiskScore    *float64  `json:"risk_score,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
	RetryCount   int32     `json:"retry_count"`

	// Frontend authentication data (from user credentials/strategy)
	BearerToken string `json:"bearer_token"` // JWT bearer token
	AppId       string `json:"app_id"`       // Application ID
	Source      string `json:"source"`       // Source platform (IOS, AND, WEB)

	// Additional fields from strategy
	StopLossType  string  `json:"stop_loss_type"`  // "FIXED" or "TRAILING"
	TrailingSLPct float64 `json:"trailing_sl_pct"` // Trailing stop loss percentage
	ProductType   string  `json:"product_type"`    // INTRADAY, DELIVERY, CASH

	// Trading mode from strategy
	TradingMode string `json:"trading_mode"` // PAPER or LIVE
}

// ExecutionEvent represents an order execution event
type ExecutionEvent struct {
	ID        int64                  `json:"id" db:"id"`
	OrderID   uuid.UUID              `json:"order_id" db:"order_id"`
	EventType string                 `json:"event_type" db:"event_type"`
	EventData map[string]interface{} `json:"event_data" db:"event_data"`
	CreatedAt time.Time              `json:"created_at" db:"created_at"`
}
