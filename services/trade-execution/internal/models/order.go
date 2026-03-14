package models

import (
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
	OrderID    uuid.UUID `json:"order_id" db:"order_id"`
	UserID     string    `json:"user_id" db:"user_id"`
	StrategyID string    `json:"strategy_id" db:"strategy_id"`
	EventID    uuid.UUID `json:"event_id" db:"event_id"`

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

	// Paper trading fields
	IsPaperTrade    bool     `json:"is_paper_trade" db:"is_paper_trade"`               // True for paper trade orders
	TradingMode     string   `json:"trading_mode" db:"trading_mode"`                    // PAPER or LIVE
	PaperExitPrice  *float64 `json:"paper_exit_price,omitempty" db:"paper_exit_price"` // Exit price for paper positions
	PaperPnL        *float64 `json:"paper_pnl,omitempty" db:"paper_pnl"`               // Final P&L for paper positions

	// Live trading exit fields
	LiveExitPrice *float64 `json:"live_exit_price,omitempty" db:"live_exit_price"` // Exit price for live positions (force-exit)
	LivePnL       *float64 `json:"live_pnl,omitempty" db:"live_pnl"`               // Final P&L for live positions (force-exit)

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
	RequestID  string   `json:"request_id"`
	UserID     string   `json:"user_id"`
	StrategyID string   `json:"strategy_id"`
	EventID    string   `json:"event_id"`
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
