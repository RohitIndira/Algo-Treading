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
	OrderTypeMarket   OrderType = "MARKET"
	OrderTypeLimit    OrderType = "LIMIT"
	OrderTypeStopLoss OrderType = "STOP_LOSS"
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
	StockCode int64 `json:"stock_code" db:"stock_code"`
	// Token is the actual trading token used by Odin. For most flows this is
	// identical to StockCode, but we keep it separate so we can evolve the
	// schema without touching the DB. Not persisted (db:"-").
	Token    int64    `json:"token,omitempty" db:"-"`
	Exchange Exchange `json:"exchange" db:"exchange"`
	Symbol   string   `json:"symbol" db:"symbol"`

	// Order details
	OrderType OrderType `json:"order_type" db:"order_type"`
	OrderSide OrderSide `json:"order_side" db:"order_side"`
	Quantity  int32     `json:"quantity" db:"quantity"`
	Price     *float64  `json:"price,omitempty" db:"price"`

	// Stop loss and take profit
	StopLoss   *float64 `json:"stop_loss,omitempty" db:"stop_loss"`
	TakeProfit *float64 `json:"take_profit,omitempty" db:"take_profit"`

	// Order validity
	Validity string `json:"validity" db:"validity"`

	// Order status
	Status OrderStatus `json:"status" db:"status"`

	// Odin API integration
	OdinOrderID  *string `json:"odin_order_id,omitempty" db:"odin_order_id"`
	OdinResponse *string `json:"odin_response,omitempty" db:"odin_response"`

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
	RequestID  string `json:"request_id"`
	UserID     string `json:"user_id"`
	StrategyID string `json:"strategy_id"`
	EventID    string `json:"event_id"`
	StockCode  int64  `json:"stock_code"`
	// Token mirrors the rules-engine OrderRequest.Token field and carries the
	// exchange-specific trading token (scrip token) used by Odin. This allows
	// us to avoid e-101 "Scrip details not found" by sending the correct
	// scrip_token instead of 0.
	Token      int64    `json:"token"`
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
}

// ExecutionEvent represents an order execution event
type ExecutionEvent struct {
	ID        int64                  `json:"id" db:"id"`
	OrderID   uuid.UUID              `json:"order_id" db:"order_id"`
	EventType string                 `json:"event_type" db:"event_type"`
	EventData map[string]interface{} `json:"event_data" db:"event_data"`
	CreatedAt time.Time              `json:"created_at" db:"created_at"`
}
