package models

import "time"

// ExecutionResult represents the result of an order execution
type ExecutionResult struct {
	// Order Identification
	ExecutionID string `json:"execution_id"`
	OrderID     string `json:"order_id"`
	SignalID    string `json:"signal_id,omitempty"`

	// User & Strategy Info
	UserID       string `json:"user_id"`
	StrategyID   string `json:"strategy_id"`
	StrategyName string `json:"strategy_name"`

	// Broker Information
	BrokerOrderID   string `json:"broker_order_id"`
	BrokerName      string `json:"broker_name"`
	ExchangeOrderID string `json:"exchange_order_id,omitempty"`

	// Execution Status
	Status        string `json:"status"` // EXECUTED, PARTIALLY_FILLED, FAILED, REJECTED, CANCELLED
	StatusMessage string `json:"status_message"`

	// Stock Details
	StockCode   int64  `json:"stock_code"`
	Symbol      string `json:"symbol"`
	CompanyName string `json:"company_name,omitempty"`
	Exchange    string `json:"exchange"`

	// Order Details (Original)
	OrderType         string  `json:"order_type"`
	OrderSide         string  `json:"order_side"`
	RequestedQuantity int32   `json:"requested_quantity"`
	RequestedPrice    float64 `json:"requested_price"`

	// Execution Details (Actual)
	ExecutedQuantity int32   `json:"executed_quantity"`
	ExecutedPrice    float64 `json:"executed_price"`
	AveragePrice     float64 `json:"average_price"`
	TotalValue       float64 `json:"total_value"`

	// Financial Details
	Brokerage       float64 `json:"brokerage"`
	ExchangeCharges float64 `json:"exchange_charges"`
	GST             float64 `json:"gst"`
	STT             float64 `json:"stt"`
	StampDuty       float64 `json:"stamp_duty"`
	TotalCharges    float64 `json:"total_charges"`
	NetAmount       float64 `json:"net_amount"`

	// Timestamps
	OrderPlacedAt   time.Time `json:"order_placed_at"`
	ExecutionTime   time.Time `json:"execution_time"`
	BrokerTimestamp time.Time `json:"broker_timestamp,omitempty"`

	// Related Information
	EventID      string  `json:"event_id,omitempty"`
	NewsCategory string  `json:"news_category,omitempty"`
	ImpactScore  int32   `json:"impact_score,omitempty"`
	MatchScore   float64 `json:"match_score,omitempty"`

	// Error Information
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
	RejectionReason string `json:"rejection_reason,omitempty"`

	// Metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// OrderUpdate represents a user-friendly order update notification
type OrderUpdate struct {
	// Order Identification
	UpdateID string `json:"update_id"`
	OrderID  string `json:"order_id"`
	UserID   string `json:"user_id"`

	// Update Type
	UpdateType string `json:"update_type"` // EXECUTION_SUCCESS, ORDER_REJECTED, etc.
	Priority   string `json:"priority"`    // HIGH, MEDIUM, LOW

	// User-Friendly Message
	Title           string `json:"title"`
	Message         string `json:"message"`
	DetailedMessage string `json:"detailed_message"`

	// Status Display
	Status      string `json:"status"`
	StatusEmoji string `json:"status_emoji"`
	StatusColor string `json:"status_color"`

	// Order Summary
	OrderSummary OrderSummary `json:"order_summary"`

	// Execution Details (if applicable)
	ExecutionDetails *ExecutionDetails `json:"execution_details,omitempty"`

	// Strategy Context
	Strategy StrategyContext `json:"strategy"`

	// Action Items
	Actions []ActionItem `json:"actions"`

	// Notification Preferences
	NotificationChannels NotificationChannels `json:"notification_channels"`

	// Timestamps
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// OrderSummary contains user-friendly order summary
type OrderSummary struct {
	Stock     string `json:"stock"`
	Action    string `json:"action"`
	Quantity  int32  `json:"quantity"`
	Price     string `json:"price"`
	Exchange  string `json:"exchange"`
	OrderType string `json:"order_type"`
}

// ExecutionDetails contains execution information for updates
type ExecutionDetails struct {
	ExecutedAt    time.Time `json:"executed_at"`
	ExecutedPrice string    `json:"executed_price"`
	TotalAmount   string    `json:"total_amount"`
	Charges       string    `json:"charges"`
	BrokerRef     string    `json:"broker_ref"`
}

// StrategyContext contains strategy information for updates
type StrategyContext struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Trigger string `json:"trigger"`
}

// ActionItem represents an action button for the user
type ActionItem struct {
	ActionType string `json:"action_type"`
	Label      string `json:"label"`
	URL        string `json:"url"`
}

// NotificationChannels specifies which channels to use
type NotificationChannels struct {
	Push  bool `json:"push"`
	Email bool `json:"email"`
	SMS   bool `json:"sms"`
	InApp bool `json:"in_app"`
}

// Trade Signal from Kafka (consumed from trade-signals topic)
type TradeSignal struct {
	OrderID      string `json:"order_id"`
	UserID       string `json:"user_id"`
	StrategyID   string `json:"strategy_id"`
	StrategyName string `json:"strategy_name"`
	// TradingMode indicates whether this signal is PAPER or LIVE.
	// When PAPER, trade-execution must NOT place real orders.
	TradingMode string `json:"trading_mode,omitempty"`
	EventID      string `json:"event_id"`
	StockCode    int64  `json:"stock_code"`
	// Token is the actual trading token (scrip token) from rules-engine
	// OrderRequest.Token. This is what Odin expects as scrip_token. Keeping it
	// alongside StockCode lets us evolve instrument mapping without breaking
	// analytics.
	Token        int64     `json:"token"`
	Symbol       string    `json:"symbol"`
	Exchange     string    `json:"exchange"`
	OrderType    string    `json:"order_type"`
	Quantity     int32     `json:"quantity"`
	Price        float64   `json:"price"`
	StopLoss     float64   `json:"stop_loss"`
	TakeProfit   float64   `json:"take_profit"`
	MatchScore   float64   `json:"match_score"`
	ImpactScore  int32     `json:"impact_score"`
	Sentiment    string    `json:"sentiment"`
	NewsCategory string    `json:"news_category"`
	Timestamp    time.Time `json:"timestamp"`
}
