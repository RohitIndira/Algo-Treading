package v2

import (
	"time"

	"github.com/google/uuid"
)

// OrderStatus constants
const (
	StatusReceived        = "RECEIVED"
	StatusValidated       = "VALIDATED"
	StatusPending         = "PENDING"
	StatusSubmitted       = "SUBMITTED"
	StatusPartiallyFilled = "PARTIALLY_FILLED"
	StatusFilled          = "FILLED"
	StatusRejected        = "REJECTED"
	StatusCancelled       = "CANCELLED"
	StatusFailed          = "FAILED"
)

// TradingMode constants
const (
	TradingModePaper = "PAPER"
	TradingModeLive  = "LIVE"
)

type Order struct {
	OrderID      uuid.UUID `gorm:"column:order_id;type:uuid;primaryKey;default:gen_random_uuid()" json:"order_id"`
	InstrumentID *int      `gorm:"column:instrument_id" json:"instrument_id,omitempty"`
	AccountID    *int      `gorm:"column:account_id" json:"account_id,omitempty"`

	// Identity
	UserID       string    `gorm:"column:user_id;type:varchar(50);not null" json:"user_id"`
	StrategyID   string    `gorm:"column:strategy_id;type:varchar(50);not null" json:"strategy_id"`
	StrategyName string    `gorm:"column:strategy_name;type:varchar(255);not null;default:''" json:"strategy_name"`
	EventID      uuid.UUID `gorm:"column:event_id;type:uuid;not null" json:"event_id"`
	SignalID     *uuid.UUID `gorm:"column:signal_id;type:uuid;uniqueIndex:idx_orders_signal_unique,where:signal_id IS NOT NULL" json:"signal_id,omitempty"`

	// Order Specification
	OrderType   string   `gorm:"column:order_type;type:varchar(10);not null" json:"order_type"`
	OrderSide   string   `gorm:"column:order_side;type:varchar(10);not null" json:"order_side"`
	Quantity    int32    `gorm:"column:quantity;not null" json:"quantity"`
	Price       *float64 `gorm:"column:price;type:decimal(15,2)" json:"price,omitempty"`
	Validity    string   `gorm:"column:validity;type:varchar(10);not null;default:'DAY'" json:"validity"`
	ProductType string   `gorm:"column:product_type;type:varchar(20);not null;default:'INTRADAY'" json:"product_type"`
	TradingMode string   `gorm:"column:trading_mode;type:varchar(10);not null;default:'LIVE'" json:"trading_mode"`

	// Current State
	Status       string   `gorm:"column:status;type:varchar(20);not null;default:'RECEIVED'" json:"status"`
	FilledQty    int32    `gorm:"column:filled_qty;not null;default:0" json:"filled_qty"`
	AvgFillPrice *float64 `gorm:"column:avg_fill_price;type:decimal(15,2)" json:"avg_fill_price,omitempty"`

	// Risk
	RiskApproved bool     `gorm:"column:risk_approved;not null;default:false" json:"risk_approved"`
	RiskScore    *float64 `gorm:"column:risk_score;type:decimal(5,2)" json:"risk_score,omitempty"`

	// Timestamps
	CreatedAt   time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	SubmittedAt *time.Time `gorm:"column:submitted_at" json:"submitted_at,omitempty"`
	ExecutedAt  *time.Time `gorm:"column:executed_at" json:"executed_at,omitempty"`

	// Error
	ErrorMessage *string `gorm:"column:error_message;type:text" json:"error_message,omitempty"`
	RetryCount   int32   `gorm:"column:retry_count;not null;default:0" json:"retry_count"`

	// Relations (preload when needed)
	Instrument    *Instrument      `gorm:"foreignKey:InstrumentID" json:"instrument,omitempty"`
	BrokerAccount *BrokerAccount   `gorm:"foreignKey:AccountID" json:"broker_account,omitempty"`
	StatusHistory []StatusHistory  `gorm:"foreignKey:OrderID;references:OrderID" json:"status_history,omitempty"`
	Fills         []Fill           `gorm:"foreignKey:OrderID;references:OrderID" json:"fills,omitempty"`
	BrokerReqs    []BrokerRequest  `gorm:"foreignKey:OrderID;references:OrderID" json:"broker_requests,omitempty"`
	StopLoss      *StopLossConfig  `gorm:"foreignKey:OrderID;references:OrderID" json:"stop_loss,omitempty"`
}

func (Order) TableName() string { return "orders" }

func (o *Order) IsPaper() bool {
	return o.TradingMode == TradingModePaper
}

func (o *Order) IsTerminal() bool {
	switch o.Status {
	case StatusFilled, StatusCancelled, StatusRejected, StatusFailed, "EXECUTED", "TRADED", "A.REJECTED", "EXPIRED":
		return true
	}
	return false
}

func (o *Order) IsFilled() bool {
	switch o.Status {
	case StatusFilled, "EXECUTED", "TRADED":
		return true
	}
	return false
}
