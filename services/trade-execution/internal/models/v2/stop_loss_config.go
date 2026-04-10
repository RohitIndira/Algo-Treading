package v2

import (
	"time"

	"github.com/google/uuid"
)

// SL type constants
const (
	SLTypeFixed    = "FIXED"
	SLTypeTrailing = "TRAILING"
)

// Trigger reason constants
const (
	TriggerSLHit          = "SL_HIT"
	TriggerTPHit          = "TP_HIT"
	TriggerPriceExceeded  = "PRICE_EXCEEDED"
)

type StopLossConfig struct {
	ID              int        `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	OrderID         uuid.UUID  `gorm:"column:order_id;type:uuid;not null;uniqueIndex" json:"order_id"`
	SLType          string     `gorm:"column:sl_type;type:varchar(10);not null;default:'FIXED'" json:"sl_type"`
	StopLossPrice   *float64   `gorm:"column:stop_loss_price;type:decimal(15,2)" json:"stop_loss_price,omitempty"`
	TakeProfitPrice *float64   `gorm:"column:take_profit_price;type:decimal(15,2)" json:"take_profit_price,omitempty"`
	StopLossPct     *float64   `gorm:"column:stop_loss_pct;type:decimal(10,4)" json:"stop_loss_pct,omitempty"`
	TakeProfitPct   *float64   `gorm:"column:take_profit_pct;type:decimal(10,4)" json:"take_profit_pct,omitempty"`
	TrailingPct     *float64   `gorm:"column:trailing_pct;type:decimal(10,4)" json:"trailing_pct,omitempty"`
	MaxMonitorPrice *float64   `gorm:"column:max_monitor_price;type:decimal(15,2)" json:"max_monitor_price,omitempty"`
	HighestPrice    *float64   `gorm:"column:highest_price;type:decimal(15,2)" json:"highest_price,omitempty"`
	IsActive        bool       `gorm:"column:is_active;not null;default:true" json:"is_active"`
	TriggeredAt     *time.Time `gorm:"column:triggered_at" json:"triggered_at,omitempty"`
	TriggerReason   *string    `gorm:"column:trigger_reason;type:varchar(20)" json:"trigger_reason,omitempty"`
	CreatedAt       time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (StopLossConfig) TableName() string { return "stop_loss_config" }
