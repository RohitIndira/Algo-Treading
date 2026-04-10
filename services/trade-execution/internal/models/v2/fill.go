package v2

import (
	"time"

	"github.com/google/uuid"
)

type Fill struct {
	FillID          uuid.UUID `gorm:"column:fill_id;type:uuid;primaryKey;default:gen_random_uuid()" json:"fill_id"`
	OrderID         uuid.UUID `gorm:"column:order_id;type:uuid;not null;index:idx_fills_order" json:"order_id"`
	FillQty         int32     `gorm:"column:fill_qty;not null" json:"fill_qty"`
	FillPrice       float64   `gorm:"column:fill_price;type:decimal(15,2);not null" json:"fill_price"`
	ExchangeTradeNo *string   `gorm:"column:exchange_trade_no;type:varchar(50)" json:"exchange_trade_no,omitempty"`
	ExchangeOrderNo *string   `gorm:"column:exchange_order_no;type:varchar(50)" json:"exchange_order_no,omitempty"`
	BrokerOrderID   *string   `gorm:"column:broker_order_id;type:varchar(50);index:idx_fills_broker" json:"broker_order_id,omitempty"`
	FilledAt        time.Time `gorm:"column:filled_at;not null;default:now();index:idx_fills_order" json:"filled_at"`
}

func (Fill) TableName() string { return "fills" }
