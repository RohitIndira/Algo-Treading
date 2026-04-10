package v2

import "time"

type Instrument struct {
	InstrumentID   int       `gorm:"column:instrument_id;primaryKey;autoIncrement" json:"instrument_id"`
	StockCode      int64     `gorm:"column:stock_code;not null;uniqueIndex:uq_instruments_stock_exchange" json:"stock_code"`
	Symbol         string    `gorm:"column:symbol;type:varchar(50);not null" json:"symbol"`
	Exchange       string    `gorm:"column:exchange;type:varchar(10);not null;uniqueIndex:uq_instruments_stock_exchange" json:"exchange"`
	ISIN           *string   `gorm:"column:isin;type:varchar(20)" json:"isin,omitempty"`
	CompanyName    *string   `gorm:"column:company_name;type:varchar(200)" json:"company_name,omitempty"`
	InstrumentType string    `gorm:"column:instrument_type;type:varchar(10);not null;default:'STK'" json:"instrument_type"`
	ExchangeToken  *string   `gorm:"column:exchange_token;type:varchar(50)" json:"exchange_token,omitempty"`
	LotSize        int       `gorm:"column:lot_size;not null;default:1" json:"lot_size"`
	TickSize       *float64  `gorm:"column:tick_size;type:decimal(10,4)" json:"tick_size,omitempty"`
	Series         *string   `gorm:"column:series;type:varchar(10)" json:"series,omitempty"`
	CodifiSymbol   *string   `gorm:"column:codifi_symbol;type:varchar(100)" json:"codifi_symbol,omitempty"`
	IsActive       bool      `gorm:"column:is_active;not null;default:true" json:"is_active"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (Instrument) TableName() string { return "instruments" }
