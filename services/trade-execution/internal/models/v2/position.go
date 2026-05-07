package v2

import (
	"time"

	"github.com/google/uuid"
)

// Position status constants
const (
	PositionOpen   = "OPEN"
	PositionClosed = "CLOSED"
)

// Position side constants
const (
	SideLong  = "LONG"
	SideShort = "SHORT"
)

// Exit reason constants
const (
	ExitSLHit            = "SL_HIT"
	ExitTPHit            = "TP_HIT"
	ExitManual           = "MANUAL"
	ExitSquareOff        = "SQUARE_OFF"
	ExitStrategyDeleted  = "STRATEGY_DELETED"
	ExitPriceExceeded    = "PRICE_EXCEEDED"
	ExitBrokerCancelled  = "BROKER_CANCELLED"
)

type Position struct {
	PositionID    uuid.UUID  `gorm:"column:position_id;type:uuid;primaryKey;default:gen_random_uuid()" json:"position_id"`
	InstrumentID  int        `gorm:"column:instrument_id;not null" json:"instrument_id"`
	UserID        string     `gorm:"column:user_id;type:varchar(50);not null" json:"user_id"`
	StrategyID    *string    `gorm:"column:strategy_id;type:varchar(50)" json:"strategy_id,omitempty"`
	StrategyName  *string    `gorm:"column:strategy_name;type:varchar(255)" json:"strategy_name,omitempty"`
	TradingMode   string     `gorm:"column:trading_mode;type:varchar(10);not null" json:"trading_mode"`
	Side          string     `gorm:"column:side;type:varchar(10);not null" json:"side"`

	// Quantities & prices (derived from position_fills)
	OpenQty       int32      `gorm:"column:open_qty;not null;default:0" json:"open_qty"`
	AvgEntryPrice *float64   `gorm:"column:avg_entry_price;type:decimal(15,2)" json:"avg_entry_price,omitempty"`
	AvgExitPrice  *float64   `gorm:"column:avg_exit_price;type:decimal(15,2)" json:"avg_exit_price,omitempty"`

	// P&L
	RealizedPnL  *float64   `gorm:"column:realized_pnl;type:decimal(15,2)" json:"realized_pnl,omitempty"`
	TotalCharges float64    `gorm:"column:total_charges;type:decimal(10,2);not null;default:0" json:"total_charges"`
	NetPnL       *float64   `gorm:"column:net_pnl;type:decimal(15,2)" json:"net_pnl,omitempty"`

	// Exit info
	ExitReason *string     `gorm:"column:exit_reason;type:varchar(20)" json:"exit_reason,omitempty"`

	// Status
	Status    string      `gorm:"column:status;type:varchar(10);not null;default:'OPEN'" json:"status"`
	OpenedAt  time.Time   `gorm:"column:opened_at;not null;default:now()" json:"opened_at"`
	ClosedAt  *time.Time  `gorm:"column:closed_at" json:"closed_at,omitempty"`
	UpdatedAt time.Time   `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`

	// Relations
	Instrument    *Instrument    `gorm:"foreignKey:InstrumentID" json:"instrument,omitempty"`
	PositionFills []PositionFill `gorm:"foreignKey:PositionID" json:"position_fills,omitempty"`
}

func (Position) TableName() string { return "positions" }

type PositionFill struct {
	ID         int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	PositionID uuid.UUID `gorm:"column:position_id;type:uuid;not null;uniqueIndex:uq_pf_position_fill" json:"position_id"`
	FillID     uuid.UUID `gorm:"column:fill_id;type:uuid;not null;uniqueIndex:uq_pf_position_fill" json:"fill_id"`
	OrderID    uuid.UUID `gorm:"column:order_id;type:uuid;not null" json:"order_id"`
	Direction  string    `gorm:"column:direction;type:varchar(5);not null" json:"direction"` // ENTRY / EXIT
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	Fill       *Fill     `gorm:"foreignKey:FillID" json:"fill,omitempty"`
}

func (PositionFill) TableName() string { return "position_fills" }
