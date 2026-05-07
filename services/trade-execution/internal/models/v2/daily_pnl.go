package v2

import "time"

type DailyPnLSummary struct {
	ID            int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	UserID        string    `gorm:"column:user_id;type:varchar(50);not null;uniqueIndex:uq_dpnl" json:"user_id"`
	StrategyID    *string   `gorm:"column:strategy_id;type:varchar(50);uniqueIndex:uq_dpnl" json:"strategy_id,omitempty"`
	TradingMode   string    `gorm:"column:trading_mode;type:varchar(10);not null;uniqueIndex:uq_dpnl" json:"trading_mode"`
	TradeDate     time.Time `gorm:"column:trade_date;type:date;not null;uniqueIndex:uq_dpnl" json:"trade_date"`

	TotalTrades   int     `gorm:"column:total_trades;not null;default:0" json:"total_trades"`
	WinningTrades int     `gorm:"column:winning_trades;not null;default:0" json:"winning_trades"`
	LosingTrades  int     `gorm:"column:losing_trades;not null;default:0" json:"losing_trades"`
	TotalInvested float64 `gorm:"column:total_invested;type:decimal(15,2);not null;default:0" json:"total_invested"`
	GrossPnL      float64 `gorm:"column:gross_pnl;type:decimal(15,2);not null;default:0" json:"gross_pnl"`
	TotalCharges  float64 `gorm:"column:total_charges;type:decimal(10,2);not null;default:0" json:"total_charges"`
	NetPnL        float64 `gorm:"column:net_pnl;type:decimal(15,2);not null;default:0" json:"net_pnl"`
	MaxDrawdown   *float64 `gorm:"column:max_drawdown;type:decimal(15,2)" json:"max_drawdown,omitempty"`
	WinRate       *float64 `gorm:"column:win_rate;type:decimal(5,2)" json:"win_rate,omitempty"`
}

func (DailyPnLSummary) TableName() string { return "daily_pnl_summary" }
