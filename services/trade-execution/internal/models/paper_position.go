package models

import (
	"time"

	"github.com/google/uuid"
)

// PaperPositionStatus represents the status of a paper trading position
type PaperPositionStatus string

const (
	PositionStatusOpen         PaperPositionStatus = "OPEN"
	PositionStatusClosedSL     PaperPositionStatus = "CLOSED_SL"     // Stopped out
	PositionStatusClosedTP     PaperPositionStatus = "CLOSED_TP"     // Take profit hit
	PositionStatusClosedManual PaperPositionStatus = "CLOSED_MANUAL" // Manually closed
)

// ExitReason represents why a position was closed
type ExitReason string

const (
	ExitReasonStopLoss         ExitReason = "STOP_LOSS"
	ExitReasonTakeProfit       ExitReason = "TAKE_PROFIT"
	ExitReasonManual           ExitReason = "MANUAL"
	ExitReasonStrategyDisabled ExitReason = "STRATEGY_DISABLED"
)

// PaperPosition represents a paper trading position
type PaperPosition struct {
	PositionID uuid.UUID `json:"position_id" db:"position_id"`
	UserID     string    `json:"user_id" db:"user_id"`
	StrategyID string    `json:"strategy_id" db:"strategy_id"`

	// Stock information
	StockCode int64  `json:"stock_code" db:"stock_code"`
	Token     int64  `json:"token" db:"token"`
	Symbol    string `json:"symbol" db:"symbol"`
	Exchange  string `json:"exchange" db:"exchange"`

	// Position details
	Quantity     int32   `json:"quantity" db:"quantity"`
	EntryPrice   float64 `json:"entry_price" db:"entry_price"`
	CurrentPrice float64 `json:"current_price" db:"current_price"`

	// Stop loss and take profit
	StopLoss   *float64 `json:"stop_loss,omitempty" db:"stop_loss"`
	TakeProfit *float64 `json:"take_profit,omitempty" db:"take_profit"`

	// PnL tracking
	UnrealizedPnL    float64 `json:"unrealized_pnl" db:"unrealized_pnl"`
	UnrealizedPnLPct float64 `json:"unrealized_pnl_pct" db:"unrealized_pnl_pct"`

	// Position status
	Status PaperPositionStatus `json:"status" db:"status"`

	// Order references
	EntryOrderID uuid.UUID  `json:"entry_order_id" db:"entry_order_id"`
	ExitOrderID  *uuid.UUID `json:"exit_order_id,omitempty" db:"exit_order_id"`

	// Timestamps
	OpenedAt    time.Time  `json:"opened_at" db:"opened_at"`
	ClosedAt    *time.Time `json:"closed_at,omitempty" db:"closed_at"`
	LastUpdated time.Time  `json:"last_updated" db:"last_updated"`
}

// PaperPnLHistory tracks realized PnL from closed positions
type PaperPnLHistory struct {
	PnLID      uuid.UUID `json:"pnl_id" db:"pnl_id"`
	UserID     string    `json:"user_id" db:"user_id"`
	StrategyID string    `json:"strategy_id" db:"strategy_id"`
	PositionID uuid.UUID `json:"position_id" db:"position_id"`

	// Stock information
	Symbol   string `json:"symbol" db:"symbol"`
	Exchange string `json:"exchange" db:"exchange"`

	// Trade details
	Quantity   int32   `json:"quantity" db:"quantity"`
	EntryPrice float64 `json:"entry_price" db:"entry_price"`
	ExitPrice  float64 `json:"exit_price" db:"exit_price"`

	// PnL calculation
	RealizedPnL    float64 `json:"realized_pnl" db:"realized_pnl"`
	RealizedPnLPct float64 `json:"realized_pnl_pct" db:"realized_pnl_pct"`

	// Exit reason
	ExitReason ExitReason `json:"exit_reason" db:"exit_reason"`

	// Timestamps
	EntryTime time.Time `json:"entry_time" db:"entry_time"`
	ExitTime  time.Time `json:"exit_time" db:"exit_time"`
}

// UserDailyPaperPnL represents aggregated daily PnL for a user
type UserDailyPaperPnL struct {
	UserID        string    `json:"user_id" db:"user_id"`
	StrategyID    string    `json:"strategy_id" db:"strategy_id"`
	TradeDate     time.Time `json:"trade_date" db:"trade_date"`
	NumTrades     int       `json:"num_trades" db:"num_trades"`
	DailyPnL      float64   `json:"daily_pnl" db:"daily_pnl"`
	AvgPnLPct     float64   `json:"avg_pnl_pct" db:"avg_pnl_pct"`
	WinningTrades int       `json:"winning_trades" db:"winning_trades"`
	LosingTrades  int       `json:"losing_trades" db:"losing_trades"`
}

// CalculatePnL calculates unrealized PnL for the position based on current price
func (p *PaperPosition) CalculatePnL(currentPrice float64) {
	p.CurrentPrice = currentPrice
	p.UnrealizedPnL = (currentPrice - p.EntryPrice) * float64(p.Quantity)
	if p.EntryPrice > 0 {
		p.UnrealizedPnLPct = ((currentPrice - p.EntryPrice) / p.EntryPrice) * 100
	}
	p.LastUpdated = time.Now()
}

// ShouldTriggerStopLoss checks if current price has hit stop loss
func (p *PaperPosition) ShouldTriggerStopLoss(currentPrice float64) bool {
	if p.StopLoss == nil {
		return false
	}
	// Stop loss triggers when price falls below SL level
	return currentPrice <= *p.StopLoss
}

// ShouldTriggerTakeProfit checks if current price has hit take profit
func (p *PaperPosition) ShouldTriggerTakeProfit(currentPrice float64) bool {
	if p.TakeProfit == nil {
		return false
	}
	// Take profit triggers when price rises above TP level
	return currentPrice >= *p.TakeProfit
}

// CalculateRealizedPnL calculates realized PnL when closing a position
func CalculateRealizedPnL(entryPrice, exitPrice float64, quantity int32) (float64, float64) {
	realizedPnL := (exitPrice - entryPrice) * float64(quantity)
	realizedPnLPct := 0.0
	if entryPrice > 0 {
		realizedPnLPct = ((exitPrice - entryPrice) / entryPrice) * 100
	}
	return realizedPnL, realizedPnLPct
}
