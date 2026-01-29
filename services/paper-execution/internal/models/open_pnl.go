package models

import "time"

// OpenPositionPnL represents unrealized P&L for open positions.
// Formula: (CurrentPrice - EntryPrice) × Quantity
type OpenPositionPnL struct {
	UserID       string                `json:"user_id"`
	StrategyID   string                `json:"strategy_id"`
	Token        int64                 `json:"token"`
	Symbol       string                `json:"symbol"`
	Exchange     string                `json:"exchange"`
	Quantity     int32                 `json:"quantity"`
	EntryPrice   float64               `json:"entry_price"`
	CurrentPrice float64               `json:"current_price"`
	UnrealizedPnL float64              `json:"unrealized_pnl"`
	PnLPercent   float64               `json:"pnl_percent"`
	Timestamp    time.Time             `json:"timestamp"`
}

// PortfolioPnLSummary is a comprehensive snapshot for a user's portfolio.
// This matches the strategy's formula:
// Portfolio Value = Market Value of All Open Positions + Total Closed PnL
type PortfolioPnLSummary struct {
	UserID            string            `json:"user_id"`
	StrategyID        string            `json:"strategy_id"`
	
	// Open positions with unrealized P&L
	OpenPositions     []OpenPositionPnL `json:"open_positions"`
	OpenPositionsCount int              `json:"open_positions_count"`
	
	// Market value of all open positions
	TotalMarketValue  float64           `json:"total_market_value"`
	
	// Unrealized P&L from open positions
	TotalUnrealizedPnL float64          `json:"total_unrealized_pnl"`
	
	// Realized P&L from closed positions
	TotalClosedPnL    float64           `json:"total_closed_pnl"`
	
	// Portfolio Value = TotalMarketValue + TotalClosedPnL
	PortfolioValue    float64           `json:"portfolio_value"`
	
	// Average per-stock capital (used for reinvestment)
	AvgPerStock       float64           `json:"avg_per_stock"`
	
	// Available capital for reinvestment (when positions exit)
	AvailableCapital  float64           `json:"available_capital"`
	
	Timestamp         time.Time         `json:"timestamp"`
}

// ReinvestmentSignal is emitted when a position fully closes and capital
// becomes available for buying a new 52W breakout stock.
type ReinvestmentSignal struct {
	UserID           string    `json:"user_id"`
	StrategyID       string    `json:"strategy_id"`
	AvailableCapital float64   `json:"available_capital"`
	ClosedToken      int64     `json:"closed_token"`
	ClosedSymbol     string    `json:"closed_symbol"`
	Timestamp        time.Time `json:"timestamp"`
}
