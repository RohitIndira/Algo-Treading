package models

import "time"

// RealtimePosition represents a single position with live valuation based on
// Redis market data. This is used in RealtimePortfolioEvent which is
// published to a dedicated Kafka topic for UI/analytics.
type RealtimePosition struct {
	Token      string  `json:"token"`
	Symbol     string  `json:"symbol"`
	Exchange   string  `json:"exchange"`
	Quantity   int32   `json:"quantity"`
	EntryPrice float64 `json:"entry_price"`
	LTP        float64 `json:"ltp"`
	PnL        float64 `json:"pnl"`
	PnLPct     float64 `json:"pnl_pct"`
}

// RealtimePortfolioEvent represents the current marked-to-market portfolio
// for a user and strategy at a point in time. It is derived from allocation
// state plus live prices from Redis.
type RealtimePortfolioEvent struct {
	UserID        string             `json:"user_id"`
	StrategyID    string             `json:"strategy_id"`
	StrategyName  string             `json:"strategy_name"`
	Mode          string             `json:"mode"` // LIVE or PAPER
	Positions     []RealtimePosition `json:"positions"`
	TotalPnL      float64            `json:"total_pnl"`       // Open PnL for current positions
	TotalInvested float64            `json:"total_invested"`  // Sum of buy_price * qty for open positions
	TotalCurrent  float64            `json:"total_current"`   // Sum of LTP * qty for open positions
	ClosedPnL     float64            `json:"closed_pnl"`      // Realized PnL from exited positions (currently 0 for 52W)
	PortfolioValue float64           `json:"portfolio_value"` // Market value of open positions + ClosedPnL
	AveragePerStock float64          `json:"average_per_stock"` // PortfolioValue / 25 (per strategy design)
	Timestamp     time.Time          `json:"timestamp"`
	// StreamID identifies the logical PnL stream for this snapshot.
	// When strategies change we rotate to a new stream so that
	// consumers can distinguish old vs new configurations.
	StreamID string `json:"stream_id,omitempty"`
}
