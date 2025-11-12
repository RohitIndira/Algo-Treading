package models

import "time"

// RiskMetrics holds computed metrics for a user
type RiskMetrics struct {
	UserID               string
	DailyTrades          int
	DailyRealizedPnL     float64
	DailyLoss            float64
	CurrentDrawdown      float64
	MaxDrawdown          float64
	PortfolioExposurePct float64
	LargestPositionPct   float64
	LastUpdated          time.Time
}

// Position represents a user's stock position
type Position struct {
	StockCode    int64
	Quantity     int
	AvgPrice     float64
	CurrentPrice float64
}
