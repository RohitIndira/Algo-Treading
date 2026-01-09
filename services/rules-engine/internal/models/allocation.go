package models

import "time"

// AllocationPosition represents a single position in the user's
// 52-week portfolio (or other strategies in future).
type AllocationPosition struct {
	Token    string `json:"token"`
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`
}

// PortfolioAllocationEvent represents the allocation state for a user
// and strategy at a point in time. This is published to Kafka so we can
// track and rebuild allocation state if needed.
type PortfolioAllocationEvent struct {
	UserID          string               `json:"user_id"`
	StrategyID      string               `json:"strategy_id"`
	StrategyName    string               `json:"strategy_name"`
	Date            string               `json:"date"`
	Positions       []AllocationPosition `json:"positions"`
	TotalPositions  int                  `json:"total_positions"`
	MaxPositions    int                  `json:"max_positions"`
	CapitalPerStock float64              `json:"capital_per_stock"`
	Timestamp       time.Time            `json:"timestamp"`
}
