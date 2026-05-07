package models

// RiskLimits defines limits for a user's trading
type RiskLimits struct {
	UserID                  string
	StrategyID              string
	MaxDailyTrades          int
	MaxDailyLoss            float64
	MaxPositionSize         float64
	MaxPerTradeRisk         float64
	MaxPortfolioExposurePct float64
	MaxConcentrationPct     float64
	CircuitBreakerLossPct   float64
	DuplicateOrderWindowSec int
}
