package models

import (
	"time"
)

// Strategy represents a user's trading strategy
type Strategy struct {
	StrategyID   string      `json:"strategy_id" bson:"strategy_id"`
	UserID       string      `json:"user_id" bson:"user_id"`
	StrategyName string      `json:"strategy_name" bson:"strategy_name"`
	Active       bool        `json:"active" bson:"active"`
	Conditions   Conditions  `json:"conditions" bson:"conditions"`
	TradeConfig  TradeConfig `json:"trade_config" bson:"trade_config"`
	RiskLimits   RiskLimits  `json:"risk_limits" bson:"risk_limits"`
	CreatedAt    time.Time   `json:"created_at" bson:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at" bson:"updated_at"`
}

// Conditions represents the conditions for a strategy
type Conditions struct {
	ImpactScoreThreshold int32      `json:"impact_score_threshold" bson:"impact_score_threshold"`
	Sentiments           []string   `json:"sentiments" bson:"sentiments"`
	Categories           []string   `json:"categories" bson:"categories"`
	Stocks               []int64    `json:"stocks" bson:"stocks"`
	PriceRange           PriceRange `json:"price_range" bson:"price_range"`
	VolumeThreshold      int64      `json:"volume_threshold" bson:"volume_threshold"`
	PctChangeThreshold   float64    `json:"pct_change_threshold" bson:"pct_change_threshold"`
}

// PriceRange represents price range filter
type PriceRange struct {
	MinPrice float64 `json:"min_price" bson:"min_price"`
	MaxPrice float64 `json:"max_price" bson:"max_price"`
}

// TradeConfig represents trade configuration
type TradeConfig struct {
	OrderType       string  `json:"order_type" bson:"order_type"` // MARKET, LIMIT
	Quantity        int32   `json:"quantity" bson:"quantity"`
	MaxPositionSize float64 `json:"max_position_size" bson:"max_position_size"`
	StopLossPct     float64 `json:"stop_loss_pct" bson:"stop_loss_pct"`
	TakeProfitPct   float64 `json:"take_profit_pct" bson:"take_profit_pct"`
	Exchange        string  `json:"exchange" bson:"exchange"`
}

// RiskLimits represents risk limits
type RiskLimits struct {
	MaxDailyTrades int32   `json:"max_daily_trades" bson:"max_daily_trades"`
	MaxLossPerDay  float64 `json:"max_loss_per_day" bson:"max_loss_per_day"`
	PositionSizing string  `json:"position_sizing" bson:"position_sizing"` // FIXED, PERCENTAGE
}

// ElasticsearchStrategy represents a strategy indexed in Elasticsearch
type ElasticsearchStrategy struct {
	StrategyID     string   `json:"strategy_id"`
	UserID         string   `json:"user_id"`
	StrategyName   string   `json:"strategy_name"`
	Active         bool     `json:"active"`
	ImpactScoreMin int32    `json:"impact_score_min"`
	Sentiments     []string `json:"sentiments"`
	Categories     []string `json:"categories"`
	Stocks         []int64  `json:"stocks"`
	PriceMin       float64  `json:"price_min"`
	PriceMax       float64  `json:"price_max"`
	VolumeMin      int64    `json:"volume_min"`
	PctChangeMin   float64  `json:"pct_change_min"`
	Exchange       string   `json:"exchange"`
	MaxDailyTrades int32    `json:"max_daily_trades"`
	MaxLossPerDay  float64  `json:"max_loss_per_day"`
	UpdatedAt      int64    `json:"updated_at"` // Unix timestamp
}

// ToElasticsearchStrategy converts Strategy to ElasticsearchStrategy
func (s *Strategy) ToElasticsearchStrategy() *ElasticsearchStrategy {
	return &ElasticsearchStrategy{
		StrategyID:     s.StrategyID,
		UserID:         s.UserID,
		StrategyName:   s.StrategyName,
		Active:         s.Active,
		ImpactScoreMin: s.Conditions.ImpactScoreThreshold,
		Sentiments:     s.Conditions.Sentiments,
		Categories:     s.Conditions.Categories,
		Stocks:         s.Conditions.Stocks,
		PriceMin:       s.Conditions.PriceRange.MinPrice,
		PriceMax:       s.Conditions.PriceRange.MaxPrice,
		VolumeMin:      s.Conditions.VolumeThreshold,
		PctChangeMin:   s.Conditions.PctChangeThreshold,
		Exchange:       s.TradeConfig.Exchange,
		MaxDailyTrades: s.RiskLimits.MaxDailyTrades,
		MaxLossPerDay:  s.RiskLimits.MaxLossPerDay,
		UpdatedAt:      s.UpdatedAt.Unix(),
	}
}

// Validate validates a strategy
func (s *Strategy) Validate() error {
	if s.StrategyID == "" {
		return ErrInvalidStrategyID
	}
	if s.UserID == "" {
		return ErrInvalidUserID
	}
	if s.Conditions.ImpactScoreThreshold < 0 || s.Conditions.ImpactScoreThreshold > 10 {
		return ErrInvalidImpactScore
	}
	if len(s.Conditions.Sentiments) == 0 {
		return ErrInvalidSentiments
	}
	if s.TradeConfig.Quantity <= 0 {
		return ErrInvalidQuantity
	}
	if s.TradeConfig.OrderType != "MARKET" && s.TradeConfig.OrderType != "LIMIT" {
		return ErrInvalidOrderType
	}
	return nil
}

// MatchesStock checks if strategy applies to the stock
func (s *Strategy) MatchesStock(stockCode int64) bool {
	// Empty stocks list means apply to all stocks
	if len(s.Conditions.Stocks) == 0 {
		return true
	}

	for _, stock := range s.Conditions.Stocks {
		if stock == stockCode {
			return true
		}
	}
	return false
}

// MatchesSentiment checks if strategy applies to the sentiment
func (s *Strategy) MatchesSentiment(sentiment string) bool {
	if len(s.Conditions.Sentiments) == 0 {
		return true
	}

	for _, s := range s.Conditions.Sentiments {
		if s == sentiment {
			return true
		}
	}
	return false
}

// MatchesCategory checks if strategy applies to the category
func (s *Strategy) MatchesCategory(category string) bool {
	if len(s.Conditions.Categories) == 0 {
		return true
	}

	for _, c := range s.Conditions.Categories {
		if c == category {
			return true
		}
	}
	return false
}
