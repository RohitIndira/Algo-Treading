package models

import (
	"strings"
	"time"
)

// Strategy represents a user's trading strategy
type Strategy struct {
	StrategyID   string      `json:"strategy_id" bson:"strategy_id"`
	UserID       string      `json:"user_id" bson:"user_id"`
	StrategyName string      `json:"strategy_name" bson:"strategy_name"`
	Version      uint64      `json:"version" bson:"version"`
	Active       bool        `json:"active" bson:"active"`
	TradingMode  string      `json:"trading_mode" bson:"trading_mode"`
	Conditions   Conditions  `json:"conditions" bson:"conditions"`
	TradeConfig  TradeConfig `json:"trade_config" bson:"trade_config"`
	RiskLimits   RiskLimits  `json:"risk_limits" bson:"risk_limits"`
	CreatedAt    time.Time   `json:"created_at" bson:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at" bson:"updated_at"`

	// Frontend authentication data (stored with strategy)
	BearerToken string `json:"bearer_token" bson:"bearer_token"` // JWT bearer token
	AppId       string `json:"app_id" bson:"app_id"`             // Application ID
	Source      string `json:"source" bson:"source"`             // Source platform
}

// StrategyConfig is the canonical in-memory configuration object used by the
// rules-engine configstore and bootstrapper.
//
// It is currently identical to Strategy.
type StrategyConfig = Strategy

// Conditions represents the conditions for a strategy
type Conditions struct {
	MatchAllNews    bool           `json:"match_all_news" bson:"match_all_news"`
	ImpactScoreMin  int32          `json:"impact_score_min" bson:"impact_score_min"`
	ImpactScoreMax  int32          `json:"impact_score_max" bson:"impact_score_max"`
	Sentiments      []string       `json:"sentiments" bson:"sentiments"`
	Categories      []string       `json:"categories" bson:"categories"`
	Stocks          []int64        `json:"stocks" bson:"stocks"`
	PriceRange      PriceRange     `json:"price_range" bson:"price_range"` // legacy; may be unset
	VolumeThreshold int64          `json:"volume_threshold" bson:"volume_threshold"`
	MinPctChange    float64        `json:"min_pct_change" bson:"min_pct_change"`
	MaxPctChange    float64        `json:"max_pct_change" bson:"max_pct_change"`
	Exchanges       []string       `json:"exchanges" bson:"exchanges"`
	MarketCapRange  MarketCapRange `json:"market_cap_range" bson:"market_cap_range"` // Market cap filter
}

// PriceRange represents price range filter
type PriceRange struct {
	MinPrice float64 `json:"min_price" bson:"min_price"`
	MaxPrice float64 `json:"max_price" bson:"max_price"`
}

// MarketCapRange represents market cap range filter (in crores)
type MarketCapRange struct {
	MinMcap float64 `json:"min_mcap" bson:"min_mcap"` // Minimum market cap in crores
	MaxMcap float64 `json:"max_mcap" bson:"max_mcap"` // Maximum market cap in crores
}

// TradeConfig represents trade configuration
type TradeConfig struct {
	OrderType       string  `json:"order_type" bson:"order_type"` // MARKET, LIMIT
	Quantity        int32   `json:"quantity" bson:"quantity"`
	MaxPositionSize float64 `json:"max_position_size" bson:"max_position_size"`
	StopLossPct     float64 `json:"stop_loss_pct" bson:"stop_loss_pct"`
	TakeProfitPct   float64 `json:"take_profit_pct" bson:"take_profit_pct"`
	Exchange        string  `json:"exchange" bson:"exchange"`
	OrderSide       string  `json:"order_side" bson:"order_side"`           // BUY/SELL
	Validity        string  `json:"validity" bson:"validity"`               // DAY/IOC
	LimitPrice      float64 `json:"limit_price" bson:"limit_price"`         // LIMIT only
	StopLossType    string  `json:"stop_loss_type" bson:"stop_loss_type"`   // FIXED, TRAILING
	TrailingSLPct   float64 `json:"trailing_sl_pct" bson:"trailing_sl_pct"` // Trailing SL percentage
	ProductType     string  `json:"product_type" bson:"product_type"`       // INTRADAY, DELIVERY, CASH
}

// RiskLimits represents risk limits
type RiskLimits struct {
	MaxDailyTrades      int32   `json:"max_daily_trades" bson:"max_daily_trades"`
	MaxLossPerDay       float64 `json:"max_loss_per_day" bson:"max_loss_per_day"`
	MaxPositionSize     float64 `json:"max_position_size" bson:"max_position_size"`
	MaxPerTradeRisk     float64 `json:"max_per_trade_risk" bson:"max_per_trade_risk"`
	PositionSizing      string  `json:"position_sizing" bson:"position_sizing"` // FIXED, PERCENTAGE
	EnableAutoSquareOff bool    `json:"enable_auto_square_off" bson:"enable_auto_square_off"`
	AutoSquareOffTime   string  `json:"auto_square_off_time" bson:"auto_square_off_time"` // "15:05" format
}

// NOTE: ElasticsearchStrategy + ToElasticsearchStrategy removed.
// Rules Engine no longer uses Elasticsearch.

// normalizeExchange removes the EXCHANGE_ prefix if present
// Converts "EXCHANGE_NSE" -> "NSE", "EXCHANGE_BSE" -> "BSE"
// Leaves "NSE" and "BSE" as-is
func normalizeExchange(exchange string) string {
	return strings.TrimPrefix(exchange, "EXCHANGE_")
}

// Validate validates a strategy
func (s *Strategy) Validate() error {
	if s.StrategyID == "" {
		return ErrInvalidStrategyID
	}
	if s.UserID == "" {
		return ErrInvalidUserID
	}
	if s.Conditions.ImpactScoreMin < 0 || s.Conditions.ImpactScoreMin > 10 {
		return ErrInvalidImpactScore
	}
	if s.Conditions.ImpactScoreMax < 0 || s.Conditions.ImpactScoreMax > 10 {
		return ErrInvalidImpactScore
	}
	if s.TradeConfig.Quantity <= 0 {
		return ErrInvalidQuantity
	}
	if s.TradeConfig.OrderType != "MARKET" && s.TradeConfig.OrderType != "LIMIT" && s.TradeConfig.OrderType != "BRACKET" && s.TradeConfig.OrderType != "STOP_LOSS" {
		return ErrInvalidOrderType
	}
	// NOTE: Sentiments/categories/stocks can be empty to mean "match all".
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
