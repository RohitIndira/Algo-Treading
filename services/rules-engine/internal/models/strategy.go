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
	Active       bool        `json:"active" bson:"active"`
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

// Conditions represents the conditions for a strategy
type Conditions struct {
	// Stock selection
	Stocks    []int64  `json:"stocks" bson:"stocks"`
	Exchanges []string `json:"exchanges" bson:"exchanges"`

	// Price-based conditions
	PriceRange         PriceRange `json:"price_range" bson:"price_range"`
	VolumeThreshold    int64      `json:"volume_threshold" bson:"volume_threshold"`
	PctChangeThreshold float64    `json:"pct_change_threshold" bson:"pct_change_threshold"`

	// Market depth conditions (primary triggers)
	MinBidQuantity          int64   `json:"min_bid_quantity" bson:"min_bid_quantity"`                     // Minimum bid quantity at best price
	MinAskQuantity          int64   `json:"min_ask_quantity" bson:"min_ask_quantity"`                     // Minimum ask quantity at best price
	MaxSpreadPct            float64 `json:"max_spread_pct" bson:"max_spread_pct"`                         // Maximum spread percentage
	MinBidAskRatio          float64 `json:"min_bid_ask_ratio" bson:"min_bid_ask_ratio"`                   // Min ratio of bid/ask quantity
	MaxBidAskRatio          float64 `json:"max_bid_ask_ratio" bson:"max_bid_ask_ratio"`                   // Max ratio of bid/ask quantity
	MinTotalDepthQty        int64   `json:"min_total_depth_qty" bson:"min_total_depth_qty"`               // Min total bid+ask quantity
	RequireLTPBetweenSpread bool    `json:"require_ltp_between_spread" bson:"require_ltp_between_spread"` // LTP must be between best bid/ask

	// Market cap filter (optional)
	MarketCapRange MarketCapRange `json:"market_cap_range" bson:"market_cap_range"`
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
	OrderSide       string  `json:"order_side" bson:"order_side"` // BUY, SELL
	Quantity        int32   `json:"quantity" bson:"quantity"`
	MaxPositionSize float64 `json:"max_position_size" bson:"max_position_size"`
	StopLossPct     float64 `json:"stop_loss_pct" bson:"stop_loss_pct"`
	TakeProfitPct   float64 `json:"take_profit_pct" bson:"take_profit_pct"`
	Exchange        string  `json:"exchange" bson:"exchange"`
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

// ElasticsearchStrategy represents a strategy indexed in Elasticsearch
type ElasticsearchStrategy struct {
	StrategyID       string   `json:"strategy_id"`
	UserID           string   `json:"user_id"`
	StrategyName     string   `json:"strategy_name"`
	Active           bool     `json:"active"`
	Stocks           []int64  `json:"stocks"`
	Exchanges        []string `json:"exchanges"`
	PriceMin         float64  `json:"price_min"`
	PriceMax         float64  `json:"price_max"`
	VolumeMin        int64    `json:"volume_min"`
	PctChangeMin     float64  `json:"pct_change_min"`
	MinBidQty        int64    `json:"min_bid_qty"`
	MinAskQty        int64    `json:"min_ask_qty"`
	MaxSpreadPct     float64  `json:"max_spread_pct"`
	MinBidAskRatio   float64  `json:"min_bid_ask_ratio"`
	MaxBidAskRatio   float64  `json:"max_bid_ask_ratio"`
	MinTotalDepthQty int64    `json:"min_total_depth_qty"`
	MaxDailyTrades   int32    `json:"max_daily_trades"`
	MaxLossPerDay    float64  `json:"max_loss_per_day"`
	UpdatedAt        int64    `json:"updated_at"` // Unix timestamp
}

// ToElasticsearchStrategy converts Strategy to ElasticsearchStrategy
func (s *Strategy) ToElasticsearchStrategy() *ElasticsearchStrategy {
	return &ElasticsearchStrategy{
		StrategyID:       s.StrategyID,
		UserID:           s.UserID,
		StrategyName:     s.StrategyName,
		Active:           s.Active,
		Stocks:           s.Conditions.Stocks,
		Exchanges:        s.Conditions.Exchanges,
		PriceMin:         s.Conditions.PriceRange.MinPrice,
		PriceMax:         s.Conditions.PriceRange.MaxPrice,
		VolumeMin:        s.Conditions.VolumeThreshold,
		PctChangeMin:     s.Conditions.PctChangeThreshold,
		MinBidQty:        s.Conditions.MinBidQuantity,
		MinAskQty:        s.Conditions.MinAskQuantity,
		MaxSpreadPct:     s.Conditions.MaxSpreadPct,
		MinBidAskRatio:   s.Conditions.MinBidAskRatio,
		MaxBidAskRatio:   s.Conditions.MaxBidAskRatio,
		MinTotalDepthQty: s.Conditions.MinTotalDepthQty,
		MaxDailyTrades:   s.RiskLimits.MaxDailyTrades,
		MaxLossPerDay:    s.RiskLimits.MaxLossPerDay,
		UpdatedAt:        s.UpdatedAt.Unix(),
	}
}

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
