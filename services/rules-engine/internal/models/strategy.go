package models

import "time"

// Strategy represents a user's trading strategy.
//
// Slimmed 2026-06-25 (Cat B trim): the news-event-driven path was removed,
// taking with it the Conditions struct, RiskLimits, MatchesStock/Sentiment/
// Category methods, the Validate method, and the BearerToken/AppId/Source
// fields (the legacy Kafka leak fix the Manthan path already addressed).
//
// What remains is what the live Manthan strategy lifecycle actually reads:
// identity (StrategyID, UserID, Name, Type), state (Active, Version, mode,
// timestamps), and the TradeConfig block the allocator + order generator
// consume.
type Strategy struct {
	StrategyID   string      `json:"strategy_id" bson:"strategy_id"`
	UserID       string      `json:"user_id" bson:"user_id"`
	StrategyName string      `json:"strategy_name" bson:"strategy_name"`
	StrategyType string      `json:"strategy_type" bson:"strategy_type"` // MANTHAN (legacy NEWS/52W_BREAKOUT no longer accepted)
	Version      uint64      `json:"version" bson:"version"`
	Active       bool        `json:"active" bson:"active"`
	TradingMode  string      `json:"trading_mode" bson:"trading_mode"`
	TradeConfig  TradeConfig `json:"trade_config" bson:"trade_config"`
	CreatedAt    time.Time   `json:"created_at" bson:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at" bson:"updated_at"`
}

// StrategyConfig is the canonical in-memory configuration object used by the
// rules-engine configstore and bootstrapper. Currently identical to Strategy.
type StrategyConfig = Strategy

// TradeConfig represents trade configuration. Carries both legacy-shaped
// fields (OrderType/Quantity/Exchange — kept because the Manthan order
// generator still reads them as the per-strategy defaults) and Manthan-
// specific portfolio sizing knobs (TotalCapital, MaxPositions, PerStockAmount).
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

	// MANTHAN-specific portfolio sizing
	TotalCapital   float64 `json:"total_capital" bson:"total_capital"`
	MaxPositions   int32   `json:"max_positions" bson:"max_positions"`
	PerStockAmount float64 `json:"per_stock_amount" bson:"per_stock_amount"`
}
