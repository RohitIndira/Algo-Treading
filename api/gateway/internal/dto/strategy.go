package dto

// CreateStrategyRequest represents the JSON body for creating a strategy
type CreateStrategyRequest struct {
	UserID              string              `json:"user_id"`
	StrategyName        string              `json:"strategy_name"`
	Description         string              `json:"description"`
	Conditions          *StrategyConditions `json:"conditions"`
	TradeConfig         *TradeConfig        `json:"trade_config"`
	RiskLimits          *RiskLimits         `json:"risk_limits"`
	ActivateImmediately bool                `json:"activate_immediately"`
	TradingMode         string              `json:"trading_mode"` // "PAPER", "LIVE"
}

// UpdateStrategyRequest represents the JSON body for updating a strategy
type UpdateStrategyRequest struct {
	UserID       string              `json:"user_id"`
	StrategyName *string             `json:"strategy_name"`
	Description  *string             `json:"description"`
	Conditions   *StrategyConditions `json:"conditions"`
	TradeConfig  *TradeConfig        `json:"trade_config"`
	RiskLimits   *RiskLimits         `json:"risk_limits"`
	TradingMode  *string             `json:"trading_mode"`
	Version      int32               `json:"version"`
}

// StrategyConditions represents JSON conditions
type StrategyConditions struct {
	MatchAllNews      bool     `json:"match_all_news"`
	ImpactScoreMin    int32    `json:"impact_score_min"`
	ImpactScoreMax    int32    `json:"impact_score_max"`
	Sentiments        []string `json:"sentiments"`
	Categories        []string `json:"categories"`
	StockCodes        []int64  `json:"stock_codes"`
	MarketCapTypes    []string `json:"market_cap_types"` // "SMALL", "MID", "LARGE"
	MinMarketCap      float64  `json:"min_market_cap"`
	MaxMarketCap      float64  `json:"max_market_cap"`
	MinPriceChangePct float64  `json:"min_price_change_pct"`
	MaxPriceChangePct float64  `json:"max_price_change_pct"`
	MinVolume         int64    `json:"min_volume"`
	Exchanges         []string `json:"exchanges"`
}

// TradeConfig represents JSON trade configuration
type TradeConfig struct {
	OrderType       string  `json:"order_type"` // "MARKET", "LIMIT"
	ProductType     string  `json:"product_type"` // "INTRADAY", "DELIVERY"
	Validity        string  `json:"validity"`
	Quantity        int32   `json:"quantity"`
	Exchange        string  `json:"exchange"`
	OrderSide       string  `json:"order_side"` // "BUY", "SELL"
	StopLossPct     float64 `json:"stop_loss_pct"`
	TakeProfitPct   float64 `json:"take_profit_pct"`
	TrailingSLPct   float64 `json:"trailing_sl_pct"`
	StopLossType    string  `json:"stop_loss_type"` // "FIXED", "TRAILING"
}

// RiskLimits represents JSON risk limits
type RiskLimits struct {
	MaxDailyTrades          int32   `json:"max_daily_trades"`
	MaxPerTradeRisk         float64 `json:"max_per_trade_risk"`
	MaxPortfolioExposurePct float64 `json:"max_portfolio_exposure_pct"`
	MaxLossPerDay           float64 `json:"max_loss_per_day"`
	EnableRiskChecks        bool    `json:"enable_risk_checks"`
	EnableAutoSquareOff     bool    `json:"enable_auto_square_off"`
	AutoSquareOffTime       string  `json:"auto_square_off_time"`
	PositionSizing          string  `json:"position_sizing"` // "FIXED", "PERCENTAGE", "RISK_BASED"
}
