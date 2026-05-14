package dto

// CreateStrategyRequest represents the JSON body for creating a strategy
type CreateStrategyRequest struct {
	UserID              string              `json:"user_id"`
	StrategyName        string              `json:"strategy_name"`
	Description         string              `json:"description"`
	StrategyType        string              `json:"strategy_type"` // "NEWS", "52W_BREAKOUT", "MANTHAN", "HFT_BIDDING"
	Conditions          *StrategyConditions `json:"conditions"`
	TradeConfig         *TradeConfig        `json:"trade_config"`
	RiskLimits          *RiskLimits         `json:"risk_limits"`
	ActivateImmediately bool                `json:"activate_immediately"`
	TradingMode         string              `json:"trading_mode"` // "PAPER", "LIVE"
	HFTConfig           *HFTConfig          `json:"hft_config"`   // required for HFT_BIDDING
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
	HFTConfig    *HFTConfig          `json:"hft_config"`
	Version      int32               `json:"version"`
}

// HFTConfig represents the JSON HFT bidding strategy configuration. Mirrors
// the user_config.HFTConfig proto message; `mode` is not accepted from the
// client — the user-config service derives it from trading_mode.
type HFTConfig struct {
	Symbol              string  `json:"symbol"`
	ISIN                string  `json:"isin"`
	Exchange            string  `json:"exchange"`
	Side                string  `json:"side"`         // "BUY", "SELL", "BOTH"
	ProductType         string  `json:"product_type"` // "INTRADAY", "DELIVERY", "CASH"
	TickSize            float64 `json:"tick_size"`
	MaxBuyQty           int32   `json:"max_buy_qty"`
	MaxSellQty          int32   `json:"max_sell_qty"`
	SingleBuyQty        int32   `json:"single_buy_qty"`
	SingleSellQty       int32   `json:"single_sell_qty"`
	BuyLimitPrice       float64 `json:"buy_limit_price"`
	SellLimitPrice      float64 `json:"sell_limit_price"`
	WindowStart         string  `json:"window_start"` // "HH:MM" (optional)
	WindowEnd           string  `json:"window_end"`   // "HH:MM" (optional)
	ModifyOnPriceChange bool    `json:"modify_on_price_change"`
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
	OrderType       string  `json:"order_type"`    // "MARKET", "LIMIT"
	ProductType     string  `json:"product_type"`  // "INTRADAY", "DELIVERY", "BRACKET"
	Validity        string  `json:"validity"`
	Quantity        int32   `json:"quantity"`
	Exchange        string  `json:"exchange"`
	OrderSide       string  `json:"order_side"`    // "BUY", "SELL"
	StopLossPct     float64 `json:"stop_loss_pct"`
	TakeProfitPct   float64 `json:"take_profit_pct"`
	TrailingSLPct   float64 `json:"trailing_sl_pct"`
	StopLossType    string  `json:"stop_loss_type"` // "FIXED", "TRAILING"

	// 52W Breakout strategy fields (Manthan)
	PositionSizingMode string  `json:"position_sizing_mode"` // "FIXED_QTY", "EMA_ALLOCATION"
	TotalCapital       float64 `json:"total_capital"`        // Total investment (default 100000)
	MaxPositions       int32   `json:"max_positions"`        // Max positions (default 25)
	PerStockAmount     float64 `json:"per_stock_amount"`     // Auto: total_capital / max_positions
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
