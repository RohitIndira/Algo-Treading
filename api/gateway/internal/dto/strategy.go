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
	// ProcessAfterMarketNews triggers a one-shot historical news backfill when this
	// strategy is first created. The rules-engine scans news from the previous
	// NSE_EQ trading day's 15:31 IST close until now and places orders for matches,
	// tagged signal_source="BACKFILL_AMN".
	ProcessAfterMarketNews bool `json:"process_after_market_news"`

	// AMNSelectedStocks is the user's explicit pick (ISINs) from the AMN preview.
	// When non-empty, the backfill places orders ONLY for these stocks; when empty
	// it falls back to all affordable matches up to the trade cap.
	AMNSelectedStocks []string `json:"amn_selected_stocks,omitempty"`

	// AMNSelection is the richer per-stock AMN preview pick (bucket + pricing). It
	// is persisted as the day-1 activation record; the backfill's ISIN filter is
	// derived from it. Preferred over AMNSelectedStocks when both are present.
	AMNSelection []AMNSelectedStock `json:"amn_selection,omitempty"`
}

// AMNSelectedStock is one stock the user picked in the AMN preview, carrying the
// preview-time bucket + pricing so 'monitor' (price-watch) picks are persisted too.
type AMNSelectedStock struct {
	ISIN           string  `json:"isin"`
	Symbol         string  `json:"symbol"`
	NSECode        int64   `json:"nse_code"`
	Bucket         string  `json:"bucket"` // "place" | "monitor"
	TargetPrice    float64 `json:"target_price"`
	EntryPrice     float64 `json:"entry_price"`
	Quantity       int32   `json:"quantity"`
	InvestedAmount float64 `json:"invested_amount"`
}

// ActivateStrategyRequest is the JSON body for POST /strategies/{id}/activate.
// For AMN strategies AMNSelection is required (fresh preview pick submitted on the
// reactivation popup); it is ignored for non-AMN strategies.
type ActivateStrategyRequest struct {
	UserID       string             `json:"user_id"`
	AMNSelection []AMNSelectedStock `json:"amn_selection,omitempty"`
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
	MarketCapTypes    []string `json:"market_cap_types"` // "SMALL", "MID", "LARGE"
	MinMarketCap      float64  `json:"min_market_cap"`
	MaxMarketCap      float64  `json:"max_market_cap"`
	MinPriceChangePct float64  `json:"min_price_change_pct"`
	MaxPriceChangePct float64  `json:"max_price_change_pct"`
	// Trade value (turnover = day volume × current LTP) filter, in ₹ crore.
	// TradeValueMode: "" = off | "ABOVE" (uses min) | "BELOW" (uses max) |
	// "RANGE" (uses both). Validated by validateConditions.
	TradeValueMode    string   `json:"trade_value_mode"`
	MinTradeValue     float64  `json:"min_trade_value"`
	MaxTradeValue     float64  `json:"max_trade_value"`
	Exchanges         []string `json:"exchanges"`
}

// MultiLevelExitLevel represents one partial-exit level for multi-level SL or TP
type MultiLevelExitLevel struct {
	LevelNum int     `json:"level_num"` // 1..5, sequential
	PricePct float64 `json:"price_pct"` // % from entry, always positive, strictly increasing
	QtyPct   float64 `json:"qty_pct"`   // % of total qty to exit; all levels must sum to 100
}

// TradeConfig represents JSON trade configuration
type TradeConfig struct {
	OrderType      string                `json:"order_type"`       // "MARKET", "LIMIT"
	ProductType    string                `json:"product_type"`     // "INTRADAY", "DELIVERY", "BRACKET"
	Validity       string                `json:"validity"`
	Quantity       int32                 `json:"quantity"`
	Exchange       string                `json:"exchange"`
	OrderSide      string                `json:"order_side"`       // "BUY", "SELL"
	StopLossPct    float64               `json:"stop_loss_pct"`
	TakeProfitPct  float64               `json:"take_profit_pct"`
	TrailingSLPct  float64               `json:"trailing_sl_pct"`
	StopLossType   string                `json:"stop_loss_type"`   // "FIXED", "TRAILING", "MULTI_LEVEL"
	TakeProfitType string                `json:"take_profit_type"` // "FIXED", "MULTI_LEVEL"
	MultiLevelSL   []MultiLevelExitLevel `json:"multi_level_sl,omitempty"`
	MultiLevelTP   []MultiLevelExitLevel `json:"multi_level_tp,omitempty"`
	// Trade window: orders only placed when IST time is within [start, end].
	// HH:MM 24-hour format (e.g. "09:15", "15:00"). Empty = no restriction.
	TradeWindowStart string `json:"trade_window_start"`
	TradeWindowEnd   string `json:"trade_window_end"`
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
	// Maximum investment amount per stock per order (₹). 0 = no limit.
	MaxAmountPerStock float64 `json:"max_amount_per_stock"`
	// Maximum number of trades this strategy may execute per day. 0 = no limit.
	MaxTradesPerStrategy int32 `json:"max_trades_per_strategy"`
}
