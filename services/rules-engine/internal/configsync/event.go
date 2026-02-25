package configsync

// This package implements consuming User Config events from Kafka and applying
// them to the in-memory configstore.

// ConfigEventType must match services/user-config/internal/events/config_event.go
// (cross-service contract).
type ConfigEventType string

const (
	ConfigCreated ConfigEventType = "CONFIG_CREATED"
	ConfigUpdated ConfigEventType = "CONFIG_UPDATED"
	ConfigDeleted ConfigEventType = "CONFIG_DELETED"
	ConfigPaused  ConfigEventType = "CONFIG_PAUSED"
	ConfigResumed ConfigEventType = "CONFIG_RESUMED"
)

type ConfigEvent struct {
	Type       ConfigEventType  `json:"type"`
	UserID     string           `json:"user_id"`
	StrategyID string           `json:"strategy_id"`
	Version    uint64           `json:"version"`
	Timestamp  int64            `json:"timestamp"` // UnixNano
	Config     *StrategyPayload `json:"config"`    // nil for DELETED/PAUSED/RESUMED
}

type StrategyPayload struct {
	StrategyID   string             `json:"strategy_id"`
	UserID       string             `json:"user_id"`
	StrategyName string             `json:"strategy_name"`
	Active       bool               `json:"active"`
	TradingMode  string             `json:"trading_mode"`
	Conditions   ConditionsPayload  `json:"conditions"`
	TradeConfig  TradeConfigPayload `json:"trade_config"`
	RiskLimits   RiskLimitsPayload  `json:"risk_limits"`
	Version      uint64             `json:"version"`
	CreatedAt    int64              `json:"created_at"` // UnixNano
	UpdatedAt    int64              `json:"updated_at"` // UnixNano
}

type ConditionsPayload struct {
	MatchAllNews      bool     `json:"match_all_news"`
	ImpactScoreMin    int32    `json:"impact_score_min"`
	ImpactScoreMax    int32    `json:"impact_score_max"`
	Sentiments        []string `json:"sentiments"`
	Categories        []string `json:"categories"`
	StockCodes        []int64  `json:"stock_codes"`
	MarketCapTypes    []string `json:"market_cap_types"`
	MinMarketCap      float64  `json:"min_market_cap"`
	MaxMarketCap      float64  `json:"max_market_cap"`
	MinPriceChangePct float64  `json:"min_price_change_pct"`
	MaxPriceChangePct float64  `json:"max_price_change_pct"`
	MinVolume         int64    `json:"min_volume"`
	Exchanges         []string `json:"exchanges"`
	CreatedAt         int64    `json:"created_at"`
}

type TradeConfigPayload struct {
	OrderType     string  `json:"order_type"`
	ProductType   string  `json:"product_type"`
	Validity      string  `json:"validity"`
	Quantity      int32   `json:"quantity"`
	Exchange      string  `json:"exchange"`
	OrderSide     string  `json:"order_side"`
	LimitPrice    float64 `json:"limit_price"`
	StopLossPct   float64 `json:"stop_loss_pct"`
	TakeProfitPct float64 `json:"take_profit_pct"`
	TrailingSLPct float64 `json:"trailing_sl_pct"`
	StopLossType  string  `json:"stop_loss_type"`
	CreatedAt     int64   `json:"created_at"`
}

type RiskLimitsPayload struct {
	MaxDailyTrades          int32   `json:"max_daily_trades"`
	MaxPerTradeRisk         float64 `json:"max_per_trade_risk"`
	MaxPortfolioExposurePct float64 `json:"max_portfolio_exposure_pct"`
	MaxLossPerDay           float64 `json:"max_loss_per_day"`
	EnableRiskChecks        bool    `json:"enable_risk_checks"`
	EnableAutoSquareOff     bool    `json:"enable_auto_square_off"`
	AutoSquareOffTime       string  `json:"auto_square_off_time"`
	CreatedAt               int64   `json:"created_at"`
}
