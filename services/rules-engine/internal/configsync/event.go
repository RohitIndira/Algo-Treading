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

	// ProcessAfterMarketNews triggers an AMN backfill when this strategy is first created.
	ProcessAfterMarketNews bool `json:"process_after_market_news"`

	// AMNSelectedStocks restricts the AMN backfill to these ISINs (user's preview
	// pick). Empty → backfill places all affordable matches up to the trade cap.
	AMNSelectedStocks []string `json:"amn_selected_stocks,omitempty"`
}

type ConditionsPayload struct {
	MatchAllNews      bool     `json:"match_all_news"`
	ImpactScoreMin    int32    `json:"impact_score_min"`
	ImpactScoreMax    int32    `json:"impact_score_max"`
	Sentiments        []string `json:"sentiments"`
	Categories        []string `json:"categories"`
	MarketCapTypes    []string `json:"market_cap_types"`
	MinMarketCap      float64  `json:"min_market_cap"`
	MaxMarketCap      float64  `json:"max_market_cap"`
	MinPriceChangePct float64  `json:"min_price_change_pct"`
	MaxPriceChangePct float64  `json:"max_price_change_pct"`
	Exchanges         []string `json:"exchanges"`
	CreatedAt         int64    `json:"created_at"`
}

// MultiLevelExitLevelPayload is the Kafka-serialised form of one exit level.
// Mirrors user-config/internal/events/config_event.go (cross-service contract).
type MultiLevelExitLevelPayload struct {
	LevelNum int     `json:"level_num"`
	PricePct float64 `json:"price_pct"`
	QtyPct   float64 `json:"qty_pct"`
}

type TradeConfigPayload struct {
	OrderType      string  `json:"order_type"`
	ProductType    string  `json:"product_type"`
	Validity       string  `json:"validity"`
	Quantity       int32   `json:"quantity"`
	Exchange       string  `json:"exchange"`
	OrderSide      string  `json:"order_side"`
	LimitPrice     float64 `json:"limit_price"`
	StopLossPct    float64 `json:"stop_loss_pct"`
	TakeProfitPct  float64 `json:"take_profit_pct"`
	TrailingSLPct  float64 `json:"trailing_sl_pct"`
	StopLossType   string  `json:"stop_loss_type"`   // "FIXED" | "TRAILING" | "MULTI_LEVEL"
	TakeProfitType string  `json:"take_profit_type"` // "FIXED" | "MULTI_LEVEL"

	// Multi-level exit levels (non-nil only when respective type == "MULTI_LEVEL")
	MultiLevelSL []MultiLevelExitLevelPayload `json:"multi_level_sl,omitempty"`
	MultiLevelTP []MultiLevelExitLevelPayload `json:"multi_level_tp,omitempty"`

	// Trade window: orders only fire when IST time is within [start, end].
	// HH:MM 24-hour format. Empty = no restriction.
	TradeWindowStart string `json:"trade_window_start"`
	TradeWindowEnd   string `json:"trade_window_end"`
	CreatedAt        int64  `json:"created_at"`
}

type RiskLimitsPayload struct {
	MaxDailyTrades          int32   `json:"max_daily_trades"`
	MaxPerTradeRisk         float64 `json:"max_per_trade_risk"`
	MaxPortfolioExposurePct float64 `json:"max_portfolio_exposure_pct"`
	MaxLossPerDay           float64 `json:"max_loss_per_day"`
	EnableRiskChecks        bool    `json:"enable_risk_checks"`
	EnableAutoSquareOff     bool    `json:"enable_auto_square_off"`
	AutoSquareOffTime       string  `json:"auto_square_off_time"`
	MaxAmountPerStock       float64 `json:"max_amount_per_stock"`
	MaxTradesPerStrategy    int32   `json:"max_trades_per_strategy"`
	CreatedAt               int64   `json:"created_at"`
}
