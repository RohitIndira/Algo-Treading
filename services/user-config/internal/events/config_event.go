package events

// ConfigEventType is the event type published to Kafka.
// These strings MUST match exactly what Rule Engine expects.
type ConfigEventType string

const (
	ConfigCreated ConfigEventType = "CONFIG_CREATED"
	ConfigUpdated ConfigEventType = "CONFIG_UPDATED"
	ConfigDeleted ConfigEventType = "CONFIG_DELETED"
	ConfigPaused  ConfigEventType = "CONFIG_PAUSED"
	ConfigResumed ConfigEventType = "CONFIG_RESUMED"
)

// ConfigEvent is the Kafka message payload published by User Config Service.
// IMPORTANT: This defines the cross-service contract.
type ConfigEvent struct {
	Type       ConfigEventType  `json:"type"`
	UserID     string           `json:"user_id"`
	StrategyID string           `json:"strategy_id"`
	Version    uint64           `json:"version"`
	Timestamp  int64            `json:"timestamp"` // UnixNano
	Config     *StrategyPayload `json:"config"`    // nil for DELETED/PAUSED/RESUMED

	// PositionHandling — how the caller wanted open positions handled
	// on this lifecycle event. Only carries meaning on Paused / Deleted;
	// empty on Created / Updated / Resumed. Set by user-config from the
	// gRPC request's position_handling field.
	//
	// Values (mirrors api/proto/user_config PositionHandling enum):
	//
	//   ""                        legacy — no explicit handling, e.g.
	//                             EOD auto-deactivate. trade-execution
	//                             consumer runs the classic
	//                             closeStrategyPositions cleanup.
	//   "KEEP_POSITIONS_OPEN"     user chose to leave lots on the book.
	//                             trade-execution consumer SKIPS position
	//                             close so SLs keep protecting fills.
	//   "SQUARE_OFF_AT_MARKET"    user-config already called
	//                             trade-execution's force-exit endpoint
	//                             synchronously (the exit orders are
	//                             SUBMITTED at the broker). The consumer
	//                             MUST SKIP closeStrategyPositions or it
	//                             would immediately CancelOrder those
	//                             same exits before they fill.
	PositionHandling string `json:"position_handling,omitempty"`
}

// StrategyPayload is the full strategy configuration.
// Values are published exactly as stored in the User Config DB model (no enum conversion here).
type StrategyPayload struct {
	StrategyID   string              `json:"strategy_id"`
	UserID       string              `json:"user_id"`
	StrategyName string              `json:"strategy_name"`
	StrategyType string              `json:"strategy_type,omitempty"`
	Active       bool                `json:"active"`
	TradingMode  string              `json:"trading_mode"`
	Conditions   *ConditionsPayload  `json:"conditions,omitempty"`
	TradeConfig  TradeConfigPayload  `json:"trade_config"`
	RiskLimits   *RiskLimitsPayload  `json:"risk_limits,omitempty"`
	Version      uint64              `json:"version"`
	CreatedAt    int64               `json:"created_at,omitempty"` // UnixNano
	UpdatedAt    int64               `json:"updated_at,omitempty"` // UnixNano
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
	CreatedAt         int64    `json:"created_at"` // UnixNano
}

type TradeConfigPayload struct {
	OrderType      string  `json:"order_type,omitempty"`
	ProductType    string  `json:"product_type,omitempty"`
	Validity       string  `json:"validity,omitempty"`
	Quantity       int32   `json:"quantity,omitempty"`
	Exchange       string  `json:"exchange,omitempty"`
	OrderSide      string  `json:"order_side,omitempty"`
	LimitPrice     float64 `json:"limit_price,omitempty"`
	StopLossPct    float64 `json:"stop_loss_pct,omitempty"`
	TakeProfitPct  float64 `json:"take_profit_pct,omitempty"`
	TrailingSLPct  float64 `json:"trailing_sl_pct,omitempty"`
	StopLossType   string  `json:"stop_loss_type,omitempty"`
	TotalCapital   float64 `json:"total_capital,omitempty"`
	MaxPositions   int32   `json:"max_positions,omitempty"`
	PerStockAmount float64 `json:"per_stock_amount,omitempty"`
	CreatedAt      int64   `json:"created_at,omitempty"` // UnixNano
}

type RiskLimitsPayload struct {
	MaxDailyTrades          int32   `json:"max_daily_trades"`
	MaxPerTradeRisk         float64 `json:"max_per_trade_risk"`
	MaxPortfolioExposurePct float64 `json:"max_portfolio_exposure_pct"`
	MaxLossPerDay           float64 `json:"max_loss_per_day"`
	EnableRiskChecks        bool    `json:"enable_risk_checks"`
	EnableAutoSquareOff     bool    `json:"enable_auto_square_off"`
	AutoSquareOffTime       string  `json:"auto_square_off_time"`
	CreatedAt               int64   `json:"created_at"` // UnixNano
}
