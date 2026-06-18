package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// TradingMode represents the trading mode (PAPER or LIVE)
type TradingMode string

const (
	TradingModePaper TradingMode = "PAPER"
	TradingModeLive  TradingMode = "LIVE"
)

// Strategy represents a user trading strategy
type Strategy struct {
	StrategyID   uuid.UUID   `db:"strategy_id" json:"strategy_id"`
	UserID       string      `db:"user_id" json:"user_id"`
	StrategyName string      `db:"strategy_name" json:"strategy_name"`
	Description  string      `db:"description" json:"description"`
	Active       bool        `db:"active" json:"active"`
	TradingMode  TradingMode `db:"trading_mode" json:"trading_mode"`
	Version      int32       `db:"version" json:"version"`
	CreatedAt    time.Time   `db:"created_at" json:"created_at"`
	UpdatedAt    time.Time   `db:"updated_at" json:"updated_at"`
	DeletedAt    *time.Time  `db:"deleted_at" json:"deleted_at,omitempty"`

	// These are now separate tables, so we use pointers and `db:"-"` or handle in repo
	Conditions  *StrategyCondition `db:"-" json:"conditions,omitempty"`
	TradeConfig *TradeConfig       `db:"-" json:"trade_config,omitempty"`
	RiskLimits  *RiskLimits        `db:"-" json:"risk_limits,omitempty"`

	// ProcessAfterMarketNews is a creation-time trigger flag. It is NOT stored in
	// the strategies table (db:"-") — it is forwarded through the outbox JSON so
	// the rules-engine Kafka consumer can initiate the AMN backfill exactly once.
	ProcessAfterMarketNews bool `db:"-" json:"process_after_market_news,omitempty"`

	// AMNSelectedStocks is the user's explicit AMN preview pick (ISINs). Like
	// ProcessAfterMarketNews it is creation-time only and NOT persisted (db:"-");
	// it is forwarded through the outbox JSON so the rules-engine backfill places
	// orders only for these stocks. Empty → backfill places all affordable matches.
	AMNSelectedStocks []string `db:"-" json:"amn_selected_stocks,omitempty"`
}

// StrategyCondition represents the conditions for triggering a strategy
type StrategyCondition struct {
	ConditionID        uuid.UUID      `db:"condition_id" json:"condition_id"`
	StrategyID         uuid.UUID      `db:"strategy_id" json:"strategy_id"`
	MatchAllNews       bool           `db:"match_all_news" json:"match_all_news"`
	ImpactScoreMin     int32          `db:"impact_score_min" json:"impact_score_min"`
	ImpactScoreMax     int32          `db:"impact_score_max" json:"impact_score_max"`
	Sentiments         pq.StringArray `db:"sentiments" json:"sentiments"`
	Categories         pq.StringArray `db:"news_categories" json:"categories"`
	MinMarketCap       *float64       `db:"min_market_cap" json:"min_market_cap,omitempty"`
	MaxMarketCap       *float64       `db:"max_market_cap" json:"max_market_cap,omitempty"`
	MarketCapTypes     pq.StringArray `db:"market_cap_types" json:"market_cap_types"`
	MinPriceChangePct  *float64       `db:"min_price_change_pct" json:"min_price_change_pct,omitempty"`
	MaxPriceChangePct  *float64       `db:"max_price_change_pct" json:"max_price_change_pct,omitempty"`
	Exchanges          pq.StringArray `db:"exchanges" json:"exchanges"`
	CreatedAt          time.Time      `db:"created_at" json:"created_at"`
}

// SLMode / TPMode constants — used in StopLossType and TakeProfitType fields.
const (
	SLModeFixed      = "FIXED"       // Single SL at a fixed percentage
	SLModeTrailing   = "TRAILING"    // Trailing SL that moves with price
	SLModeMultiLevel = "MULTI_LEVEL" // Up to 5 partial exits at different SL levels
	TPModeFixed      = "FIXED"       // Single TP at a fixed percentage
	TPModeMultiLevel = "MULTI_LEVEL" // Up to 5 partial exits at different TP levels
)

// MaxMultiLevelExits is the hard cap on the number of levels allowed for both SL and TP.
const MaxMultiLevelExits = 5

// MultiLevelExitLevel defines one partial exit level for multi-level SL or TP.
//
// price_pct: percentage distance from entry price — always positive.
//   - For SL (BUY): trigger when LTP ≤ entry*(1 − price_pct/100)
//   - For SL (SELL): trigger when LTP ≥ entry*(1 + price_pct/100)
//   - For TP (BUY): trigger when LTP ≥ entry*(1 + price_pct/100)
//   - For TP (SELL): trigger when LTP ≤ entry*(1 − price_pct/100)
//
// qty_pct: percentage of the TOTAL position quantity to exit at this level (0..100).
// All levels for a given exit type must sum to exactly 100.
type MultiLevelExitLevel struct {
	LevelNum int     `json:"level_num"` // 1..5, must be sequential
	PricePct float64 `json:"price_pct"` // % from entry, always positive, strictly increasing
	QtyPct   float64 `json:"qty_pct"`   // % of total qty to exit here; all levels sum to 100
}

// TradeConfig represents the trade execution configuration
type TradeConfig struct {
	TradeConfigID   uuid.UUID `db:"trade_config_id" json:"trade_config_id"`
	StrategyID      uuid.UUID `db:"strategy_id" json:"strategy_id"`
	OrderType       string    `db:"order_type" json:"order_type"`
	ProductType     string    `db:"product_type" json:"product_type"`
	Validity        string    `db:"validity" json:"validity"`
	Quantity        int32     `db:"quantity" json:"quantity"`
	Exchange        string    `db:"exchange" json:"exchange"`
	OrderSide       string    `db:"order_side" json:"order_side"`
	MaxPositionSize *float64  `db:"max_position_size" json:"max_position_size,omitempty"`
	LimitPrice      *float64  `db:"limit_price" json:"limit_price,omitempty"`
	StopLossPct     *float64  `db:"stop_loss_pct" json:"stop_loss_pct,omitempty"`
	TakeProfitPct   *float64  `db:"take_profit_pct" json:"take_profit_pct,omitempty"`
	TrailingSLPct   *float64  `db:"trailing_sl_pct" json:"trailing_sl_pct,omitempty"`

	// StopLossType: "FIXED" | "TRAILING" | "MULTI_LEVEL"
	StopLossType string `db:"stop_loss_type" json:"stop_loss_type"`

	// TakeProfitType: "FIXED" | "MULTI_LEVEL"
	// Defaults to "FIXED" for backward compatibility.
	TakeProfitType string `db:"take_profit_type" json:"take_profit_type"`

	// MultiLevelSL holds up to 5 partial SL exit levels.
	// Only used when StopLossType == "MULTI_LEVEL". Stored as JSONB.
	MultiLevelSL []MultiLevelExitLevel `db:"-" json:"multi_level_sl,omitempty"`

	// MultiLevelTP holds up to 5 partial TP exit levels.
	// Only used when TakeProfitType == "MULTI_LEVEL". Stored as JSONB.
	MultiLevelTP []MultiLevelExitLevel `db:"-" json:"multi_level_tp,omitempty"`

	// TradeWindowStart / TradeWindowEnd define the IST time range (HH:MM, 24h)
	// within which this strategy is allowed to place orders.
	// Empty string means no restriction. Valid range: "09:15" to "15:00".
	TradeWindowStart string `db:"trade_window_start" json:"trade_window_start"`
	TradeWindowEnd   string `db:"trade_window_end" json:"trade_window_end"`

	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// RiskLimits represents risk management limits
type RiskLimits struct {
	RiskLimitID             uuid.UUID `db:"risk_limit_id" json:"risk_limit_id"`
	StrategyID              uuid.UUID `db:"strategy_id" json:"strategy_id"`
	MaxDailyTrades          *int32    `db:"max_daily_trades" json:"max_daily_trades,omitempty"`
	MaxPerTradeRisk         *float64  `db:"max_per_trade_risk" json:"max_per_trade_risk,omitempty"`
	MaxPortfolioExposurePct *float64  `db:"max_portfolio_exposure_pct" json:"max_portfolio_exposure_pct,omitempty"`
	MaxLossPerDay           *float64  `db:"max_loss_per_day" json:"max_loss_per_day,omitempty"`
	EnableRiskChecks        bool      `db:"enable_risk_checks" json:"enable_risk_checks"`
	EnableAutoSquareOff     bool      `db:"enable_auto_square_off" json:"enable_auto_square_off"`
	AutoSquareOffTime       string    `db:"auto_square_off_time" json:"auto_square_off_time"`
	// MaxAmountPerStock caps total order value (quantity × price) per stock. NULL = no limit.
	MaxAmountPerStock    *float64 `db:"max_amount_per_stock" json:"max_amount_per_stock,omitempty"`
	// MaxTradesPerStrategy caps how many trades this strategy may fire per day. NULL = no limit.
	MaxTradesPerStrategy *int32   `db:"max_trades_per_strategy" json:"max_trades_per_strategy,omitempty"`
	CreatedAt            time.Time `db:"created_at" json:"created_at"`
}

// ExecutionOutbox represents the outbox table for transactional messaging
type ExecutionOutbox struct {
	ID          int64     `db:"id" json:"id"`
	AggregateID uuid.UUID `db:"aggregate_id" json:"aggregate_id"`
	EventType   string    `db:"event_type" json:"event_type"`
	Payload     []byte    `db:"payload" json:"payload"` // Raw JSON
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
	Processed   bool      `db:"processed" json:"processed"`
}

// CreateStrategyRequest represents a request to create a new strategy
type CreateStrategyRequest struct {
	UserID              string             `json:"user_id" validate:"required"`
	StrategyName        string             `json:"strategy_name" validate:"required"`
	Description         string             `json:"description"`
	Conditions          *StrategyCondition `json:"conditions" validate:"required"`
	TradeConfig         *TradeConfig       `json:"trade_config" validate:"required"`
	RiskLimits          *RiskLimits        `json:"risk_limits" validate:"required"`
	ActivateImmediately bool               `json:"activate_immediately"`
	TradingMode         TradingMode        `json:"trading_mode"`
	ProcessAfterMarketNews bool            `json:"process_after_market_news"`
	AMNSelectedStocks   []string           `json:"amn_selected_stocks,omitempty"`

	// Frontend authentication data
	IndiraAuth *IndiraAuthContext `json:"indira_auth,omitempty"`
}

// IndiraAuthContext stores auth details
type IndiraAuthContext struct {
	UserID      string `json:"user_id"`
	AppID       string `json:"app_id"`
	Source      string `json:"source"`
	BearerToken string `json:"bearer_token"`
}

// UpdateStrategyRequest represents a request to update a strategy
type UpdateStrategyRequest struct {
	StrategyID   uuid.UUID          `json:"strategy_id" validate:"required"`
	UserID       string             `json:"user_id" validate:"required"`
	StrategyName *string            `json:"strategy_name,omitempty"`
	Description  *string            `json:"description,omitempty"`
	Conditions   *StrategyCondition `json:"conditions,omitempty"`
	TradeConfig  *TradeConfig       `json:"trade_config,omitempty"`
	RiskLimits   *RiskLimits        `json:"risk_limits,omitempty"`
	TradingMode  *TradingMode       `json:"trading_mode,omitempty"`
	Version      int32              `json:"version" validate:"required"`

	// IndiraAuth refreshes the broker credentials for this user.
	// Pass this whenever the Indira bearer token is renewed (tokens expire daily).
	IndiraAuth *IndiraAuthContext `json:"indira_auth,omitempty"`
}
