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

// StrategyType represents the type of strategy
type StrategyType string

const (
	StrategyTypeNews        StrategyType = "NEWS"
	StrategyType52WBreakout StrategyType = "52W_BREAKOUT"
	StrategyTypeManthan     StrategyType = "MANTHAN"
)

// Strategy represents a user trading strategy
type Strategy struct {
	StrategyID   uuid.UUID   `db:"strategy_id" json:"strategy_id"`
	UserID       string      `db:"user_id" json:"user_id"`
	StrategyName string       `db:"strategy_name" json:"strategy_name"`
	Description  string       `db:"description" json:"description"`
	Active       bool         `db:"active" json:"active"`
	StrategyType StrategyType `db:"strategy_type" json:"strategy_type"`
	TradingMode  TradingMode  `db:"trading_mode" json:"trading_mode"`
	Version      int32       `db:"version" json:"version"`
	CreatedAt    time.Time   `db:"created_at" json:"created_at"`
	UpdatedAt    time.Time   `db:"updated_at" json:"updated_at"`
	DeletedAt    *time.Time  `db:"deleted_at" json:"deleted_at,omitempty"`
	// StoppedAt is set when the user hits STOP (terminal transition).
	// Row stays visible in reads with status=STOPPED; RESUME rejects
	// any row where this is non-null. See migrations/015_add_stopped_at.sql.
	StoppedAt    *time.Time  `db:"stopped_at" json:"stopped_at,omitempty"`

	// These are now separate tables, so we use pointers and `db:"-"` or handle in repo
	Conditions   *StrategyCondition `db:"-" json:"conditions,omitempty"`
	TradeConfig  *TradeConfig       `db:"-" json:"trade_config,omitempty"`
	RiskLimits   *RiskLimits        `db:"-" json:"risk_limits,omitempty"`
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
	StockCodes         pq.Int64Array  `db:"stock_codes" json:"stock_codes"`
	MinMarketCap       *float64       `db:"min_market_cap" json:"min_market_cap,omitempty"`
	MaxMarketCap       *float64       `db:"max_market_cap" json:"max_market_cap,omitempty"`
	MarketCapTypes     pq.StringArray `db:"market_cap_types" json:"market_cap_types"`
	MinPriceChangePct  *float64       `db:"min_price_change_pct" json:"min_price_change_pct,omitempty"`
	MaxPriceChangePct  *float64       `db:"max_price_change_pct" json:"max_price_change_pct,omitempty"`
	MinVolume          *int64         `db:"min_volume" json:"min_volume,omitempty"`
	Exchanges          pq.StringArray `db:"exchanges" json:"exchanges"`
	CreatedAt          time.Time      `db:"created_at" json:"created_at"`
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
	StopLossType    string    `db:"stop_loss_type" json:"stop_loss_type"`

	// 52W Breakout strategy fields (Manthan)
	PositionSizingMode string   `db:"position_sizing_mode" json:"position_sizing_mode"` // FIXED_QTY or EMA_ALLOCATION
	TotalCapital       *float64 `db:"total_capital" json:"total_capital,omitempty"`      // Total investment (default ₹1,00,000)
	MaxPositions       *int32   `db:"max_positions" json:"max_positions,omitempty"`      // Max stocks to hold (default 25)
	PerStockAmount     *float64 `db:"per_stock_amount" json:"per_stock_amount,omitempty"` // Auto: total_capital / max_positions

	CreatedAt       time.Time `db:"created_at" json:"created_at"`
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
	CreatedAt               time.Time `db:"created_at" json:"created_at"`
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
	StrategyType        StrategyType       `json:"strategy_type"` // NEWS or 52W_BREAKOUT
	Conditions          *StrategyCondition `json:"conditions"`    // Required for NEWS, optional for 52W
	TradeConfig         *TradeConfig       `json:"trade_config" validate:"required"`
	RiskLimits          *RiskLimits        `json:"risk_limits"`
	ActivateImmediately bool               `json:"activate_immediately"`
	TradingMode         TradingMode        `json:"trading_mode"`

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
}
