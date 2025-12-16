package models

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Strategy represents a user trading strategy
type Strategy struct {
	StrategyID   uuid.UUID          `db:"strategy_id" json:"strategy_id"`
	UserID       string             `db:"user_id" json:"user_id"`
	StrategyName string             `db:"strategy_name" json:"strategy_name"`
	Description  string             `db:"description" json:"description"`
	Active       bool               `db:"active" json:"active"`
	MatchAllNews bool               `db:"match_all_news" json:"match_all_news"`
	Version      int32              `db:"version" json:"version"`
	CreatedAt    time.Time          `db:"created_at" json:"created_at"`
	UpdatedAt    time.Time          `db:"updated_at" json:"updated_at"`
	Conditions   *StrategyCondition `json:"conditions,omitempty"`
	TradeConfig  *TradeConfig       `json:"trade_config,omitempty"`
	RiskLimits   *RiskLimits        `json:"risk_limits,omitempty"`

	// Frontend authentication data (stored with strategy for order execution)
	BearerToken string `db:"bearer_token" json:"bearer_token,omitempty"` // JWT bearer token
	AppId       string `db:"app_id" json:"app_id,omitempty"`             // Application ID
	Source      string `db:"source" json:"source,omitempty"`             // Source platform
}

// StrategyCondition represents the conditions for triggering a strategy
type StrategyCondition struct {
	ConditionID          uuid.UUID      `db:"condition_id" json:"condition_id"`
	StrategyID           uuid.UUID      `db:"strategy_id" json:"strategy_id"`
	ImpactScoreThreshold int32          `db:"impact_score_threshold" json:"impact_score_threshold"`
	Sentiments           pq.StringArray `db:"sentiments" json:"sentiments"`
	Categories           pq.StringArray `db:"categories" json:"categories"`
	StockCodes           pq.Int64Array  `db:"stock_codes" json:"stock_codes"`
	PriceRangeMin        *float64       `db:"price_range_min" json:"price_range_min,omitempty"`
	PriceRangeMax        *float64       `db:"price_range_max" json:"price_range_max,omitempty"`
	VolumeThreshold      *int64         `db:"volume_threshold" json:"volume_threshold,omitempty"`
	PctChangeThreshold   *float64       `db:"pct_change_threshold" json:"pct_change_threshold,omitempty"`
	Exchanges            pq.StringArray `db:"exchanges" json:"exchanges"`
	MinMarketCap         *float64       `db:"min_market_cap" json:"min_market_cap,omitempty"` // Market cap filter in crores
	MaxMarketCap         *float64       `db:"max_market_cap" json:"max_market_cap,omitempty"` // Market cap filter in crores
	CreatedAt            time.Time      `db:"created_at" json:"created_at"`
}

// TradeConfig represents the trade execution configuration
type TradeConfig struct {
	TradeConfigID   uuid.UUID `db:"trade_config_id" json:"trade_config_id"`
	StrategyID      uuid.UUID `db:"strategy_id" json:"strategy_id"`
	OrderType       string    `db:"order_type" json:"order_type"`
	Quantity        int32     `db:"quantity" json:"quantity"`
	MaxPositionSize *float64  `db:"max_position_size" json:"max_position_size,omitempty"`
	StopLossPct     *float64  `db:"stop_loss_pct" json:"stop_loss_pct,omitempty"`
	TakeProfitPct   *float64  `db:"take_profit_pct" json:"take_profit_pct,omitempty"`
	Exchange        string    `db:"exchange" json:"exchange"`
	OrderSide       string    `db:"order_side" json:"order_side"`
	LimitPrice      *float64  `db:"limit_price" json:"limit_price,omitempty"`
	Validity        string    `db:"validity" json:"validity"`
	StopLossType    string    `db:"stop_loss_type" json:"stop_loss_type"`             // FIXED or TRAILING
	TrailingSLPct   *float64  `db:"trailing_sl_pct" json:"trailing_sl_pct,omitempty"` // Trailing SL percentage
	ProductType     string    `db:"product_type" json:"product_type"`                 // INTRADAY, DELIVERY, CASH
	CreatedAt       time.Time `db:"created_at" json:"created_at"`
}

// RiskLimits represents risk management limits
type RiskLimits struct {
	RiskLimitID             uuid.UUID `db:"risk_limit_id" json:"risk_limit_id"`
	StrategyID              uuid.UUID `db:"strategy_id" json:"strategy_id"`
	MaxDailyTrades          *int32    `db:"max_daily_trades" json:"max_daily_trades,omitempty"`
	MaxLossPerDay           *float64  `db:"max_loss_per_day" json:"max_loss_per_day,omitempty"`
	PositionSizing          string    `db:"position_sizing" json:"position_sizing"`
	MaxPortfolioExposurePct *float64  `db:"max_portfolio_exposure_pct" json:"max_portfolio_exposure_pct,omitempty"`
	MaxPerTradeRisk         *float64  `db:"max_per_trade_risk" json:"max_per_trade_risk,omitempty"`
	EnableRiskChecks        bool      `db:"enable_risk_checks" json:"enable_risk_checks"`
	EnableAutoSquareOff     bool      `db:"enable_auto_square_off" json:"enable_auto_square_off"` // Auto square-off at market close
	AutoSquareOffTime       string    `db:"auto_square_off_time" json:"auto_square_off_time"`     // Time in HH:MM format (e.g., "15:05")
	CreatedAt               time.Time `db:"created_at" json:"created_at"`
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

	// Frontend authentication data (from HTTP headers, stored with strategy)
	BearerToken string `json:"bearer_token,omitempty"`
	AppId       string `json:"app_id,omitempty"`
	Source      string `json:"source,omitempty"`

	IndiraUserID  string `json:"indira_user_id"`  // e.g., "ISPL19122"
    IndiraAppID   string `json:"indira_app_id"`   // Application ID from frontend
    IndiraSource  string `json:"indira_source"`   // "IOS", "AND", or "WEB"
    IndiraToken   string `json:"indira_token"`    // JWT bearer token
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
	Version      int32              `json:"version" validate:"required"`
}

// Value implements the driver.Valuer interface for JSON marshaling
func (s StrategyCondition) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Scan implements the sql.Scanner interface for JSON unmarshaling
func (s *StrategyCondition) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, s)
}
