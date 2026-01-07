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

// ConfigureCash52WeekStrategyRequest is a high-level request used by the
// service layer to configure the managed Cash 52-week High strategy for a
// user. Most fields are optional and will take backend defaults; the
// frontend typically only provides UserID, Enabled and CapitalPerStock.
type ConfigureCash52WeekStrategyRequest struct {
	UserID          string  `json:"user_id"`
	Enabled         bool    `json:"enabled"`
	CapitalPerStock float64 `json:"capital_per_stock"`

	// Optional overrides (not required from frontend today)
	MaxPositions  int     `json:"-"`
	StopLossPct   float64 `json:"-"`
	TakeProfitPct float64 `json:"-"`
	RiskProfile   string  `json:"-"`
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
