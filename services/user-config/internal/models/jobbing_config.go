package models

import (
	"time"

	"github.com/google/uuid"
)

// JobbingConfig represents a per-user, per-token jobbing strategy configuration.
// Unlike generic strategies, jobbing configs are lightweight and token-specific,
// allowing users to configure jobbing parameters for multiple instruments independently.
type JobbingConfig struct {
	ID         uuid.UUID `db:"id" json:"id"`
	UserID     string    `db:"user_id" json:"user_id"`
	StrategyID string    `db:"strategy_id" json:"strategy_id"`

	// Token/Stock identification
	Token    string `db:"token" json:"token"`
	Symbol   string `db:"symbol" json:"symbol"`
	Exchange string `db:"exchange" json:"exchange"`

	// Price range limits
	LowerRange  float64 `db:"lower_range" json:"lower_range"`
	HigherRange float64 `db:"higher_range" json:"higher_range"`

	// Order placement parameters
	InitialBuyOffset float64 `db:"initial_buy_offset" json:"initial_buy_offset"`
	DistanceContinue float64 `db:"distance_continue" json:"distance_continue"`

	// Quantity management
	QuantityPerOrder int32 `db:"quantity_per_order" json:"quantity_per_order"`
	MaxQuantity      int32 `db:"max_quantity" json:"max_quantity"`

	// Trading mode
	TradingMode string `db:"trading_mode" json:"trading_mode"` // LIVE or PAPER

	// Status
	Enabled    bool       `db:"enabled" json:"enabled"`
	EnabledAt  *time.Time `db:"enabled_at" json:"enabled_at,omitempty"`
	DisabledAt *time.Time `db:"disabled_at" json:"disabled_at,omitempty"`

	// Timestamps
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// ConfigureJobbingStrategyRequest represents a request to configure jobbing strategy for a user
type ConfigureJobbingStrategyRequest struct {
	UserID  string               `json:"user_id" validate:"required"`
	Configs []JobbingTokenConfig `json:"configs" validate:"required,min=1,dive"`
}

// JobbingTokenConfig represents configuration for a single token in the jobbing strategy
type JobbingTokenConfig struct {
	Token    string `json:"token" validate:"required"`
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`

	// Price range (required)
	LowerRange  float64 `json:"lower_range" validate:"required,gt=0"`
	HigherRange float64 `json:"higher_range" validate:"required,gtfield=LowerRange"`

	// Order parameters (optional, will use defaults)
	InitialBuyOffset *float64 `json:"initial_buy_offset,omitempty" validate:"omitempty,gt=0"`
	DistanceContinue *float64 `json:"distance_continue,omitempty" validate:"omitempty,gt=0"`

	// Quantity (optional, will use defaults)
	QuantityPerOrder *int32 `json:"quantity_per_order,omitempty" validate:"omitempty,gt=0"`
	MaxQuantity      *int32 `json:"max_quantity,omitempty" validate:"omitempty,gte=1"`

	// Trading mode (optional, defaults to LIVE)
	TradingMode *string `json:"trading_mode,omitempty" validate:"omitempty,oneof=LIVE PAPER"`

	// Enable/disable flag (optional, defaults to true)
	Enabled *bool `json:"enabled,omitempty"`
}

// UpdateJobbingConfigRequest represents a request to update a single jobbing config
type UpdateJobbingConfigRequest struct {
	UserID string `json:"user_id" validate:"required"`
	Token  string `json:"token" validate:"required"`

	// Optional updates
	LowerRange       *float64 `json:"lower_range,omitempty" validate:"omitempty,gt=0"`
	HigherRange      *float64 `json:"higher_range,omitempty" validate:"omitempty,gt=0"`
	InitialBuyOffset *float64 `json:"initial_buy_offset,omitempty" validate:"omitempty,gt=0"`
	DistanceContinue *float64 `json:"distance_continue,omitempty" validate:"omitempty,gt=0"`
	QuantityPerOrder *int32   `json:"quantity_per_order,omitempty" validate:"omitempty,gt=0"`
	MaxQuantity      *int32   `json:"max_quantity,omitempty" validate:"omitempty,gte=1"`
	TradingMode      *string  `json:"trading_mode,omitempty" validate:"omitempty,oneof=LIVE PAPER"`
	Enabled          *bool    `json:"enabled,omitempty"`
}

// DeleteJobbingConfigRequest represents a request to delete a jobbing config
type DeleteJobbingConfigRequest struct {
	UserID string `json:"user_id" validate:"required"`
	Token  string `json:"token" validate:"required"`
}

// GetJobbingConfigsRequest represents a request to get all jobbing configs for a user
type GetJobbingConfigsRequest struct {
	UserID      string `json:"user_id" validate:"required"`
	EnabledOnly bool   `json:"enabled_only"`
}

// JobbingConfigEvent represents a Kafka event for jobbing configuration changes
type JobbingConfigEvent struct {
	EventType string        `json:"event_type"` // CREATED, UPDATED, DELETED, ENABLED, DISABLED
	Timestamp time.Time     `json:"timestamp"`
	UserID    string        `json:"user_id"` // Top-level for consumer compatibility
	Token     string        `json:"token"`   // Top-level for consumer compatibility
	Config    JobbingConfig `json:"config"`
}

// ConfigureJobbingStrategyResponse represents the response after configuring jobbing strategy
type ConfigureJobbingStrategyResponse struct {
	Success    bool            `json:"success"`
	Message    string          `json:"message"`
	UserID     string          `json:"user_id"`
	Configs    []JobbingConfig `json:"configs"`
	TotalCount int             `json:"total_count"`
}

// Validate validates the JobbingTokenConfig
func (c *JobbingTokenConfig) Validate() error {
	if c.Token == "" {
		return ErrInvalidJobbingConfig("token is required")
	}
	if c.LowerRange <= 0 {
		return ErrInvalidJobbingConfig("lower_range must be greater than 0")
	}
	if c.HigherRange <= c.LowerRange {
		return ErrInvalidJobbingConfig("higher_range must be greater than lower_range")
	}
	if c.InitialBuyOffset != nil && *c.InitialBuyOffset <= 0 {
		return ErrInvalidJobbingConfig("initial_buy_offset must be greater than 0")
	}
	if c.DistanceContinue != nil && *c.DistanceContinue <= 0 {
		return ErrInvalidJobbingConfig("distance_continue must be greater than 0")
	}
	if c.QuantityPerOrder != nil && *c.QuantityPerOrder <= 0 {
		return ErrInvalidJobbingConfig("quantity_per_order must be greater than 0")
	}
	if c.MaxQuantity != nil && *c.MaxQuantity < 1 {
		return ErrInvalidJobbingConfig("max_quantity must be at least 1")
	}
	if c.QuantityPerOrder != nil && c.MaxQuantity != nil && *c.MaxQuantity < *c.QuantityPerOrder {
		return ErrInvalidJobbingConfig("max_quantity must be >= quantity_per_order")
	}
	return nil
}

// ApplyDefaults applies default values to optional fields
func (c *JobbingTokenConfig) ApplyDefaults() {
	if c.Exchange == "" {
		c.Exchange = "NSE"
	}
	if c.InitialBuyOffset == nil {
		defaultOffset := 0.01
		c.InitialBuyOffset = &defaultOffset
	}
	if c.DistanceContinue == nil {
		defaultDistance := 0.01
		c.DistanceContinue = &defaultDistance
	}
	if c.QuantityPerOrder == nil {
		defaultQty := int32(1)
		c.QuantityPerOrder = &defaultQty
	}
	if c.MaxQuantity == nil {
		defaultMaxQty := int32(10)
		c.MaxQuantity = &defaultMaxQty
	}
	if c.TradingMode == nil {
		defaultMode := "LIVE"
		c.TradingMode = &defaultMode
	}
	if c.Enabled == nil {
		defaultEnabled := true
		c.Enabled = &defaultEnabled
	}
}

// ErrInvalidJobbingConfig creates a custom error for invalid jobbing configuration
func ErrInvalidJobbingConfig(msg string) error {
	return &ValidationError{
		Field:   "jobbing_config",
		Message: msg,
	}
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Field + ": " + e.Message
}
