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
	StrategyType string             `json:"strategy_type"`
	Active       bool               `json:"active"`
	TradingMode  string             `json:"trading_mode"`
	TradeConfig  TradeConfigPayload `json:"trade_config"`
	Version      uint64             `json:"version"`
	CreatedAt    int64              `json:"created_at"` // UnixNano
	UpdatedAt    int64              `json:"updated_at"` // UnixNano

	// Conditions + RiskLimits fields removed 2026-06-25 (Cat B trim).
	// They came from the news-event-driven strategy schema; the user-config
	// Kafka message still includes them but json.Unmarshal silently ignores
	// unknown fields, so receiving older messages is harmless.
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
	TrailingSLPct  float64 `json:"trailing_sl_pct"`
	StopLossType   string  `json:"stop_loss_type"`
	TotalCapital   float64 `json:"total_capital"`
	MaxPositions   int32   `json:"max_positions"`
	PerStockAmount float64 `json:"per_stock_amount"`
	CreatedAt      int64   `json:"created_at"`
}

// RiskLimitsPayload removed 2026-06-25 (Cat B trim) — news-event path's risk
// schema. Manthan enforces its own caps via rebalancer + allocator.
