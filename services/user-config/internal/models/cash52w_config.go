package models

import "time"

// Cash52WConfig represents the minimal, production-ready configuration
// for the managed Cash 52-Week High strategy. This is stored in the
// dedicated cash52w_configs table and avoids any dummy generic
// strategy/trade_config values.
type Cash52WConfig struct {
	UserID          string    `db:"user_id" json:"user_id"`
	Enabled         bool      `db:"enabled" json:"enabled"`
	CapitalPerStock float64   `db:"capital_per_stock" json:"capital_per_stock"`
	TradingMode     string    `db:"trading_mode" json:"trading_mode"` // LIVE / PAPER
	UpdatedAt       time.Time `db:"updated_at" json:"updated_at"`
}
