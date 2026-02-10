package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/lib/pq"
)

// Cash52WConfig represents the production-ready configuration
// for the managed Cash 52-Week High strategy
//
// ARCHITECTURE:
// 1. User configures via API Gateway (REST)
// 2. Saved to PostgreSQL (persistence)
// 3. Published to Kafka topic: user-config-updates
// 4. rules-engine consumes and caches in-memory (ConfigStore)
// 5. rules-engine uses cached config (NO DB reads during trading!)
type Cash52WConfig struct {
	// Identity
	UserID  string `db:"user_id" json:"user_id"`
	Enabled bool   `db:"enabled" json:"enabled"`

	// Portfolio Configuration
	TotalCapital    float64 `db:"total_capital" json:"total_capital"`         // e.g., 500000
	CapitalPerStock float64 `db:"capital_per_stock" json:"capital_per_stock"` // e.g., 20000
	MaxStocks       int     `db:"max_stocks" json:"max_stocks"`               // e.g., 25
	AutoRebalance   bool    `db:"auto_rebalance" json:"auto_rebalance"`       // Auto-buy when SL hit

	// Stop-Loss Levels (JSONB in DB)
	StopLossLevels StopLossLevels `db:"stop_loss_levels" json:"stop_loss_levels"`

	// Profit Target Levels (JSONB in DB)
	ProfitLevels ProfitLevels `db:"profit_levels" json:"profit_levels"`

	// Trading Mode
	TradingMode string `db:"trading_mode" json:"trading_mode"` // LIVE / PAPER

	// Manual Controls (Emergency actions)
	ForceExitAll    bool           `db:"force_exit_all" json:"force_exit_all"`
	ForceExitStocks pq.StringArray `db:"force_exit_stocks" json:"force_exit_stocks"` // ["NSE:RELIANCE", "NSE:TCS"]
	PauseNewEntries bool           `db:"pause_new_entries" json:"pause_new_entries"`

	// Metadata
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
	Version   int       `db:"version" json:"version"` // For optimistic locking
}

// StopLossLevels defines multi-level stop-loss configuration
// Example: -10% exit 50%, then -20% exit remaining 50%
type StopLossLevels struct {
	Level1 StopLossLevel `json:"level_1"`
	Level2 StopLossLevel `json:"level_2"`
}

// StopLossLevel represents a single stop-loss trigger
type StopLossLevel struct {
	TriggerPercent      float64 `json:"trigger_percent"`       // e.g., -10 (negative for loss)
	ExitQuantityPercent int     `json:"exit_quantity_percent"` // e.g., 50 (exit 50% of position)
	Type                string  `json:"type"`                  // "fixed" or "trailing"
	Enabled             bool    `json:"enabled"`               // Allow disabling levels
}

// ProfitLevels defines multi-level profit target configuration
// Example: +15% book 33%, +30% book 50%, +50% book 100%
type ProfitLevels struct {
	Level1 ProfitLevel `json:"level_1"`
	Level2 ProfitLevel `json:"level_2"`
	Level3 ProfitLevel `json:"level_3"`
}

// ProfitLevel represents a single profit target
type ProfitLevel struct {
	TriggerPercent      float64 `json:"trigger_percent"`       // e.g., 15 (positive for profit)
	ExitQuantityPercent int     `json:"exit_quantity_percent"` // e.g., 33 (book 33% profit)
	Type                string  `json:"type"`                  // "fixed" or "trailing"
	TrailPercent        float64 `json:"trail_percent"`         // Only for trailing type (e.g., 10%)
	Enabled             bool    `json:"enabled"`               // Allow disabling levels
}

// Validate performs comprehensive validation on the configuration
func (c *Cash52WConfig) Validate() error {
	// User ID validation
	if c.UserID == "" {
		return errors.New("user_id is required")
	}

	// Capital validation
	if c.TotalCapital < 50000 {
		return errors.New("total_capital must be >= ₹50,000")
	}
	if c.TotalCapital > 100000000 {
		return errors.New("total_capital must be <= ₹10,00,00,000")
	}

	if c.CapitalPerStock < 5000 {
		return errors.New("capital_per_stock must be >= ₹5,000")
	}
	if c.CapitalPerStock > 1000000 {
		return errors.New("capital_per_stock must be <= ₹10,00,000")
	}

	// Portfolio validation
	if c.MaxStocks < 5 {
		return errors.New("max_stocks must be >= 5")
	}
	if c.MaxStocks > 100 {
		return errors.New("max_stocks must be <= 100")
	}

	// Check if total capital supports max stocks
	if c.TotalCapital < float64(c.MaxStocks)*c.CapitalPerStock {
		return errors.New("total_capital is insufficient for max_stocks * capital_per_stock")
	}

	// Stop-loss validation
	if err := c.validateStopLoss(); err != nil {
		return err
	}

	// Profit validation
	if err := c.validateProfitLevels(); err != nil {
		return err
	}

	// Trading mode validation
	if c.TradingMode != "LIVE" && c.TradingMode != "PAPER" {
		return errors.New("trading_mode must be LIVE or PAPER")
	}

	return nil
}

// validateStopLoss validates stop-loss configuration
func (c *Cash52WConfig) validateStopLoss() error {
	// Level 1 validation
	if c.StopLossLevels.Level1.Enabled {
		if c.StopLossLevels.Level1.TriggerPercent >= 0 {
			return errors.New("stop_loss level_1 trigger_percent must be negative")
		}
		if c.StopLossLevels.Level1.TriggerPercent < -50 {
			return errors.New("stop_loss level_1 trigger_percent must be >= -50%")
		}
		if c.StopLossLevels.Level1.ExitQuantityPercent <= 0 || c.StopLossLevels.Level1.ExitQuantityPercent > 100 {
			return errors.New("stop_loss level_1 exit_quantity_percent must be 1-100")
		}
		if c.StopLossLevels.Level1.Type != "fixed" && c.StopLossLevels.Level1.Type != "trailing" {
			return errors.New("stop_loss level_1 type must be 'fixed' or 'trailing'")
		}
	}

	// Level 2 validation
	if c.StopLossLevels.Level2.Enabled {
		if c.StopLossLevels.Level2.TriggerPercent >= 0 {
			return errors.New("stop_loss level_2 trigger_percent must be negative")
		}
		if c.StopLossLevels.Level2.TriggerPercent < -50 {
			return errors.New("stop_loss level_2 trigger_percent must be >= -50%")
		}
		if c.StopLossLevels.Level2.ExitQuantityPercent <= 0 || c.StopLossLevels.Level2.ExitQuantityPercent > 100 {
			return errors.New("stop_loss level_2 exit_quantity_percent must be 1-100")
		}
		if c.StopLossLevels.Level2.Type != "fixed" && c.StopLossLevels.Level2.Type != "trailing" {
			return errors.New("stop_loss level_2 type must be 'fixed' or 'trailing'")
		}

		// Level 2 must be deeper than Level 1
		if c.StopLossLevels.Level1.Enabled && c.StopLossLevels.Level2.TriggerPercent > c.StopLossLevels.Level1.TriggerPercent {
			return errors.New("stop_loss level_2 must be deeper than level_1 (more negative)")
		}
	}

	return nil
}

// validateProfitLevels validates profit target configuration
func (c *Cash52WConfig) validateProfitLevels() error {
	// Level 1 validation
	if c.ProfitLevels.Level1.Enabled {
		if c.ProfitLevels.Level1.TriggerPercent <= 0 {
			return errors.New("profit level_1 trigger_percent must be positive")
		}
		if c.ProfitLevels.Level1.TriggerPercent > 200 {
			return errors.New("profit level_1 trigger_percent must be <= 200%")
		}
		if c.ProfitLevels.Level1.ExitQuantityPercent <= 0 || c.ProfitLevels.Level1.ExitQuantityPercent > 100 {
			return errors.New("profit level_1 exit_quantity_percent must be 1-100")
		}
		if c.ProfitLevels.Level1.Type != "fixed" && c.ProfitLevels.Level1.Type != "trailing" {
			return errors.New("profit level_1 type must be 'fixed' or 'trailing'")
		}
	}

	// Level 2 validation
	if c.ProfitLevels.Level2.Enabled {
		if c.ProfitLevels.Level2.TriggerPercent <= 0 {
			return errors.New("profit level_2 trigger_percent must be positive")
		}
		if c.ProfitLevels.Level2.TriggerPercent > 200 {
			return errors.New("profit level_2 trigger_percent must be <= 200%")
		}
		if c.ProfitLevels.Level2.ExitQuantityPercent <= 0 || c.ProfitLevels.Level2.ExitQuantityPercent > 100 {
			return errors.New("profit level_2 exit_quantity_percent must be 1-100")
		}
		if c.ProfitLevels.Level2.Type != "fixed" && c.ProfitLevels.Level2.Type != "trailing" {
			return errors.New("profit level_2 type must be 'fixed' or 'trailing'")
		}

		// Level 2 must be higher than Level 1
		if c.ProfitLevels.Level1.Enabled && c.ProfitLevels.Level2.TriggerPercent <= c.ProfitLevels.Level1.TriggerPercent {
			return errors.New("profit level_2 must be higher than level_1")
		}
	}

	// Level 3 validation
	if c.ProfitLevels.Level3.Enabled {
		if c.ProfitLevels.Level3.TriggerPercent <= 0 {
			return errors.New("profit level_3 trigger_percent must be positive")
		}
		if c.ProfitLevels.Level3.TriggerPercent > 200 {
			return errors.New("profit level_3 trigger_percent must be <= 200%")
		}
		if c.ProfitLevels.Level3.ExitQuantityPercent <= 0 || c.ProfitLevels.Level3.ExitQuantityPercent > 100 {
			return errors.New("profit level_3 exit_quantity_percent must be 1-100")
		}
		if c.ProfitLevels.Level3.Type != "fixed" && c.ProfitLevels.Level3.Type != "trailing" {
			return errors.New("profit level_3 type must be 'fixed' or 'trailing'")
		}
		if c.ProfitLevels.Level3.Type == "trailing" && c.ProfitLevels.Level3.TrailPercent <= 0 {
			return errors.New("profit level_3 trail_percent must be positive for trailing type")
		}

		// Level 3 must be higher than Level 2
		if c.ProfitLevels.Level2.Enabled && c.ProfitLevels.Level3.TriggerPercent <= c.ProfitLevels.Level2.TriggerPercent {
			return errors.New("profit level_3 must be higher than level_2")
		}
	}

	return nil
}

// DefaultBalancedConfig returns a balanced default configuration
func DefaultBalancedConfig(userID string) *Cash52WConfig {
	return &Cash52WConfig{
		UserID:          userID,
		Enabled:         false, // User must explicitly enable
		TotalCapital:    500000,
		CapitalPerStock: 20000,
		MaxStocks:       25,
		AutoRebalance:   true,
		StopLossLevels: StopLossLevels{
			Level1: StopLossLevel{
				TriggerPercent:      -10,
				ExitQuantityPercent: 50,
				Type:                "fixed",
				Enabled:             true,
			},
			Level2: StopLossLevel{
				TriggerPercent:      -20,
				ExitQuantityPercent: 100,
				Type:                "trailing",
				Enabled:             true,
			},
		},
		ProfitLevels: ProfitLevels{
			Level1: ProfitLevel{
				TriggerPercent:      15,
				ExitQuantityPercent: 33,
				Type:                "fixed",
				Enabled:             true,
			},
			Level2: ProfitLevel{
				TriggerPercent:      30,
				ExitQuantityPercent: 50,
				Type:                "fixed",
				Enabled:             true,
			},
			Level3: ProfitLevel{
				TriggerPercent:      50,
				ExitQuantityPercent: 100,
				Type:                "trailing",
				TrailPercent:        10,
				Enabled:             true,
			},
		},
		TradingMode:     "PAPER",
		ForceExitAll:    false,
		ForceExitStocks: []string{},
		PauseNewEntries: false,
		UpdatedAt:       time.Now(),
		Version:         1,
	}
}

// Scan implements sql.Scanner for StopLossLevels (JSONB)
func (s *StopLossLevels) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("failed to scan StopLossLevels")
	}
	return json.Unmarshal(bytes, s)
}

// Value implements driver.Valuer for StopLossLevels (JSONB)
func (s StopLossLevels) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Scan implements sql.Scanner for ProfitLevels (JSONB)
func (p *ProfitLevels) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("failed to scan ProfitLevels")
	}
	return json.Unmarshal(bytes, p)
}

// Value implements driver.Valuer for ProfitLevels (JSONB)
func (p ProfitLevels) Value() (driver.Value, error) {
	return json.Marshal(p)
}
