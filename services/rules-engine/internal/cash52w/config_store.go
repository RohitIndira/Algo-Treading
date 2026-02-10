package cash52w

import (
	"sync"
	"time"
)

// ============================================================================
// PHASE 1: Enhanced Cash52W Configuration with Multi-Level SL/Profit
// ============================================================================

// StopLossLevel represents a single stop-loss trigger point
type StopLossLevel struct {
	TriggerPercent      float64 `json:"trigger_percent"`       // e.g. -10 for 10% loss
	ExitQuantityPercent int     `json:"exit_quantity_percent"` // e.g. 50 for 50% exit
	Type                string  `json:"type"`                  // "fixed" or "trailing"
	Enabled             bool    `json:"enabled"`
}

// StopLossLevels contains multiple stop-loss levels
type StopLossLevels struct {
	Level1 StopLossLevel `json:"level_1"`
	Level2 StopLossLevel `json:"level_2"`
}

// ProfitLevel represents a single profit-taking trigger point
type ProfitLevel struct {
	TriggerPercent      float64 `json:"trigger_percent"`       // e.g. 15 for 15% profit
	ExitQuantityPercent int     `json:"exit_quantity_percent"` // e.g. 33 for 33% exit
	Type                string  `json:"type"`                  // "fixed" or "trailing"
	TrailPercent        float64 `json:"trail_percent"`         // for trailing only
	Enabled             bool    `json:"enabled"`
}

// ProfitLevels contains multiple profit-taking levels
type ProfitLevels struct {
	Level1 ProfitLevel `json:"level_1"`
	Level2 ProfitLevel `json:"level_2"`
	Level3 ProfitLevel `json:"level_3"`
}

// ConfigEvent mirrors the PHASE 1 ENHANCED 52W config published by
// user-config service to the user-config-updates topic.
type ConfigEvent struct {
	UserID           string          `json:"user_id"`
	Enabled          bool            `json:"enabled"`
	TotalCapital     float64         `json:"total_capital"`
	CapitalPerStock  float64         `json:"capital_per_stock"`
	MaxStocks        int             `json:"max_stocks"`
	AutoRebalance    bool            `json:"auto_rebalance"`
	StopLossLevels   StopLossLevels  `json:"stop_loss_levels"`
	ProfitLevels     ProfitLevels    `json:"profit_levels"`
	TradingMode      string          `json:"trading_mode"`
	ForceExitAll     bool            `json:"force_exit_all"`
	ForceExitStocks  []string        `json:"force_exit_stocks"`
	PauseNewEntries  bool            `json:"pause_new_entries"`
	UpdatedAt        time.Time       `json:"updated_at"`
	Version          int             `json:"version"`
}

// UserConfig is the PHASE 1 ENHANCED in-memory configuration for a user's
// 52W strategy with multi-level profit/SL support.
type UserConfig struct {
	Enabled          bool
	TotalCapital     float64
	CapitalPerStock  float64
	MaxStocks        int
	AutoRebalance    bool
	StopLossLevels   StopLossLevels
	ProfitLevels     ProfitLevels
	TradingMode      string
	ForceExitAll     bool
	ForceExitStocks  []string
	PauseNewEntries  bool
	EnabledSince     time.Time
	Version          int
}

// ConfigStore maintains 52W user configs in memory based on the
// user-configs.cash52w Kafka stream.
type ConfigStore struct {
	mu      sync.RWMutex
	configs map[string]UserConfig // key: user_id
	// onEnable is called when a user becomes enabled (CREATE/UPDATE enabled=true)
	// so the engine can perform catch-up/backfill.
	onEnable func(userID string, enabledSince time.Time)
}

func NewConfigStore() *ConfigStore {
	return &ConfigStore{
		configs: make(map[string]UserConfig),
	}
}

// SetOnEnable registers a callback that is triggered whenever a user is
// enabled for the 52W strategy.
func (s *ConfigStore) SetOnEnable(fn func(userID string, enabledSince time.Time)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onEnable = fn
}

// ApplyEvent applies a PHASE 1 ENHANCED 52W config event to in-memory state.
// Handles all new fields: multi-level SL/profit, force exit, pause controls.
func (s *ConfigStore) ApplyEvent(ev ConfigEvent) {
	if ev.UserID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If disabled, remove from active configs
	if !ev.Enabled {
		delete(s.configs, ev.UserID)
		return
	}

	// Check if this is a new enablement (for backfill trigger)
	wasEnabled := false
	if existing, ok := s.configs[ev.UserID]; ok {
		wasEnabled = existing.Enabled
	}

	// Upsert the full Phase 1 enhanced config
	cfg := UserConfig{
		Enabled:          ev.Enabled,
		TotalCapital:     ev.TotalCapital,
		CapitalPerStock:  ev.CapitalPerStock,
		MaxStocks:        ev.MaxStocks,
		AutoRebalance:    ev.AutoRebalance,
		StopLossLevels:   ev.StopLossLevels,
		ProfitLevels:     ev.ProfitLevels,
		TradingMode:      ev.TradingMode,
		ForceExitAll:     ev.ForceExitAll,
		ForceExitStocks:  ev.ForceExitStocks,
		PauseNewEntries:  ev.PauseNewEntries,
		EnabledSince:     ev.UpdatedAt,
		Version:          ev.Version,
	}

	s.configs[ev.UserID] = cfg

	// Trigger backfill only if this is a NEW enablement (not an update)
	if !wasEnabled && s.onEnable != nil {
		go s.onEnable(ev.UserID, ev.UpdatedAt)
	}
}

// Snapshot returns the list of active users (enabled==true) and their
// trading modes. This is used by the engine to refresh its user list
// and per-user trading modes periodically.
func (s *ConfigStore) Snapshot() ([]string, map[string]string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make([]string, 0, len(s.configs))
	modes := make(map[string]string, len(s.configs))

	for uid, cfg := range s.configs {
		if !cfg.Enabled {
			continue
		}
		users = append(users, uid)
		modes[uid] = cfg.TradingMode
	}

	return users, modes
}

// Get returns the UserConfig for a given user, if present.
func (s *ConfigStore) Get(userID string) (UserConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cfg, ok := s.configs[userID]
	return cfg, ok
}
