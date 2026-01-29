package cash52w

import (
	"sync"
	"time"
)

// ConfigEvent mirrors the compact 52W config event published by the
// user-config service to the user-configs.cash52w topic.
type ConfigEvent struct {
	EventType       string  `json:"event_type"`
	UserID          string  `json:"user_id"`
	Enabled         bool    `json:"enabled"`
	CapitalPerStock float64 `json:"capital_per_stock"`
	TradingMode     string  `json:"trading_mode"`
	Timestamp       int64   `json:"timestamp"`
}

// UserConfig is the in-memory representation of a user's 52W
// configuration as understood by the rules-engine.
type UserConfig struct {
	Enabled         bool
	CapitalPerStock float64
	TradingMode     string
	EnabledSince    time.Time
}

// ConfigStore maintains 52W user configs in memory based on the
// user-configs.cash52w Kafka stream.
type ConfigStore struct {
	mu      sync.RWMutex
	configs map[string]UserConfig // key: user_id
}

func NewConfigStore() *ConfigStore {
	return &ConfigStore{
		configs: make(map[string]UserConfig),
	}
}

// ApplyEvent applies a single 52W config event to the in-memory state.
// CREATE/UPDATE with enabled=true will upsert the config and set
// EnabledSince to the event timestamp. DELETE or enabled=false will
// remove the config.
func (s *ConfigStore) ApplyEvent(ev ConfigEvent) {
	if ev.UserID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch ev.EventType {
	case "CREATE", "UPDATE":
		if !ev.Enabled {
			delete(s.configs, ev.UserID)
			return
		}

		enabledSince := time.Unix(ev.Timestamp, 0)
		cfg := UserConfig{
			Enabled:         ev.Enabled,
			CapitalPerStock: ev.CapitalPerStock,
			TradingMode:     ev.TradingMode,
			EnabledSince:    enabledSince,
		}
		s.configs[ev.UserID] = cfg

	case "DELETE":
		delete(s.configs, ev.UserID)
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
