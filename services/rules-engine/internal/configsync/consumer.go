package configsync

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// Store is the minimal interface required by the config sync consumer.
// It is implemented by *configstore.ConfigStore.
type Store interface {
	Upsert(cfg *models.StrategyConfig) error
	Pause(userID, strategyID string, version uint64) error
	Resume(userID, strategyID string, version uint64) error
	Remove(userID, strategyID string, version uint64) error
	GetStrategy(userID, strategyID string) (*models.StrategyConfig, bool)
}

// Consumer converts Kafka user-config-events payloads into store operations.
//
// This is intentionally pure/business-logic and does not deal with kafka.Reader.
// The kafka wrapper (internal/kafka/config_consumer.go) is responsible for
// polling messages and committing offsets.
type Consumer struct {
	store Store
}

func NewConsumer(store Store) *Consumer {
	return &Consumer{store: store}
}

// ProcessMessage parses one Kafka message value and applies it to the store.
//
// Semantics:
//   - JSON parse errors: returned as error
//   - Unknown event types: ignored (nil error)
//   - Stale versions: ignored (nil error)
func (c *Consumer) ProcessMessage(ctx context.Context, value []byte) error {
	_ = ctx
	if c == nil || c.store == nil {
		return fmt.Errorf("configsync consumer store is nil")
	}

	var ev ConfigEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return fmt.Errorf("unmarshal ConfigEvent: %w", err)
	}

	// Basic validation for events that require ids
	if ev.UserID == "" || ev.StrategyID == "" {
		// don't crash on malformed data
		return nil
	}

	// stale check against current snapshot (fast path)
	if cur, ok := c.store.GetStrategy(ev.UserID, ev.StrategyID); ok {
		if ev.Version != 0 && ev.Version <= cur.Version {
			return nil
		}
	}

	switch ev.Type {
	case ConfigCreated, ConfigUpdated:
		if ev.Config == nil {
			return nil
		}
		m, err := ToModelStrategy(ev.Config)
		if err != nil {
			return nil
		}
		// enforce envelope version
		m.Version = ev.Version
		return c.store.Upsert((*models.StrategyConfig)(m))

	case ConfigPaused:
		return c.store.Pause(ev.UserID, ev.StrategyID, ev.Version)
	case ConfigResumed:
		return c.store.Resume(ev.UserID, ev.StrategyID, ev.Version)
	case ConfigDeleted:
		return c.store.Remove(ev.UserID, ev.StrategyID, ev.Version)
	default:
		return nil
	}
}
