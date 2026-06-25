package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configsync"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// ConfigConsumer consumes "user-config-events" and applies deltas to ConfigStore.
// Single goroutine. Never crashes on bad messages.
//
// The OnManthanCreatedFn named type was inlined 2026-06-25 — manthan.Wire
// passes a closure through its own narrow interface (without importing
// internal/kafka), so we no longer benefit from a named alias here.

type ConfigConsumer struct {
	reader           KafkaReader
	configStore      *configstore.ConfigStore
	onManthanCreated func(ctx context.Context, strategy *models.Strategy)
	processed        atomic.Int64
	errors           atomic.Int64
}

func NewConfigConsumer(reader KafkaReader, store *configstore.ConfigStore) *ConfigConsumer {
	return &ConfigConsumer{reader: reader, configStore: store}
}

func (c *ConfigConsumer) SetOnManthanCreated(fn func(ctx context.Context, strategy *models.Strategy)) {
	c.onManthanCreated = fn
}

func (c *ConfigConsumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			c.errors.Add(1)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("config consumer: fetch error: %v", err)
			time.Sleep(250 * time.Millisecond)
			continue
		}

		var ev configsync.ConfigEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			c.errors.Add(1)
			log.Printf("config consumer: parse error: %v", err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		switch string(ev.Type) {
		case "CONFIG_CREATED", "CONFIG_UPDATED":
			if ev.Config == nil {
				log.Printf("config consumer: missing config payload for %s", ev.Type)
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			if existing, ok := c.configStore.GetStrategy(ev.UserID, ev.StrategyID); ok {
				if existing.Version >= ev.Version {
					log.Printf("config consumer: skipping stale event existing=%d incoming=%d", existing.Version, ev.Version)
					_ = c.reader.CommitMessages(ctx, msg)
					continue
				}
			}

			m, err := configsync.ToModelStrategy(ev.Config)
			if err != nil {
				c.errors.Add(1)
				log.Printf("config consumer: map payload error: %v", err)
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}
			m.Version = ev.Version
			m.Active = true
			if err := c.configStore.Upsert((*models.StrategyConfig)(m)); err != nil {
				c.errors.Add(1)
				log.Printf("config consumer: upsert failed: %v", err)
			}
			if string(ev.Type) == "CONFIG_CREATED" && m.StrategyType == "MANTHAN" && c.onManthanCreated != nil {
				log.Printf("config consumer: MANTHAN strategy created — triggering catch-up for user=%s strategy=%s", m.UserID, m.StrategyID)
				go c.onManthanCreated(ctx, m)
			}

		case "CONFIG_PAUSED":
			if err := c.configStore.Pause(ev.UserID, ev.StrategyID, ev.Version); err != nil {
				c.errors.Add(1)
				log.Printf("config consumer: pause failed: %v", err)
			}

		case "CONFIG_RESUMED":
			if err := c.configStore.Resume(ev.UserID, ev.StrategyID, ev.Version); err != nil {
				c.errors.Add(1)
				log.Printf("config consumer: resume failed: %v", err)
			}

		case "CONFIG_DELETED":
			if err := c.configStore.Remove(ev.UserID, ev.StrategyID, ev.Version); err != nil {
				c.errors.Add(1)
				log.Printf("config consumer: remove failed: %v", err)
			}

		default:
			log.Printf("config consumer: WARN unknown event type %q — skipping", ev.Type)
		}

		c.processed.Add(1)
		_ = c.reader.CommitMessages(ctx, msg)
	}
}

func (c *ConfigConsumer) Stats() (processed int64, errors int64) {
	return c.processed.Load(), c.errors.Load()
}

// (no extra helpers)
