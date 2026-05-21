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

// BackfillTrigger launches the after-market-news backfill for a newly created
// strategy. Implemented by backfill.Service. Run MUST be non-blocking — the
// config consumer is single-threaded, so a blocking call would stall every
// later config event in the partition.
type BackfillTrigger interface {
	Run(ctx context.Context, strategy *models.Strategy)
}

// ConfigConsumer consumes "user-config-events" and applies deltas to ConfigStore.
// Single goroutine. Never crashes on bad messages.
type ConfigConsumer struct {
	reader      KafkaReader
	configStore *configstore.ConfigStore
	backfill    BackfillTrigger
	processed   atomic.Int64
	errors      atomic.Int64
}

// NewConfigConsumer builds a consumer without the backfill hook (tests).
func NewConfigConsumer(reader KafkaReader, store *configstore.ConfigStore) *ConfigConsumer {
	return &ConfigConsumer{reader: reader, configStore: store}
}

// NewConfigConsumerWithBackfill is the production constructor. Pass nil for
// backfill to disable the feature (e.g. when MongoDB is unavailable).
func NewConfigConsumerWithBackfill(reader KafkaReader, store *configstore.ConfigStore, backfill BackfillTrigger) *ConfigConsumer {
	return &ConfigConsumer{reader: reader, configStore: store, backfill: backfill}
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

			m, err := configsync.ToModelStrategy(ev.Config)
			if err != nil {
				c.errors.Add(1)
				log.Printf("config consumer: map payload error: %v", err)
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}
			m.Version = ev.Version
			m.Active = true

			// After-Market News backfill fires on every CONFIG_CREATED — even a
			// "stale" replay. This is deliberate: the backfill must trigger here
			// rather than after the staleness gate below, because on restart the
			// bootstrapper has already loaded the strategy into the config store,
			// which would make the replayed CONFIG_CREATED look stale and skip
			// the trigger — losing the backfill for any strategy created while
			// rules-engine was down. BackfillService is idempotent (backfill_jobs
			// claim via INSERT ON CONFLICT), so replaying is a cheap no-op once
			// the job already exists.
			if c.backfill != nil && string(ev.Type) == "CONFIG_CREATED" && m.ProcessAfterMarketNews {
				c.backfill.Run(ctx, m)
			}

			// Staleness gate — applies to the in-memory config store upsert only.
			if existing, ok := c.configStore.GetStrategy(ev.UserID, ev.StrategyID); ok {
				if existing.Version >= ev.Version {
					log.Printf("config consumer: skipping stale event existing=%d incoming=%d", existing.Version, ev.Version)
					_ = c.reader.CommitMessages(ctx, msg)
					continue
				}
			}

			if err := c.configStore.Upsert((*models.StrategyConfig)(m)); err != nil {
				c.errors.Add(1)
				log.Printf("config consumer: upsert failed: %v", err)
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
