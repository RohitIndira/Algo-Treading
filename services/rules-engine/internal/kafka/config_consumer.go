package kafka

import (
	"context"
	"encoding/json"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configsync"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
)

// ConfigConsumer consumes "user-config-events" and applies deltas to ConfigStore.
// Single goroutine. Never crashes on bad messages.
type ConfigConsumer struct {
	reader      KafkaReader
	configStore *configstore.ConfigStore
	amnTrigger  func(strategy *models.Strategy) // optional; called on CONFIG_CREATED with ProcessAfterMarketNews=true
	// capReset clears the strategy's daily trade counter on (re)activation so the
	// MaxTradesPerStrategy cap restarts fresh — mirroring the old "count only
	// signals since last activation" semantic. Optional; nil = no reset.
	capReset  func(strategyID string)
	processed atomic.Int64
	errors    atomic.Int64
}

// NewConfigConsumer creates a ConfigConsumer. amnTrigger may be nil to disable AMN backfill.
func NewConfigConsumer(reader KafkaReader, store *configstore.ConfigStore, amnTrigger func(*models.Strategy)) *ConfigConsumer {
	return &ConfigConsumer{reader: reader, configStore: store, amnTrigger: amnTrigger}
}

// SetCapReset wires the daily-trade-counter reset hook, invoked on strategy
// creation, edit, activation, and resume. Call before Start(). Pass nil to disable.
func (c *ConfigConsumer) SetCapReset(fn func(strategyID string)) {
	c.capReset = fn
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

		c.processMessageRecovered(ctx, msg)
	}
}

// processMessageRecovered processes one message with panic recovery, fulfilling
// the "Single goroutine. Never crashes on bad messages." contract above for the
// panic case too — a payload that panics deep in ToModelStrategy/Upsert/amnTrigger
// must not kill the only goroutine applying config changes. The message is still
// committed on panic (same as any other unprocessable event) so a poison-pill
// message can't get stuck retrying forever instead of just being skipped.
func (c *ConfigConsumer) processMessageRecovered(ctx context.Context, msg kafka.Message) {
	defer func() {
		if rec := recover(); rec != nil {
			c.errors.Add(1)
			log.Printf("config consumer: PANIC recovered (message skipped): %v\n%s", rec, debug.Stack())
			_ = c.reader.CommitMessages(ctx, msg)
		}
	}()
	c.processMessage(ctx, msg)
}

func (c *ConfigConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	var ev configsync.ConfigEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		c.errors.Add(1)
		log.Printf("config consumer: parse error: %v", err)
		_ = c.reader.CommitMessages(ctx, msg)
		return
	}

	switch string(ev.Type) {
	case "CONFIG_CREATED", "CONFIG_UPDATED", "CONFIG_ACTIVATED":
		if ev.Config == nil {
			log.Printf("config consumer: missing config payload for %s", ev.Type)
			_ = c.reader.CommitMessages(ctx, msg)
			return
		}

		if existing, ok := c.configStore.GetStrategy(ev.UserID, ev.StrategyID); ok {
			if existing.Version >= ev.Version {
				log.Printf("config consumer: skipping stale event existing=%d incoming=%d", existing.Version, ev.Version)
				_ = c.reader.CommitMessages(ctx, msg)
				return
			}
		}

		m, err := configsync.ToModelStrategy(ev.Config)
		if err != nil {
			c.errors.Add(1)
			log.Printf("config consumer: map payload error: %v", err)
			_ = c.reader.CommitMessages(ctx, msg)
			return
		}
		m.Version = ev.Version
		m.Active = true
		if err := c.configStore.Upsert((*models.StrategyConfig)(m)); err != nil {
			c.errors.Add(1)
			log.Printf("config consumer: upsert failed: %v", err)
		}

		// Reset the daily trade counter so the MaxTradesPerStrategy cap restarts
		// fresh on create/edit/activation — matching the old "since last
		// activation" reset semantic.
		if c.capReset != nil {
			c.capReset(ev.StrategyID)
		}

		// Audit: capture strategy initialization (CONFIG_CREATED only) with its
		// full parameter set, so the compliance trail records every strategy as
		// it comes online.
		if ev.Type == configsync.ConfigCreated {
			log.Printf("STRATEGY_INITIALIZED event=STRATEGY_INITIALIZED strategy_id=%s user_id=%s strategy_name=%q version=%d trading_mode=%s quantity=%d order_type=%s exchange=%s max_amount_per_stock=%.2f max_trades_per_strategy=%d process_after_market_news=%t",
				m.StrategyID, m.UserID, m.StrategyName, m.Version, m.TradingMode,
				m.TradeConfig.Quantity, m.TradeConfig.OrderType, m.TradeConfig.Exchange,
				m.RiskLimits.MaxAmountPerStock, m.RiskLimits.MaxTradesPerStrategy, m.ProcessAfterMarketNews)
		} else {
			// CONFIG_UPDATED = plain edits; CONFIG_ACTIVATED = re-activations. Both
			// upsert the strategy; only the latter re-runs the AMN backfill below.
			log.Printf("STRATEGY_UPDATED event=%s strategy_id=%s user_id=%s strategy_name=%q version=%d trading_mode=%s active=%t",
				ev.Type, m.StrategyID, m.UserID, m.StrategyName, m.Version, m.TradingMode, m.Active)
		}

		// Trigger the AMN backfill when an AMN strategy is created (once per lifecycle)
		// or reactivated (CONFIG_ACTIVATED — re-runs with that day's fresh selection).
		// Plain CONFIG_UPDATED edits never re-run it. The stale-version guard above
		// makes a redelivered event a no-op, so the backfill fires once per activation.
		//
		// Reactivation only runs the backfill when the user actually picked stocks: an
		// empty pick (e.g. no matching news in the AMN window) reactivates the strategy
		// for live trading with NO backfill, and must NOT fall back to placing every
		// match. Creation keeps its place-all-matches fallback (empty pick = place all).
		if m.ProcessAfterMarketNews && c.amnTrigger != nil {
			if ev.Type == configsync.ConfigCreated ||
				(ev.Type == configsync.ConfigActivated && len(m.AMNSelectedStocks) > 0) {
				c.amnTrigger(m)
			}
		}

	case "CONFIG_PAUSED":
		// Capture the name before pausing (thin events don't carry it).
		pausedName := ""
		if s, ok := c.configStore.GetStrategy(ev.UserID, ev.StrategyID); ok {
			pausedName = s.StrategyName
		}
		if err := c.configStore.Pause(ev.UserID, ev.StrategyID, ev.Version); err != nil {
			c.errors.Add(1)
			log.Printf("config consumer: pause failed: %v", err)
		} else {
			log.Printf("STRATEGY_DEACTIVATED event=STRATEGY_DEACTIVATED strategy_id=%s user_id=%s strategy_name=%q version=%d",
				ev.StrategyID, ev.UserID, pausedName, ev.Version)
		}

	case "CONFIG_RESUMED":
		resumedName := ""
		if s, ok := c.configStore.GetStrategy(ev.UserID, ev.StrategyID); ok {
			resumedName = s.StrategyName
		}
		if err := c.configStore.Resume(ev.UserID, ev.StrategyID, ev.Version); err != nil {
			c.errors.Add(1)
			log.Printf("config consumer: resume failed: %v", err)
		} else {
			// Resuming re-activates the strategy — restart its daily trade cap.
			if c.capReset != nil {
				c.capReset(ev.StrategyID)
			}
			log.Printf("STRATEGY_RESUMED event=STRATEGY_RESUMED strategy_id=%s user_id=%s strategy_name=%q version=%d",
				ev.StrategyID, ev.UserID, resumedName, ev.Version)
		}

	case "CONFIG_DELETED":
		// Capture the name before removing it from the store.
		deletedName := ""
		if s, ok := c.configStore.GetStrategy(ev.UserID, ev.StrategyID); ok {
			deletedName = s.StrategyName
		}
		if err := c.configStore.Remove(ev.UserID, ev.StrategyID, ev.Version); err != nil {
			c.errors.Add(1)
			log.Printf("config consumer: remove failed: %v", err)
		} else {
			log.Printf("STRATEGY_DELETED event=STRATEGY_DELETED strategy_id=%s user_id=%s strategy_name=%q version=%d",
				ev.StrategyID, ev.UserID, deletedName, ev.Version)
		}

	default:
		log.Printf("config consumer: WARN unknown event type %q — skipping", ev.Type)
	}

	c.processed.Add(1)
	_ = c.reader.CommitMessages(ctx, msg)
}

func (c *ConfigConsumer) Stats() (processed int64, errors int64) {
	return c.processed.Load(), c.errors.Load()
}

// (no extra helpers)
