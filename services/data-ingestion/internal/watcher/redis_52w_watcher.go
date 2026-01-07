package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	redispkg "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"

	"go.uber.org/zap"
)

// MarketSnapshot represents the subset of fields we care about from the
// real-time market feed stored in Redis under keys like:
//
//	market:nse:<token>
//	market:bse:<token>
//
// We intentionally keep this minimal; the full JSON is forwarded to Kafka
// unchanged so downstream consumers can evolve independently.
type MarketSnapshot struct {
	Symbol          string  `json:"symbol"`
	Token           string  `json:"token"`
	Exchange        string  `json:"exchange"`
	LTP             float64 `json:"ltp"`
	Week52High      float64 `json:"week_52_high"`
	Week52Low       float64 `json:"week_52_low"`
	Week52HighDate  string  `json:"week_52_high_date"`
	IsNewWeek52High bool    `json:"is_new_week_52_high"`
	IsNewWeek52Low  bool    `json:"is_new_week_52_low"`
	Timestamp       int64   `json:"timestamp"`
	LastUpdated     string  `json:"last_updated"`
}

// Redis52WWatcher scans the external Redis market store and publishes
// a Kafka event the first time a symbol breaks its 52-week high on a given day.
//
// This is intentionally stateless across restarts (in-memory dedupe only);
// downstream consumers must be idempotent. The goal is to avoid re-scanning
// all symbols per user request and instead maintain a continuous stream of
// "today's 52-week breakouts".
type Redis52WWatcher struct {
	client       *redispkg.Client
	pub          publisher.Publisher
	lgr          *logger.Logger
	pollInterval time.Duration

	mu         sync.Mutex
	seenToday  map[string]struct{} // key: YYYY-MM-DD|exchange|token
	lastDayStr string
}

// NewRedis52WWatcher constructs a new watcher.
func NewRedis52WWatcher(client *redispkg.Client, pub publisher.Publisher, pollInterval time.Duration, lgr *logger.Logger) *Redis52WWatcher {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	return &Redis52WWatcher{
		client:       client,
		pub:          pub,
		lgr:          lgr,
		pollInterval: pollInterval,
		seenToday:    make(map[string]struct{}),
		lastDayStr:   todayStr(),
	}
}

// Run starts the polling loop until the context is cancelled.
func (w *Redis52WWatcher) Run(ctx context.Context) error {
	w.lgr.Info("started redis 52w-high watcher",
		zap.Duration("poll_interval", w.pollInterval))

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	// Initial scan so we don't wait for the first tick.
	if err := w.scanOnce(ctx); err != nil {
		w.lgr.Error("initial redis 52w scan error", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			w.lgr.Info("redis 52w-high watcher stopping", zap.Error(ctx.Err()))
			return ctx.Err()
		case <-ticker.C:
			if err := w.scanOnce(ctx); err != nil {
				w.lgr.Error("redis 52w scan error", zap.Error(err))
			}
		}
	}
}

// scanOnce scans both NSE and BSE market keys and publishes any new
// 52-week high breakouts for the current day.
func (w *Redis52WWatcher) scanOnce(ctx context.Context) error {
	// Reset daily dedupe if we moved to a new day.
	w.resetIfNewDay()

	if err := w.scanPattern(ctx, "market:nse:*"); err != nil {
		return err
	}
	if err := w.scanPattern(ctx, "market:bse:*"); err != nil {
		return err
	}
	return nil
}

func (w *Redis52WWatcher) scanPattern(ctx context.Context, pattern string) error {
	// Use SCAN to avoid blocking Redis for large keyspaces.
	iter := w.client.Scan(ctx, 0, pattern, 1000).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()

		// Read raw JSON value.
		raw, err := w.client.Get(ctx, key).Result()
		if err != nil {
			// Ignore keys that disappear between SCAN and GET.
			w.lgr.Warn("redis get failure for market key",
				zap.String("key", key),
				zap.Error(err))
			continue
		}

		var snap MarketSnapshot
		if err := json.Unmarshal([]byte(raw), &snap); err != nil {
			w.lgr.Warn("failed to unmarshal market snapshot",
				zap.String("key", key),
				zap.Error(err))
			continue
		}

		if !snap.IsNewWeek52High {
			continue
		}

		// Only consider symbols that actually broke 52-week high **today**.
		// We enforce two things:
		//   1) week_52_high_date must be today's date (the breakout day), and
		//   2) last_updated (or timestamp) must also be today, so we don't
		//      re-emit old breakouts carried over in Redis.
		if !isBreakoutToday(snap) {
			continue
		}

		// Basic normalization of exchange name (nse/bse -> NSE/BSE).
		exch := strings.ToUpper(snap.Exchange)
		if exch == "" {
			// Fallback: infer from key prefix.
			if strings.HasPrefix(key, "market:nse:") {
				exch = "NSE"
			} else if strings.HasPrefix(key, "market:bse:") {
				exch = "BSE"
			}
		}
		snap.Exchange = exch

		if snap.Token == "" && snap.Symbol == "" {
			w.lgr.Warn("market snapshot missing token/symbol, skipping",
				zap.String("key", key))
			continue
		}

		if w.alreadySeenToday(snap) {
			continue
		}

		// Publish the original JSON payload so downstream consumers
		// receive the full market snapshot as stored in Redis.
		if err := w.pub.Publish(ctx, []byte(snap.Token), []byte(raw)); err != nil {
			w.lgr.Error("failed to publish 52w breakout event",
				zap.String("key", key),
				zap.String("token", snap.Token),
				zap.Error(err))
			continue
		}

		w.markSeenToday(snap)

		w.lgr.Info("published 52w-high breakout",
			zap.String("token", snap.Token),
			zap.String("symbol", snap.Symbol),
			zap.String("exchange", snap.Exchange),
			zap.Float64("ltp", snap.LTP),
			zap.Float64("week_52_high", snap.Week52High))
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("redis scan error for pattern %s: %w", pattern, err)
	}

	return nil
}

func todayStr() string {
	return time.Now().Format("2006-01-02")
}

// isBreakoutToday returns true if:
//   - the 52-week high date (week_52_high_date) is today, AND
//   - the snapshot's last_updated (or, as a fallback, timestamp) is also today.
//
// This matches the requirement: "push all tokens that were updated today AND
// broke 52-week high today", but not old breakouts from previous days.
func isBreakoutToday(snap MarketSnapshot) bool {
	day := todayStr()

	// 1) The recorded 52-week high date must be today.
	if snap.Week52HighDate != "" && snap.Week52HighDate != day {
		return false
	}

	// 2) Prefer parsing the explicit last_updated timestamp if present.
	if snap.LastUpdated != "" {
		if t, err := time.Parse(time.RFC3339Nano, snap.LastUpdated); err == nil {
			local := t.Local()
			return local.Format("2006-01-02") == day
		}
	}

	// 3) Fallback: use the millisecond epoch "timestamp" field if non-zero.
	if snap.Timestamp != 0 {
		t := time.Unix(0, snap.Timestamp*int64(time.Millisecond)).Local()
		return t.Format("2006-01-02") == day
	}

	// If we have no reliable time information, be conservative and treat it as
	// not-from-today so we don't emit stale breakouts.
	return false
}

// resetIfNewDay clears the in-memory dedupe map when day boundary changes.
func (w *Redis52WWatcher) resetIfNewDay() {
	w.mu.Lock()
	defer w.mu.Unlock()

	day := todayStr()
	if day != w.lastDayStr {
		w.seenToday = make(map[string]struct{})
		w.lastDayStr = day
	}
}

func (w *Redis52WWatcher) alreadySeenToday(snap MarketSnapshot) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := fmt.Sprintf("%s|%s|%s", w.lastDayStr, snap.Exchange, snap.Token)
	_, exists := w.seenToday[key]
	return exists
}

func (w *Redis52WWatcher) markSeenToday(snap MarketSnapshot) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := fmt.Sprintf("%s|%s|%s", w.lastDayStr, snap.Exchange, snap.Token)
	w.seenToday[key] = struct{}{}
}
