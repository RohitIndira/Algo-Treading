// Package tradecap implements the per-strategy, per-day "committed trades"
// counter that enforces a strategy's MaxTradesPerStrategy limit.
//
// Only *actual trades* consume a slot: an immediate order at publish time
// (rules-engine) and a price-monitor watch at the moment its target triggers
// and the entry is sent (trade-execution). Watches that are merely waiting for
// a price level do NOT reserve a slot, so monitoring a stock never eats the cap.
//
// The counter lives in Redis so the limit is a single source of truth shared by
// every rules-engine replica and the trade-execution price monitor. Reservation
// is atomic (a Lua INCR), which makes the cap a true hard ceiling and enforces
// first-come-first-served when several stocks trigger in the same instant:
// concurrent reservations are serialized by Redis, exactly `limit` of them win,
// and the rest are rejected.
package tradecap

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// keyPrefix namespaces the per-strategy daily counter keys.
	keyPrefix = "strat:trades"
	// defaultTTL keeps the daily key alive comfortably past one IST trading day
	// so it self-cleans without a cron. The IST date is part of the key, so a
	// stale key can never bleed into the next day's count.
	defaultTTL = 26 * time.Hour
)

// CapReached is returned by Reserve when the strategy has already used all of
// its daily trade slots. It is a sentinel count, not an error.
const CapReached int64 = -1

// IST is India Standard Time (UTC+5:30) — the boundary that defines a trading
// day. Both services must compute the day key from the same zone or they would
// key into different counters near midnight.
var IST = time.FixedZone("IST", 5*60*60+30*60)

// Key builds the counter key for a strategy on the IST day of `now`.
// Exported so tests and operators can compute/inspect the exact key.
func Key(strategyID string, now time.Time) string {
	return fmt.Sprintf("%s:%s:%s", keyPrefix, strategyID, now.In(IST).Format("2006-01-02"))
}

// reserveScript atomically claims one slot. It optionally seeds the counter from
// a durable committed-trade base (ARGV[3]) the first time the key is touched,
// so a Redis flush mid-day cannot silently re-open a hard cap. Returns the new
// count, or -1 (CapReached) when the strategy is already at its limit.
//
//	KEYS[1] = counter key
//	ARGV[1] = limit (<=0 means unlimited)
//	ARGV[2] = ttl seconds
//	ARGV[3] = seed (committed-trade base; applied only if key absent, 0 = skip)
var reserveScript = redis.NewScript(`
local key   = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl   = tonumber(ARGV[2])
local seed  = tonumber(ARGV[3])
if redis.call('EXISTS', key) == 0 and seed > 0 then
  redis.call('SET', key, seed)
end
local n = redis.call('INCR', key)
redis.call('EXPIRE', key, ttl)
if limit > 0 and n > limit then
  redis.call('DECR', key)
  return -1
end
return n
`)

// releaseScript decrements the counter but never below zero. Used to hand a
// reserved slot back when a placement definitively fails after reserving.
var releaseScript = redis.NewScript(`
local key = KEYS[1]
local n = tonumber(redis.call('GET', key) or '0')
if n > 0 then
  return redis.call('DECR', key)
end
return 0
`)

// Reserver is a thin, concurrency-safe handle over a Redis client. Construct one
// per service from that service's shared Redis client and reuse it.
type Reserver struct {
	rdb *redis.Client
	ttl time.Duration
}

// New builds a Reserver over the given Redis client. Returns nil if rdb is nil
// so callers can treat "no Redis" as "enforcement disabled" via IsEnabled.
func New(rdb *redis.Client) *Reserver {
	if rdb == nil {
		return nil
	}
	return &Reserver{rdb: rdb, ttl: defaultTTL}
}

// IsEnabled reports whether the reserver can talk to Redis. Callers use it to
// decide their fail-open/fail-closed behaviour when Redis was never available.
func (r *Reserver) IsEnabled() bool {
	return r != nil && r.rdb != nil
}

// Reserve atomically claims one trade slot for the strategy on the IST day of
// `now`. limit <= 0 means unlimited (fast no-op that still tracks the count).
// seed initializes the counter from a durable committed-trade count, applied
// only when the key does not yet exist; pass 0 to skip seeding.
//
// Returns the reserved slot number (>= 1) on success, or CapReached (-1) when
// the strategy is already at its limit. A non-nil error means Redis failed and
// the caller must decide how to fail.
func (r *Reserver) Reserve(ctx context.Context, strategyID string, limit int32, seed int64, now time.Time) (int64, error) {
	if !r.IsEnabled() {
		return 0, fmt.Errorf("tradecap: reserver not initialized")
	}
	return reserveScript.Run(ctx, r.rdb, []string{Key(strategyID, now)},
		limit, int(r.ttl.Seconds()), seed).Int64()
}

// Release hands one previously reserved slot back (best-effort, floored at 0).
// Call only when a reservation you made will definitely not become a trade,
// e.g. the Kafka publish or broker placement failed after reserving.
func (r *Reserver) Release(ctx context.Context, strategyID string, now time.Time) error {
	if !r.IsEnabled() {
		return fmt.Errorf("tradecap: reserver not initialized")
	}
	return releaseScript.Run(ctx, r.rdb, []string{Key(strategyID, now)}).Err()
}

// Reset clears the strategy's counter for the IST day of `now` so the daily cap
// starts fresh. Call on strategy (re)activation to mirror the previous
// "count only signals since last activation" semantic.
func (r *Reserver) Reset(ctx context.Context, strategyID string, now time.Time) error {
	if !r.IsEnabled() {
		return fmt.Errorf("tradecap: reserver not initialized")
	}
	return r.rdb.Del(ctx, Key(strategyID, now)).Err()
}
