package livealgos

import (
	"context"
	"fmt"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// Status is what FetchByTokens reports back to the caller so the
// response envelope can carry an explicit `ltp_status` field.
//
// Motivation (memory: reference_ltp_tunnel_silent_fail): the
// staging-redis-tunnel silently half-closes — systemd sees the process
// as active but MGet either times out or returns nil for every key.
// Falling back to `LTP=0` in that case is worse than an empty response
// because 0 becomes real negative netPnL numbers on the UI. Explicit
// UNAVAILABLE lets the UI show "—" instead of "-100%".
type Status string

const (
	// StatusHealthy — probe is up AND MGet succeeded on this call.
	// Result map may still be partial (some tokens have no data yet);
	// per-token missingness is separate from overall LTP-subsystem health.
	StatusHealthy Status = "HEALTHY"

	// StatusUnavailable — probe is down OR MGet failed on this call.
	// Returned map will be nil. Handlers MUST NOT substitute LTP=0.
	StatusUnavailable Status = "UNAVAILABLE"
)

// LTPQuote is one parsed live tick from Redis. Mirrors the JSON
// shape stored at `market:nse:<token>` on the staging LTP feed.
// Fields we don't currently render are still deserialised (harmless)
// so that future features get access without a shape change.
type LTPQuote struct {
	Symbol        string  `json:"symbol"`
	Token         string  `json:"token"`
	Exchange      string  `json:"exchange"`
	LTP           float64 `json:"ltp"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	PrevClose     float64 `json:"prev_close"`
	PercentChange float64 `json:"percent_change"`
	Volume        int64   `json:"volume"`
	Week52High    float64 `json:"week_52_high"`
	Week52Low     float64 `json:"week_52_low"`
}

// LTPStore is a thin wrapper around a *redis.Client that speaks the
// specific key format we use for the live tick feed (`market:nse:<token>`).
// Kept as its own type so the callers depend on a small interface, not
// the whole *redis.Client — makes it swappable for a stub in tests.
//
// Holds a runtime health atomic so handlers can distinguish "tunnel
// silently died" from "no ticks yet" without racing on error checks.
type LTPStore struct {
	rdb     *redis.Client
	healthy atomic.Bool
}

// NewLTPStore wraps an already-connected redis.Client. Caller owns the
// client's lifecycle (opens/closes it in main.go).
//
// The store starts in the UNHEALTHY state until either Start()'s first
// probe succeeds or a caller wires MarkHealthy() (test injection). This
// is fail-safe: a store constructed but never Start()'d reports
// Unavailable rather than pretending everything's fine.
func NewLTPStore(rdb *redis.Client) *LTPStore {
	return &LTPStore{rdb: rdb}
}

// GetString reads one raw key from the feed redis — the admin console's
// EMA-allocation read (manthan:ema:allocations) rides the existing
// connection instead of opening another client. ("", error) when the
// store is unwired or the key is absent.
func (s *LTPStore) GetString(ctx context.Context, key string) (string, error) {
	if s == nil || s.rdb == nil {
		return "", fmt.Errorf("ltp store unwired")
	}
	return s.rdb.Get(ctx, key).Result()
}

// IsHealthy reports the current probe status. Lock-free read.
func (s *LTPStore) IsHealthy() bool {
	if s == nil {
		return false
	}
	return s.healthy.Load()
}

// MarkHealthy is a test hook for hermetic tests that don't want to
// spin up a real Redis. Prod code should let Start() drive the flag.
func (s *LTPStore) MarkHealthy(v bool) {
	if s == nil {
		return
	}
	s.healthy.Store(v)
}

// Start runs the periodic PING probe. Blocks until ctx cancelled — call
// it from a goroutine. Safe to call once per LTPStore; multiple concurrent
// Starts would produce redundant PINGs but no correctness issue.
//
// Probe cadence defaults to 5 s if interval <= 0. First probe fires
// immediately so the store transitions HEALTHY (or stays UNHEALTHY)
// within milliseconds of boot, not after the first interval.
//
// A single PING failure flips the store UNHEALTHY. A subsequent success
// flips it back — no hysteresis by design. The tunnel failure mode we
// care about (per reference_ltp_tunnel_silent_fail) is a hard, sustained
// half-close, not intermittent packet loss.
func (s *LTPStore) Start(ctx context.Context, interval time.Duration) {
	if s == nil || s.rdb == nil {
		return
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}

	s.probeOnce(ctx)

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.probeOnce(ctx)
		}
	}
}

// probeOnce does one PING with a short deadline and updates the flag.
// Logs on transitions so ops can grep for the exact moment the tunnel
// died — silent monitoring was the whole 2026-07-07/08 problem.
func (s *LTPStore) probeOnce(ctx context.Context) {
	pctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	err := s.rdb.Ping(pctx).Err()
	prev := s.healthy.Load()
	now := err == nil
	s.healthy.Store(now)

	if prev != now {
		if now {
			log.Printf("livealgos: LTP probe → HEALTHY (Redis responded)")
		} else {
			log.Printf("livealgos: LTP probe → UNAVAILABLE: %v", err)
		}
	}
}

// FetchByTokens returns a map[token] → LTPQuote for the requested set
// PLUS an explicit Status so the caller can surface HEALTHY/UNAVAILABLE
// to the frontend.
//
// Batched via MGET — one round-trip regardless of how many tokens.
//
// Semantics:
//   - Empty input          → empty map, StatusHealthy (nothing to fetch)
//   - Probe UNHEALTHY      → nil map, StatusUnavailable — MGet skipped
//                            (avoid piling a slow call on a dead tunnel)
//   - MGet fails           → nil map, StatusUnavailable
//                            (and flip healthy=false so subsequent calls
//                            fail fast until probeOnce recovers)
//   - MGet succeeds        → (partial) map, StatusHealthy
//                            Missing / malformed values are silently
//                            omitted — bad Redis state shouldn't block
//                            an otherwise-serviceable response.
//
// Key format: hard-coded to `market:nse:<token>` — every stock we
// track on the Manthan side is on NSE. If we ever start handling BSE,
// this will need to accept an exchange prefix per token.
func (s *LTPStore) FetchByTokens(ctx context.Context, tokens []string) (map[string]LTPQuote, Status) {
	if len(tokens) == 0 {
		// Vacuously healthy — no ticks to fetch means the LTP subsystem
		// wasn't stressed at all. Handlers with zero ACTIVE positions
		// don't get penalised by a dead tunnel.
		return map[string]LTPQuote{}, StatusHealthy
	}
	if s == nil || s.rdb == nil {
		return nil, StatusUnavailable
	}
	if !s.healthy.Load() {
		// Fail fast on a known-dead tunnel. Saves a per-request 2s timeout.
		return nil, StatusUnavailable
	}

	keys := make([]string, len(tokens))
	for i, t := range tokens {
		keys[i] = "market:nse:" + t
	}

	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		// MGet failure ⇒ tunnel likely dead. Flip flag so the next few
		// FetchByTokens calls short-circuit without waiting on a timeout;
		// the periodic probe will recover if the tunnel comes back.
		s.healthy.Store(false)
		log.Printf("livealgos: LTP MGet %d keys failed: %v", len(keys), err)
		return nil, StatusUnavailable
	}

	out := make(map[string]LTPQuote, len(vals))
	for i, v := range vals {
		if v == nil {
			// Token not in Redis — market feed hasn't seen it yet, or
			// the symbol isn't in our tracked universe. Not a failure.
			continue
		}
		raw, ok := v.(string)
		if !ok {
			log.Printf("livealgos: LTP unexpected type %T for token %s", v, tokens[i])
			continue
		}
		var q LTPQuote
		if err := json.Unmarshal([]byte(raw), &q); err != nil {
			log.Printf("livealgos: LTP unmarshal failed for token %s: %v", tokens[i], err)
			continue
		}
		out[tokens[i]] = q
	}
	return out, StatusHealthy
}
