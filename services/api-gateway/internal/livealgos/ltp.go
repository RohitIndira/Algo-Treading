package livealgos

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
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
type LTPStore struct {
	rdb *redis.Client
}

// NewLTPStore wraps an already-connected redis.Client. Caller owns the
// client's lifecycle (opens/closes it in main.go).
func NewLTPStore(rdb *redis.Client) *LTPStore {
	return &LTPStore{rdb: rdb}
}

// FetchByTokens returns a map[token] → LTPQuote for the requested set.
// Batched via MGET — one round-trip regardless of how many tokens.
//
// Semantics:
//   - Empty input      → empty map, nil error
//   - Missing token    → omitted from the result (not treated as an error)
//   - Malformed value  → logged, then omitted (bad Redis state shouldn't
//                        block an otherwise-serviceable response)
//   - Redis unreachable→ error surfaced up so the handler can decide
//                        whether to degrade gracefully (return positions
//                        without LTP) or fail
//
// Key format: hard-coded to `market:nse:<token>` — every stock we
// track on the Manthan side is on NSE. If we ever start handling BSE,
// this will need to accept an exchange prefix per token.
func (s *LTPStore) FetchByTokens(ctx context.Context, tokens []string) (map[string]LTPQuote, error) {
	if len(tokens) == 0 {
		return map[string]LTPQuote{}, nil
	}

	keys := make([]string, len(tokens))
	for i, t := range tokens {
		keys[i] = "market:nse:" + t
	}

	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("livealgos: LTP MGet %d keys: %w", len(keys), err)
	}

	out := make(map[string]LTPQuote, len(vals))
	for i, v := range vals {
		if v == nil {
			// Token not in Redis — market feed hasn't seen it yet, or
			// the symbol isn't in our tracked universe. Not an error.
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
	return out, nil
}
