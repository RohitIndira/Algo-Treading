package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// maxTicksScanned bounds the LRANGE read per velocity check. At realistic tick
// rates a 1s window is a handful of entries; 400 is generous headroom.
const maxTicksScanned = 400

// TickHistory reads the recent tick stream that trade-execution's tickstore
// writes to Redis: list key ticks:{exchange}:{token}, RPUSH of {"ltp","ts"}
// JSON, where ts is Unix nanoseconds and exchange is lowercase.
//
// It powers the market-price-protection (velocity) check and is strictly
// best-effort / fail-open: when history is unavailable (different Redis host,
// empty list, parse error, too few ticks) MaxMovePct returns ok=false and the
// caller MUST NOT block the order — missing data can never halt trading.
type TickHistory struct {
	client *redis.Client
	logger *zap.Logger
}

// NewTickHistory opens a Redis client to the tick-store DB. Returns an error if
// the initial ping fails; callers should treat that as non-fatal (pass nil to
// the handler, which disables the velocity check).
func NewTickHistory(addr, password string, db int, logger *zap.Logger) (*TickHistory, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     10,
		MinIdleConns: 2,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("tickstore redis ping: %w", err)
	}
	return &TickHistory{client: client, logger: logger}, nil
}

// Close releases the underlying Redis client.
func (t *TickHistory) Close() error {
	if t == nil || t.client == nil {
		return nil
	}
	return t.client.Close()
}

// MaxMovePct returns the peak-to-trough percentage move across the ticks within
// the most recent `window` for exch:token. ok is false when there is
// insufficient history (fewer than 2 valid ticks in the window) — the caller
// must fail open in that case.
func (t *TickHistory) MaxMovePct(ctx context.Context, exch string, token int64, window time.Duration) (float64, bool) {
	if t == nil || t.client == nil || token <= 0 || window <= 0 {
		return 0, false
	}

	key := fmt.Sprintf("ticks:%s:%d", strings.ToLower(exch), token)
	vals, err := t.client.LRange(ctx, key, -maxTicksScanned, -1).Result()
	if err != nil || len(vals) < 2 {
		return 0, false
	}

	cutoff := time.Now().Add(-window).UnixNano()
	var minLTP, maxLTP float64
	seen := 0
	for _, v := range vals {
		var tick struct {
			LTP float64 `json:"ltp"`
			TS  int64   `json:"ts"`
		}
		if json.Unmarshal([]byte(v), &tick) != nil {
			continue
		}
		if tick.TS < cutoff || tick.LTP <= 0 {
			continue
		}
		if seen == 0 || tick.LTP < minLTP {
			minLTP = tick.LTP
		}
		if seen == 0 || tick.LTP > maxLTP {
			maxLTP = tick.LTP
		}
		seen++
	}

	if seen < 2 || minLTP <= 0 {
		return 0, false
	}
	return (maxLTP - minLTP) / minLTP * 100.0, true
}
