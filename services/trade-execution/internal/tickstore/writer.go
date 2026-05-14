package tickstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/marketws"
	"github.com/go-redis/redis/v8"
)

const (
	batchMax    = 100                      // max ticks flushed per pipeline round-trip
	flushEvery  = 5 * time.Millisecond     // max wait before flushing partial batch
	channelCap  = 50_000                   // buffer: ~25s of full-market burst at 2000 stocks/s
	tickTTL     = 12 * 60 * 60            // 43200 seconds — 12 hours
)

// TickWriter drains TickData from a channel and persists each tick to
// localhost Redis DB=1 using RPUSH + EXPIRE in a batched pipeline.
//
// Key format:  ticks:{exchange}:{token}   e.g. ticks:nse:2475
// Value:       JSON array entries — {"symbol","token","exchange","ltp","ts"}
// TTL:         12 hours (reset on every push)
//
// The drain loop is completely independent of the algo path. If Redis is
// unavailable or slow, ticks are dropped from the buffered channel — the
// WebSocket client and PriceMonitor are never blocked.
type TickWriter struct {
	client *redis.Client
	inCh   chan marketws.TickData
}

// NewTickWriter connects to the given Redis address on DB=1 (isolated from
// the algo's DB=2) and returns a ready TickWriter.
// Returns an error if the initial ping fails; callers should treat this as
// non-fatal and simply skip calling SetTickWriter on the WebSocket client.
func NewTickWriter(addr, password string) (*TickWriter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           1, // isolated from algo Redis (DB=2)
		PoolSize:     3,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("tickstore: redis ping failed: %w", err)
	}

	return &TickWriter{
		client: client,
		inCh:   make(chan marketws.TickData, channelCap),
	}, nil
}

// InCh returns the send-only channel that marketws.Client writes ticks into.
func (tw *TickWriter) InCh() chan<- marketws.TickData {
	return tw.inCh
}

// Start runs the drain loop. Call this in a goroutine: go tw.Start(ctx).
// It exits cleanly when ctx is cancelled or the channel is closed.
func (tw *TickWriter) Start(ctx context.Context) {
	log.Println("[tickstore] drain loop started — writing to localhost Redis DB=1")

	batch := make([]marketws.TickData, 0, batchMax)
	timer := time.NewTimer(flushEvery)
	defer timer.Stop()
	defer tw.client.Close()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		pipe := tw.client.Pipeline()
		for _, t := range batch {
			entry, err := json.Marshal(struct {
				Symbol    string  `json:"symbol"`
				Token     string  `json:"token"`
				Exchange  string  `json:"exchange"`
				LTP       float64 `json:"ltp"`
				Timestamp int64   `json:"ts"`
			}{
				Symbol:    t.Symbol,
				Token:     t.Token,
				Exchange:  t.Exchange,
				LTP:       t.LTP,
				Timestamp: t.Timestamp,
			})
			if err != nil {
				continue
			}

			key := fmt.Sprintf("ticks:%s:%s", t.Exchange, t.Token)
			pipe.RPush(ctx, key, entry)
			pipe.Expire(ctx, key, tickTTL*time.Second)
		}

		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			log.Printf("[tickstore] pipeline error (ticks dropped): %v", err)
		}

		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush() // flush remaining ticks before exit
			log.Println("[tickstore] drain loop stopped")
			return

		case t, ok := <-tw.inCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, t)
			if len(batch) >= batchMax {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(flushEvery)
			}

		case <-timer.C:
			flush()
			timer.Reset(flushEvery)
		}
	}
}
