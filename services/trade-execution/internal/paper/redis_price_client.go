package paper

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// MarketTick is the JSON structure stored in Redis under market:{exchange}:{token} keys.
type MarketTick struct {
	Symbol    string  `json:"symbol"`
	Token     string  `json:"token"`
	Exchange  string  `json:"exchange"`
	LTP       float64 `json:"ltp"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	PrevClose float64 `json:"prev_close"`
	Volume    int64   `json:"volume"`
	Timestamp int64   `json:"timestamp"`
}

// RedisPriceClient reads live market prices from Redis.
//
// Key format: market:{exchange_lowercase}:{token}
// Example:    market:nse:2475
type RedisPriceClient struct {
	client *redis.Client
}

// NewRedisPriceClient creates a Redis client for market price lookups.
// Returns an error if the initial connection ping fails; callers should treat
// this as non-fatal and pass nil to the monitor/executor when unavailable.
func NewRedisPriceClient(addr, password string) (*RedisPriceClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           0,
		PoolSize:     5,
		MinIdleConns: 2,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis price client: ping failed: %w", err)
	}

	return &RedisPriceClient{client: client}, nil
}

// GetLTP returns the current last-traded-price for the given exchange and token.
//
//	exchange: "NSE" or "BSE" (case-insensitive)
//	token:    instrument token / StockCode  (e.g. 2475)
//
// Returns an error if the key is missing, expired, or LTP is 0.
func (r *RedisPriceClient) GetLTP(ctx context.Context, exchange string, token int64) (float64, error) {
	if r == nil || r.client == nil {
		return 0, fmt.Errorf("redis price client is nil")
	}

	key := fmt.Sprintf("market:%s:%d", strings.ToLower(exchange), token)

	raw, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redis GET %s: %w", key, err)
	}

	var tick MarketTick
	if err := json.Unmarshal([]byte(raw), &tick); err != nil {
		return 0, fmt.Errorf("redis unmarshal %s: %w", key, err)
	}

	if tick.LTP <= 0 {
		return 0, fmt.Errorf("redis: ltp=0 for key %s", key)
	}

	return tick.LTP, nil
}

// Close closes the underlying Redis connection.
func (r *RedisPriceClient) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}
