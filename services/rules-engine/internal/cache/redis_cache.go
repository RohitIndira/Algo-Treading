package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// RedisCache is a minimal Redis wrapper used by rules-engine for:
//   - LTP lookup (Get)
//   - Pub/Sub publishing (Publish)
//
// It intentionally does NOT implement the previous StrategyCache.
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	logger *zap.Logger
}

type Config struct {
	Addrs       []string
	Password    string
	DB          int
	PoolSize    int
	MinIdleConn int
	CacheTTL    time.Duration
	ClusterMode bool
}

func NewRedisCache(addrs []string, password string, db int, poolSize int, minIdleConns int, ttl time.Duration, clusterMode bool, logger *zap.Logger) (*RedisCache, error) {
	_ = clusterMode // cluster mode not yet supported in this lightweight wrapper
	if len(addrs) == 0 {
		return nil, fmt.Errorf("redis addrs empty")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	opt := &redis.Options{
		Addr:         addrs[0],
		Password:     password,
		DB:           db,
		PoolSize:     poolSize,
		MinIdleConns: minIdleConns,
	}

	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisCache{client: client, ttl: ttl, logger: logger}, nil
}

func (c *RedisCache) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

// Raw exposes the underlying go-redis client so shared helpers (e.g. the
// tradecap per-strategy trade counter) can run their own scripts on the same
// connection pool. Returns nil when the cache is not initialized.
func (c *RedisCache) Raw() *redis.Client {
	if c == nil {
		return nil
	}
	return c.client
}

func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	if c == nil || c.client == nil {
		return "", fmt.Errorf("redis cache not initialized")
	}
	return c.client.Get(ctx, key).Result()
}

// MGet fetches many keys in a single round-trip. The returned slice is aligned
// 1:1 with keys; a missing key yields a nil element. Used by the AMN snapshot /
// preview to price hundreds of stocks without N serial GETs.
func (c *RedisCache) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("redis cache not initialized")
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return c.client.MGet(ctx, keys...).Result()
}

func (c *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("redis cache not initialized")
	}
	if ttl <= 0 {
		ttl = c.ttl
	}
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c *RedisCache) Publish(ctx context.Context, channel string, message interface{}) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("redis cache not initialized")
	}
	return c.client.Publish(ctx, channel, message).Err()
}

// GetFloat returns a float64 stored at key. Returns 0 (no error) when the key
// does not exist so callers can treat a missing exposure counter as zero.
func (c *RedisCache) GetFloat(ctx context.Context, key string) (float64, error) {
	if c == nil || c.client == nil {
		return 0, fmt.Errorf("redis cache not initialized")
	}
	val, err := c.client.Get(ctx, key).Float64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// IncrByFloat atomically increments a float64 counter at key and returns the
// new value. Creates the key with value `incr` if it does not yet exist.
func (c *RedisCache) IncrByFloat(ctx context.Context, key string, incr float64) (float64, error) {
	if c == nil || c.client == nil {
		return 0, fmt.Errorf("redis cache not initialized")
	}
	return c.client.IncrByFloat(ctx, key, incr).Result()
}
