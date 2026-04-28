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

// Client returns the underlying Redis client for direct access.
func (c *RedisCache) Client() *redis.Client {
	return c.client
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

func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	if c == nil || c.client == nil {
		return "", fmt.Errorf("redis cache not initialized")
	}
	return c.client.Get(ctx, key).Result()
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
