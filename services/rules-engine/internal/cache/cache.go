package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// Cache interface defines cache operations
type Cache interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	GetMulti(ctx context.Context, keys []string) (map[string]string, error)
	SetMulti(ctx context.Context, items map[string]interface{}, ttl time.Duration) error
	DeletePattern(ctx context.Context, pattern string) error
	Close() error
}

// RedisCache implements Cache using Redis
type RedisCache struct {
	client redis.UniversalClient
	ttl    time.Duration
	logger *zap.Logger
}

// NewRedisCache creates a new Redis cache client
func NewRedisCache(addrs []string, password string, db int, poolSize, minIdleConns int, ttl time.Duration, clusterMode bool, logger *zap.Logger) (*RedisCache, error) {
	var client redis.UniversalClient

	if clusterMode {
		// Redis Cluster mode
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			Password:     password,
			PoolSize:     poolSize,
			MinIdleConns: minIdleConns,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolTimeout:  4 * time.Second,
		})
	} else {
		// Single Redis instance or Sentinel mode
		client = redis.NewClient(&redis.Options{
			Addr:         addrs[0],
			Password:     password,
			DB:           db,
			PoolSize:     poolSize,
			MinIdleConns: minIdleConns,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolTimeout:  4 * time.Second,
		})
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	logger.Info("Redis cache initialized",
		zap.Strings("addrs", addrs),
		zap.Bool("cluster_mode", clusterMode),
		zap.Duration("ttl", ttl))

	return &RedisCache{
		client: client,
		ttl:    ttl,
		logger: logger,
	}, nil
}

// Get retrieves a value from cache
func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", models.ErrCacheMiss
	}
	if err != nil {
		return "", fmt.Errorf("redis get error: %w", err)
	}
	return val, nil
}

// Set stores a value in cache
func (c *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if ttl == 0 {
		ttl = c.ttl
	}

	var data string
	switch v := value.(type) {
	case string:
		data = v
	default:
		bytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}
		data = string(bytes)
	}

	err := c.client.Set(ctx, key, data, ttl).Err()
	if err != nil {
		return fmt.Errorf("redis set error: %w", err)
	}

	return nil
}

// Delete removes a value from cache
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("redis delete error: %w", err)
	}
	return nil
}

// Exists checks if a key exists in cache
func (c *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	count, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists error: %w", err)
	}
	return count > 0, nil
}

// GetMulti retrieves multiple values from cache
func (c *RedisCache) GetMulti(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget error: %w", err)
	}

	result := make(map[string]string, len(keys))
	for i, val := range values {
		if val != nil {
			result[keys[i]] = val.(string)
		}
	}

	return result, nil
}

// SetMulti stores multiple values in cache
func (c *RedisCache) SetMulti(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	if ttl == 0 {
		ttl = c.ttl
	}

	pipe := c.client.Pipeline()
	for key, value := range items {
		var data string
		switch v := value.(type) {
		case string:
			data = v
		default:
			bytes, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
			}
			data = string(bytes)
		}
		pipe.Set(ctx, key, data, ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline error: %w", err)
	}

	return nil
}

// DeletePattern deletes keys matching a pattern
func (c *RedisCache) DeletePattern(ctx context.Context, pattern string) error {
	iter := c.client.Scan(ctx, 0, pattern, 100).Iterator()

	keys := []string{}
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("redis scan error: %w", err)
	}

	if len(keys) > 0 {
		if err := c.client.Del(ctx, keys...).Err(); err != nil {
			return fmt.Errorf("redis delete error: %w", err)
		}
		c.logger.Debug("Deleted keys matching pattern",
			zap.String("pattern", pattern),
			zap.Int("count", len(keys)))
	}

	return nil
}

// Close closes the Redis connection
func (c *RedisCache) Close() error {
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("failed to close redis client: %w", err)
	}
	c.logger.Info("Redis cache closed")
	return nil
}

// Ping checks Redis connectivity
func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// GetStats returns Redis statistics
func (c *RedisCache) GetStats(ctx context.Context) (*redis.PoolStats, error) {
	stats := c.client.PoolStats()
	return stats, nil
}
