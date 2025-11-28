package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// Config holds Redis configuration
type Config struct {
	Address      string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Client wraps redis.Client with additional functionality
type Client struct {
	*redis.Client
	config Config
}

// New creates a new Redis client
func New(config Config) (*Client, error) {
	// Set default values
	if config.PoolSize == 0 {
		config.PoolSize = 100
	}
	if config.MinIdleConns == 0 {
		config.MinIdleConns = 10
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 3 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 3 * time.Second
	}

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:         config.Address,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		MaxRetries:   config.MaxRetries,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	return &Client{
		Client: client,
		config: config,
	}, nil
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.Client.Close()
}

// HealthCheck performs a health check on the Redis connection
func (c *Client) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return c.Ping(ctx).Err()
}

// SetWithExpiry sets a key-value pair with expiration
func (c *Client) SetWithExpiry(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.Set(ctx, key, value, expiration).Err()
}

// GetString gets a string value by key
func (c *Client) GetString(ctx context.Context, key string) (string, error) {
	return c.Get(ctx, key).Result()
}

// Delete deletes one or more keys
func (c *Client) Delete(ctx context.Context, keys ...string) error {
	return c.Del(ctx, keys...).Err()
}

// Exists checks if key exists
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	result, err := c.Client.Exists(ctx, key).Result()
	return result > 0, err
}

// Increment increments a key by 1
func (c *Client) Increment(ctx context.Context, key string) (int64, error) {
	return c.Incr(ctx, key).Result()
}

// IncrementBy increments a key by a specific amount
func (c *Client) IncrementBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.IncrBy(ctx, key, value).Result()
}

// Decrement decrements a key by 1
func (c *Client) Decrement(ctx context.Context, key string) (int64, error) {
	return c.Decr(ctx, key).Result()
}

// DecrementBy decrements a key by a specific amount
func (c *Client) DecrementBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.DecrBy(ctx, key, value).Result()
}

// HashSet sets a field in a hash
func (c *Client) HashSet(ctx context.Context, key, field string, value interface{}) error {
	return c.HSet(ctx, key, field, value).Err()
}

// HashGet gets a field from a hash
func (c *Client) HashGet(ctx context.Context, key, field string) (string, error) {
	return c.HGet(ctx, key, field).Result()
}

// HashGetAll gets all fields from a hash
func (c *Client) HashGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.HGetAll(ctx, key).Result()
}

// HashDelete deletes fields from a hash
func (c *Client) HashDelete(ctx context.Context, key string, fields ...string) error {
	return c.HDel(ctx, key, fields...).Err()
}

// ListPush pushes an element to the end of a list
func (c *Client) ListPush(ctx context.Context, key string, values ...interface{}) error {
	return c.RPush(ctx, key, values...).Err()
}

// ListPop pops an element from the beginning of a list
func (c *Client) ListPop(ctx context.Context, key string) (string, error) {
	return c.LPop(ctx, key).Result()
}

// ListLength gets the length of a list
func (c *Client) ListLength(ctx context.Context, key string) (int64, error) {
	return c.LLen(ctx, key).Result()
}

// SetAdd adds members to a set
func (c *Client) SetAdd(ctx context.Context, key string, members ...interface{}) error {
	return c.SAdd(ctx, key, members...).Err()
}

// SetMembers gets all members of a set
func (c *Client) SetMembers(ctx context.Context, key string) ([]string, error) {
	return c.SMembers(ctx, key).Result()
}

// SetIsMember checks if a value is a member of a set
func (c *Client) SetIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	return c.SIsMember(ctx, key, member).Result()
}

// SetRemove removes members from a set
func (c *Client) SetRemove(ctx context.Context, key string, members ...interface{}) error {
	return c.SRem(ctx, key, members...).Err()
}

// Expire sets a timeout on a key
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return c.Client.Expire(ctx, key, expiration).Err()
}

// TTL gets the time to live for a key
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.Client.TTL(ctx, key).Result()
}
