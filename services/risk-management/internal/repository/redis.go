package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisRepository struct {
	client *redis.Client
}

func NewRedisRepository(addr string) *RedisRepository {
	return &RedisRepository{client: redis.NewClient(&redis.Options{Addr: addr})}
}

func (r *RedisRepository) IncrementDailyTradeCount(ctx context.Context, userID, orderID string) error {
	key := fmt.Sprintf("user:%s:trades:daily", userID)
	return r.client.ZAdd(ctx, key, &redis.Z{Score: float64(time.Now().Unix()), Member: orderID}).Err()
}

func (r *RedisRepository) GetTodayTradeCount(ctx context.Context, userID string) (int64, error) {
	key := fmt.Sprintf("user:%s:trades:daily", userID)
	today := time.Now().Truncate(24 * time.Hour).Unix()
	return r.client.ZCount(ctx, key, fmt.Sprintf("%d", today), "+inf").Result()
}

func (r *RedisRepository) AddDailyLoss(ctx context.Context, userID string, loss float64) error {
	key := fmt.Sprintf("user:%s:loss:daily", userID)
	return r.client.IncrByFloat(ctx, key, loss).Err()
}

func (r *RedisRepository) GetDailyLoss(ctx context.Context, userID string) (float64, error) {
	key := fmt.Sprintf("user:%s:loss:daily", userID)
	f, err := r.client.Get(ctx, key).Float64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return f, nil
}

// GetUserBalance returns the user's available balance stored in Redis (string -> float64)
func (r *RedisRepository) GetUserBalance(ctx context.Context, userID string) (float64, error) {
	key := fmt.Sprintf("user:%s:balance", userID)
	// attempt to read as float using helper
	f, err := r.client.Get(ctx, key).Float64()
	if err == redis.Nil {
		return 0, nil
	}
	return f, err
}

// GetRiskProfile returns a simple risk profile string stored by the user (e.g., "conservative", "moderate", "aggressive")
func (r *RedisRepository) GetRiskProfile(ctx context.Context, userID string) (string, error) {
	key := fmt.Sprintf("user:%s:risk_profile", userID)
	s, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return s, err
}

func (r *RedisRepository) SetPosition(ctx context.Context, userID string, stockCode int64, quantity int, avgPrice float64) error {
	key := fmt.Sprintf("user:%s:positions:%d", userID, stockCode)
	return r.client.HSet(ctx, key, "quantity", quantity, "avg_price", avgPrice, "last_update", time.Now().Unix()).Err()
}

func (r *RedisRepository) GetPositionHash(ctx context.Context, userID string, stockCode int64) (map[string]string, error) {
	key := fmt.Sprintf("user:%s:positions:%d", userID, stockCode)
	return r.client.HGetAll(ctx, key).Result()
}

func (r *RedisRepository) GetUserPositionsKeys(ctx context.Context, userID string) ([]string, error) {
	pattern := fmt.Sprintf("user:%s:positions:*", userID)
	return r.client.Keys(ctx, pattern).Result()
}

func (r *RedisRepository) AddDuplicateOrder(ctx context.Context, userID, fingerprint, orderID string, windowSec int) error {
	key := fmt.Sprintf("user:%s:orders:fingerprint:%s", userID, fingerprint)
	// set with TTL; value is orderID
	return r.client.Set(ctx, key, orderID, time.Duration(windowSec)*time.Second).Err()
}

func (r *RedisRepository) CheckDuplicateOrder(ctx context.Context, userID, fingerprint string) (bool, error) {
	key := fmt.Sprintf("user:%s:orders:fingerprint:%s", userID, fingerprint)
	exists, err := r.client.Exists(ctx, key).Result()
	return exists > 0, err
}

func (r *RedisRepository) ResetDailyCounters(ctx context.Context, userID string) error {
	tradeKey := fmt.Sprintf("user:%s:trades:daily", userID)
	lossKey := fmt.Sprintf("user:%s:loss:daily", userID)
	_, err := r.client.Del(ctx, tradeKey, lossKey).Result()
	return err
}
