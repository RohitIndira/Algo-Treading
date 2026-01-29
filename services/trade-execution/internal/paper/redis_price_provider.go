package paper

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

// RedisPriceProvider implements PriceProvider using Redis market data
type RedisPriceProvider struct {
	redisClient *redis.Client
	// Cache of successful key patterns per token to avoid trying all patterns every time
	keyPatternCache map[int64]string
	mu              sync.RWMutex
}

// NewRedisPriceProvider creates a new Redis-based price provider
func NewRedisPriceProvider(redisClient *redis.Client) *RedisPriceProvider {
	return &RedisPriceProvider{
		redisClient:     redisClient,
		keyPatternCache: make(map[int64]string),
	}
}

// MarketData represents the structure of market data in Redis
type MarketData struct {
	Token    int64   `json:"token"`
	Symbol   string  `json:"symbol"`
	LTP      float64 `json:"ltp"`
	Open     float64 `json:"open"`
	High     float64 `json:"high"`
	Low      float64 `json:"low"`
	Close    float64 `json:"close"`
	Volume   int64   `json:"volume"`
	Exchange string  `json:"exchange"`
}

// GetLivePrice retrieves the current live price for a token from Redis
func (rpp *RedisPriceProvider) GetLivePrice(ctx context.Context, token int64, exchange string) (float64, error) {
	if token == 0 {
		return 0, fmt.Errorf("invalid token: 0")
	}

	// Try cached key pattern first
	rpp.mu.RLock()
	cachedKey, hasCached := rpp.keyPatternCache[token]
	rpp.mu.RUnlock()

	if hasCached {
		price, err := rpp.getPriceFromKey(ctx, cachedKey)
		if err == nil {
			return price, nil
		}
		// Cache miss or expired, remove from cache
		rpp.mu.Lock()
		delete(rpp.keyPatternCache, token)
		rpp.mu.Unlock()
	}

	// Try different Redis key patterns based on how data-ingestion stores data
	// Pattern 1: market:{exchange}:{token} (lowercase)
	key1 := fmt.Sprintf("market:%s:%d", strings.ToLower(exchange), token)

	// Pattern 2: market:{EXCHANGE}:{token} (uppercase)
	key2 := fmt.Sprintf("market:%s:%d", strings.ToUpper(exchange), token)

	// Pattern 3: market:data:{exchange}:{token}
	key3 := fmt.Sprintf("market:data:%s:%d", strings.ToUpper(exchange), token)

	// Pattern 4: market:{token} (if exchange is not in key)
	key4 := fmt.Sprintf("market:%d", token)

	// Pattern 5: stock:{token}
	key5 := fmt.Sprintf("stock:%d", token)

	keys := []string{key1, key2, key3, key4, key5}

	var lastErr error
	for _, key := range keys {
		price, err := rpp.getPriceFromKey(ctx, key)
		if err == redis.Nil {
			continue // Try next key pattern
		}
		if err != nil {
			lastErr = err
			log.Printf("Error reading Redis key %s: %v", key, err)
			continue
		}

		if price > 0 {
			// Cache this successful key pattern
			rpp.mu.Lock()
			rpp.keyPatternCache[token] = key
			rpp.mu.Unlock()
			return price, nil
		}
	}

	// If we get here, we couldn't find the price
	if lastErr != nil {
		return 0, fmt.Errorf("failed to get live price for token %d: %w", token, lastErr)
	}

	return 0, fmt.Errorf("price not found in Redis for token %d (exchange: %s)", token, exchange)
}

// getPriceFromKey fetches and parses price from a specific Redis key
func (rpp *RedisPriceProvider) getPriceFromKey(ctx context.Context, key string) (float64, error) {
	data, err := rpp.redisClient.Get(ctx, key).Bytes()
	if err != nil {
		return 0, err
	}

	// Parse the JSON data
	var marketData MarketData
	if err := json.Unmarshal(data, &marketData); err != nil {
		return 0, fmt.Errorf("failed to parse market data from key %s: %w", key, err)
	}

	if marketData.LTP > 0 {
		return marketData.LTP, nil
	}

	// If LTP is 0, try Close price as fallback
	if marketData.Close > 0 {
		return marketData.Close, nil
	}

	return 0, fmt.Errorf("no valid price in key %s", key)
}

// GetBatchPrices retrieves prices for multiple tokens at once (more efficient)
func (rpp *RedisPriceProvider) GetBatchPrices(ctx context.Context, tokens []int64, exchange string) (map[int64]float64, error) {
	prices := make(map[int64]float64)
	if len(tokens) == 0 {
		return prices, nil
	}

	// Use pipeline for batch operations
	pipe := rpp.redisClient.Pipeline()

	// Prepare commands for all tokens using cached patterns or default pattern
	cmds := make(map[int64]*redis.StringCmd)
	tokensWithoutCache := make([]int64, 0)

	rpp.mu.RLock()
	for _, token := range tokens {
		if cachedKey, ok := rpp.keyPatternCache[token]; ok {
			// Use cached key pattern
			cmds[token] = pipe.Get(ctx, cachedKey)
		} else {
			// Try primary pattern first
			key := fmt.Sprintf("market:data:%s:%d", strings.ToUpper(exchange), token)
			cmds[token] = pipe.Get(ctx, key)
			tokensWithoutCache = append(tokensWithoutCache, token)
		}
	}
	rpp.mu.RUnlock()

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		log.Printf("Pipeline execution warning: %v", err)
		// Don't fail completely, process individual results
	}

	// Process results
	tokensNeedingFallback := make([]int64, 0)
	for token, cmd := range cmds {
		data, err := cmd.Bytes()
		if err == redis.Nil {
			tokensNeedingFallback = append(tokensNeedingFallback, token)
			continue
		} else if err != nil {
			tokensNeedingFallback = append(tokensNeedingFallback, token)
			continue
		}

		var marketData MarketData
		if err := json.Unmarshal(data, &marketData); err != nil {
			log.Printf("Failed to parse market data for token %d: %v", token, err)
			tokensNeedingFallback = append(tokensNeedingFallback, token)
			continue
		}

		price := 0.0
		if marketData.LTP > 0 {
			price = marketData.LTP
		} else if marketData.Close > 0 {
			price = marketData.Close
		}

		if price > 0 {
			prices[token] = price
		} else {
			tokensNeedingFallback = append(tokensNeedingFallback, token)
		}
	}

	// Try fallback patterns for tokens that failed
	if len(tokensNeedingFallback) > 0 {
		for _, token := range tokensNeedingFallback {
			// Try alternative key patterns
			fallbackKeys := []string{
				fmt.Sprintf("market:%s:%d", strings.ToLower(exchange), token),
				fmt.Sprintf("market:%s:%d", strings.ToUpper(exchange), token),
				fmt.Sprintf("market:%d", token),
				fmt.Sprintf("stock:%d", token),
			}

			for _, key := range fallbackKeys {
				price, err := rpp.getPriceFromKey(ctx, key)
				if err == nil && price > 0 {
					prices[token] = price
					// Cache this successful pattern
					rpp.mu.Lock()
					rpp.keyPatternCache[token] = key
					rpp.mu.Unlock()
					break
				}
			}
		}
	}

	return prices, nil
}

// CheckConnection verifies Redis connection is working
func (rpp *RedisPriceProvider) CheckConnection(ctx context.Context) error {
	return rpp.redisClient.Ping(ctx).Err()
}
