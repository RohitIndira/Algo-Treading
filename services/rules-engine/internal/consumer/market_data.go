package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// MarketDataResult holds all market data fields extracted from a single Redis GET.
// Consolidating LTP, PrevClose, and TickSize into one struct eliminates
// redundant Redis calls on the hot path.
type MarketDataResult struct {
	LTP           float64
	PrevClose     float64
	TickSize      float64
	PercentChange float64
	// Volume is the day-cumulative traded quantity from the same Redis payload.
	// Used with LTP to derive trade value (turnover) for the trade-value filter.
	Volume   int64
	DPRLower float64 // exchange daily price range lower band (0 = unavailable)
	DPRUpper float64 // exchange daily price range upper band (0 = unavailable)
	Exchange string  // which exchange the data came from ("nse" or "bse")
	Token    int64   // the token used for the Redis key
}

// TickSizeCache is a concurrency-safe in-memory cache for per-stock tick sizes.
// Tick sizes are static during a trading day, so caching them avoids repeated
// Redis look-ups after the first fetch.
type TickSizeCache struct {
	mu    sync.RWMutex
	ticks map[int64]float64 // stockCode → tickSize
}

// NewTickSizeCache creates a new tick size cache.
func NewTickSizeCache() *TickSizeCache {
	return &TickSizeCache{
		ticks: make(map[int64]float64),
	}
}

// Get returns the cached tick size for a stock code, or 0 if not cached.
func (c *TickSizeCache) Get(stockCode int64) (float64, bool) {
	c.mu.RLock()
	ts, ok := c.ticks[stockCode]
	c.mu.RUnlock()
	return ts, ok
}

// Set stores a tick size for a stock code.
func (c *TickSizeCache) Set(stockCode int64, tickSize float64) {
	c.mu.Lock()
	c.ticks[stockCode] = tickSize
	c.mu.Unlock()
}

// getMarketDataFromRedis fetches LTP, prev_close, and tick_size from Redis in
// a single GET call. It tries both NSE and BSE exchanges (preferred exchange
// first) and returns all three values from the first successful lookup.
//
// This replaces the separate getLTPFromRedis() and getPrevCloseFromRedis()
// functions, reducing Redis round-trips from 2-3 to exactly 1.
func getMarketDataFromRedis(
	ctx context.Context,
	redisCache *cache.RedisCache,
	stockData models.StockData,
	logger *zap.Logger,
) (*MarketDataResult, error) {
	exchangeLower := strings.ToLower(stockData.Exchange)
	exchangeLower = strings.TrimSuffix(exchangeLower, "_eq")

	// Priority order: specified exchange first, then the other one.
	exchanges := []string{"nse", "bse"}
	if exchangeLower == "bse" {
		exchanges = []string{"bse", "nse"}
	}

	var lastErr error
	for _, exch := range exchanges {
		var token int64
		if exch == "nse" {
			token = stockData.NSECode
		} else {
			token = stockData.BSECode
		}
		if token <= 0 {
			continue
		}

		key := fmt.Sprintf("market:%s:%d", exch, token)
		jsonData, err := redisCache.Get(ctx, key)
		if err != nil {
			if err == models.ErrCacheMiss {
				lastErr = err
				continue
			}
			lastErr = fmt.Errorf("redis get error for key %s: %w", key, err)
			continue
		}

		var raw struct {
			LTP           float64 `json:"ltp"`
			PrevClose     float64 `json:"prev_close"`
			TickSize      float64 `json:"tick_size"`
			PercentChange float64 `json:"percent_change"`
			Volume        int64   `json:"volume"`
			DPRLower      float64 `json:"dpr_lower"`
			DPRUpper      float64 `json:"dpr_upper"`
		}
		if err := json.Unmarshal([]byte(jsonData), &raw); err != nil {
			lastErr = fmt.Errorf("failed to parse market data JSON for key %s: %w", key, err)
			continue
		}
		if raw.LTP <= 0 {
			lastErr = fmt.Errorf("invalid LTP value %.2f for token %d on %s", raw.LTP, token, exch)
			continue
		}
		if raw.TickSize <= 0 {
			lastErr = fmt.Errorf("tick_size missing or invalid (%.4f) for token %d on %s", raw.TickSize, token, exch)
			continue
		}

		logger.Debug("Market data fetched from Redis (single GET)",
			zap.String("key", key),
			zap.String("exchange", exch),
			zap.Int64("token", token),
			zap.Float64("ltp", raw.LTP),
			zap.Float64("prev_close", raw.PrevClose),
			zap.Float64("tick_size", raw.TickSize))

		return &MarketDataResult{
			LTP:           raw.LTP,
			PrevClose:     raw.PrevClose,
			TickSize:      raw.TickSize,
			PercentChange: raw.PercentChange,
			Volume:        raw.Volume,
			DPRLower:      raw.DPRLower,
			DPRUpper:      raw.DPRUpper,
			Exchange:      exch,
			Token:         token,
		}, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("market data not found in Redis for %s (NSE:%d, BSE:%d): %w",
			stockData.Symbol, stockData.NSECode, stockData.BSECode, lastErr)
	}
	return nil, fmt.Errorf("market data not found in Redis for %s (NSE:%d, BSE:%d)",
		stockData.Symbol, stockData.NSECode, stockData.BSECode)
}

// roundToTickSize rounds a price to the nearest multiple of the given tick size.
// For example, roundToTickSize(281.73, 0.05) = 281.75.
//
// The function uses integer arithmetic in "paise" units to avoid floating-point
// precision issues. It works correctly for any tick size expressible as a
// fraction of 1 rupee (0.01, 0.05, 0.10, 0.25, etc.).
func roundToTickSize(price float64, tickSize float64) float64 {
	if tickSize <= 0 {
		return price // safety: should never happen since we validate tick_size
	}
	// Convert both to integer "paise" (hundredths) to avoid float precision issues.
	// Use 1e8 for precision with very small tick sizes.
	const scale = 1e8
	priceSc := int64(math.Round(price * scale))
	tickSc := int64(math.Round(tickSize * scale))
	if tickSc <= 0 {
		return price
	}
	// Round to nearest tick: (price + tick/2) / tick * tick
	rounded := ((priceSc + tickSc/2) / tickSc) * tickSc
	return float64(rounded) / scale
}

// GetMarketDataFromRedis is the exported entry point for the backfill package.
func GetMarketDataFromRedis(ctx context.Context, redisCache *cache.RedisCache, stockData models.StockData, logger *zap.Logger) (*MarketDataResult, error) {
	return getMarketDataFromRedis(ctx, redisCache, stockData, logger)
}

// GetMarketDataBatchNSE fetches market data for many NSE tokens in a single
// Redis MGET round-trip (key form market:nse:<token>). Returns a map keyed by
// token; tokens with no data, invalid LTP, or invalid tick size are omitted.
// This replaces N serial GetMarketDataFromRedis calls on the AMN preview path.
func GetMarketDataBatchNSE(ctx context.Context, redisCache *cache.RedisCache, tokens []int64, logger *zap.Logger) map[int64]*MarketDataResult {
	out := make(map[int64]*MarketDataResult, len(tokens))
	if redisCache == nil || len(tokens) == 0 {
		return out
	}

	// Deduplicate tokens and build aligned key list.
	uniq := make([]int64, 0, len(tokens))
	seen := make(map[int64]struct{}, len(tokens))
	for _, t := range tokens {
		if t <= 0 {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		uniq = append(uniq, t)
	}
	if len(uniq) == 0 {
		return out
	}

	keys := make([]string, len(uniq))
	for i, t := range uniq {
		keys[i] = fmt.Sprintf("market:nse:%d", t)
	}

	vals, err := redisCache.MGet(ctx, keys...)
	if err != nil {
		if logger != nil {
			logger.Warn("market data batch MGET failed", zap.Error(err))
		}
		return out
	}

	for i, v := range vals {
		s, ok := v.(string)
		if !ok || s == "" {
			continue
		}
		var raw struct {
			LTP           float64 `json:"ltp"`
			PrevClose     float64 `json:"prev_close"`
			TickSize      float64 `json:"tick_size"`
			PercentChange float64 `json:"percent_change"`
		}
		if err := json.Unmarshal([]byte(s), &raw); err != nil {
			continue
		}
		if raw.LTP <= 0 || raw.TickSize <= 0 {
			continue
		}
		out[uniq[i]] = &MarketDataResult{
			LTP:           raw.LTP,
			PrevClose:     raw.PrevClose,
			TickSize:      raw.TickSize,
			PercentChange: raw.PercentChange,
			Exchange:      "nse",
			Token:         uniq[i],
		}
	}
	return out
}

// RoundToTickSize is the exported entry point for the backfill package.
func RoundToTickSize(price, tickSize float64) float64 {
	return roundToTickSize(price, tickSize)
}
