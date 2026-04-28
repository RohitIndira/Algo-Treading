package ema

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Service manages EMA computation, storage, and lookups.
// EMAs are stored in Redis for O(1) access by the breakout detection system.
type Service struct {
	redis    *redis.Client
	yahoo    *YahooFetcher
	bhavcopy *BhavcopyFetcher
	logger   *zap.Logger
}

// NewService creates a new EMA service.
func NewService(redisClient *redis.Client, logger *zap.Logger) *Service {
	return &Service{
		redis:    redisClient,
		yahoo:    NewYahooFetcher(logger),
		bhavcopy: NewBhavcopyFetcher(logger),
		logger:   logger,
	}
}

// RedisEMAData is the structure stored in Redis for each stock.
type RedisEMAData struct {
	Symbol    string  `json:"symbol"`
	EMA21     float64 `json:"ema_21"`
	EMA50     float64 `json:"ema_50"`
	EMA100    float64 `json:"ema_100"`
	Score     int     `json:"score"`
	AllocPct  float64 `json:"alloc_pct"`
	LastClose float64 `json:"last_close"`
	UpdatedAt string  `json:"updated_at"`
}

// --- Initial Setup (One-time) ---

// SeedFromYahoo fetches 6 months of close prices from Yahoo Finance for all
// given symbols, computes EMA 21/50/100, and stores in Redis.
// Run once on first setup. Takes ~10-15 min for 3300 NSE stocks.
func (s *Service) SeedFromYahoo(ctx context.Context, symbols []string) error {
	s.logger.Info("Seeding EMAs from Yahoo Finance", zap.Int("stocks", len(symbols)))
	start := time.Now()

	// Fetch all close prices from Yahoo
	allCloses := s.yahoo.FetchAllNSE(symbols)

	// Compute EMAs and store in Redis
	pipe := s.redis.Pipeline()
	seeded := 0

	for sym, closes := range allCloses {
		if len(closes) < 21 {
			continue // Need at least 21 days for EMA 21
		}

		lastClose := closes[len(closes)-1]
		result := ComputeAll(closes, lastClose)
		result.Symbol = sym

		data := RedisEMAData{
			Symbol:    sym,
			EMA21:     result.EMA21,
			EMA50:     result.EMA50,
			EMA100:    result.EMA100,
			Score:     result.Score,
			AllocPct:  result.AllocPct,
			LastClose: lastClose,
			UpdatedAt: time.Now().Format("2006-01-02"),
		}

		val, _ := json.Marshal(data)
		key := fmt.Sprintf("ema:%s", sym)
		pipe.Set(ctx, key, val, 0) // No expiry — updated daily

		seeded++
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}

	s.logger.Info("EMA seeding complete",
		zap.Int("seeded", seeded),
		zap.Int("skipped", len(symbols)-seeded),
		zap.Duration("duration", time.Since(start)))

	return nil
}

// --- Daily Update ---

// UpdateDailyFromBhavcopy downloads the latest bhavcopy and updates all EMAs.
// Called daily at 6 AM. Downloads one CSV (~350KB), extracts close prices,
// and updates EMAs for all stocks in ~5 seconds.
func (s *Service) UpdateDailyFromBhavcopy(ctx context.Context) error {
	closePrices, err := s.bhavcopy.FetchLatestCloses()
	if err != nil {
		return fmt.Errorf("fetch bhavcopy closes: %w", err)
	}
	return s.UpdateDaily(ctx, closePrices)
}

// UpdateDaily updates all EMAs with given closing prices.
// closePrices is map[symbol]todayClose.
func (s *Service) UpdateDaily(ctx context.Context, closePrices map[string]float64) error {
	s.logger.Info("Updating daily EMAs", zap.Int("stocks", len(closePrices)))

	pipe := s.redis.Pipeline()
	updated := 0

	for sym, todayClose := range closePrices {
		if todayClose <= 0 {
			continue
		}

		// Get current EMAs from Redis
		key := fmt.Sprintf("ema:%s", sym)
		val, err := s.redis.Get(ctx, key).Result()
		if err != nil {
			continue // Stock not seeded yet — skip
		}

		var current RedisEMAData
		if json.Unmarshal([]byte(val), &current) != nil {
			continue
		}

		// Update each EMA with today's close (O(1) per stock)
		newEMA21 := UpdateEMA(todayClose, current.EMA21, Period21)
		newEMA50 := UpdateEMA(todayClose, current.EMA50, Period50)
		newEMA100 := UpdateEMA(todayClose, current.EMA100, Period100)

		// Recompute score with new EMAs
		score := ComputeScore(todayClose, newEMA21, newEMA50, newEMA100)
		alloc := ScoreToAllocation(score)

		data := RedisEMAData{
			Symbol:    sym,
			EMA21:     newEMA21,
			EMA50:     newEMA50,
			EMA100:    newEMA100,
			Score:     score,
			AllocPct:  alloc,
			LastClose: todayClose,
			UpdatedAt: time.Now().Format("2006-01-02"),
		}

		newVal, _ := json.Marshal(data)
		pipe.Set(ctx, key, newVal, 0)
		updated++
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}

	s.logger.Info("Daily EMA update complete", zap.Int("updated", updated))
	return nil
}

// --- Lookup (O(1)) ---

// GetScore returns the EMA score and allocation for a stock.
// Called by breakout detection to decide position sizing.
// Returns score=0, alloc=0.50 if stock not found (safe default).
func (s *Service) GetScore(ctx context.Context, symbol string) (int, float64) {
	key := fmt.Sprintf("ema:%s", symbol)
	val, err := s.redis.Get(ctx, key).Result()
	if err != nil {
		return 0, 0.50 // Default: neutral score, 50% allocation
	}

	var data RedisEMAData
	if json.Unmarshal([]byte(val), &data) != nil {
		return 0, 0.50
	}

	return data.Score, data.AllocPct
}

// GetFullEMA returns the complete EMA data for a stock.
func (s *Service) GetFullEMA(ctx context.Context, symbol string) (*RedisEMAData, error) {
	key := fmt.Sprintf("ema:%s", symbol)
	val, err := s.redis.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var data RedisEMAData
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// IsSeeded checks if EMAs have been seeded (any ema:* keys exist in Redis).
func (s *Service) IsSeeded(ctx context.Context) bool {
	keys, err := s.redis.Keys(ctx, "ema:*").Result()
	return err == nil && len(keys) > 0
}
