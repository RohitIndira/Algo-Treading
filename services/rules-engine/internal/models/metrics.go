package models

import (
	"sync"
	"sync/atomic"
	"time"
)

// MatchingStats holds statistics for rule matching
type MatchingStats struct {
	mu sync.RWMutex

	// Overall statistics
	TotalEventsProcessed int64
	TotalMatchesFound    int64
	TotalOrdersGenerated int64

	// Performance metrics
	TotalEvaluationTimeMs int64
	EvaluationCount       int64

	// Cache statistics
	CacheHits   int64
	CacheMisses int64

	// Error statistics
	EvaluationErrors int64
	KafkaErrors      int64
	// RabbitMQErrors removed 2026-06-25 — RabbitMQ publisher gone.

	// Start time
	StartTime time.Time

	// Top matched strategies
	TopStrategies map[string]*StrategyStats

	// Top matched stocks
	TopStocks map[int64]*StockStats
}

// StrategyStats holds statistics for a strategy
type StrategyStats struct {
	StrategyID   string
	StrategyName string
	MatchCount   int64
}

// StockStats holds statistics for a stock
type StockStats struct {
	StockCode  int64
	Symbol     string
	MatchCount int64
}

// NewMatchingStats creates a new MatchingStats instance
func NewMatchingStats() *MatchingStats {
	return &MatchingStats{
		StartTime:     time.Now(),
		TopStrategies: make(map[string]*StrategyStats),
		TopStocks:     make(map[int64]*StockStats),
	}
}

// IncrementEventsProcessed atomically increments events processed
func (s *MatchingStats) IncrementEventsProcessed() {
	atomic.AddInt64(&s.TotalEventsProcessed, 1)
}

// IncrementMatchesFound atomically increments matches found
func (s *MatchingStats) IncrementMatchesFound() {
	atomic.AddInt64(&s.TotalMatchesFound, 1)
}

// IncrementOrdersGenerated atomically increments orders generated
func (s *MatchingStats) IncrementOrdersGenerated() {
	atomic.AddInt64(&s.TotalOrdersGenerated, 1)
}

// AddEvaluationTime atomically adds evaluation time
func (s *MatchingStats) AddEvaluationTime(durationMs int64) {
	atomic.AddInt64(&s.TotalEvaluationTimeMs, durationMs)
	atomic.AddInt64(&s.EvaluationCount, 1)
}

// IncrementCacheHits atomically increments cache hits
func (s *MatchingStats) IncrementCacheHits() {
	atomic.AddInt64(&s.CacheHits, 1)
}

// IncrementCacheMisses atomically increments cache misses
func (s *MatchingStats) IncrementCacheMisses() {
	atomic.AddInt64(&s.CacheMisses, 1)
}

// IncrementEvaluationErrors atomically increments evaluation errors
func (s *MatchingStats) IncrementEvaluationErrors() {
	atomic.AddInt64(&s.EvaluationErrors, 1)
}

// IncrementKafkaErrors atomically increments kafka errors
func (s *MatchingStats) IncrementKafkaErrors() {
	atomic.AddInt64(&s.KafkaErrors, 1)
}

// IncrementRabbitMQErrors removed 2026-06-25 — RabbitMQ publisher gone.

// RecordStrategyMatch records a strategy match
func (s *MatchingStats) RecordStrategyMatch(strategyID, strategyName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if stats, exists := s.TopStrategies[strategyID]; exists {
		atomic.AddInt64(&stats.MatchCount, 1)
	} else {
		s.TopStrategies[strategyID] = &StrategyStats{
			StrategyID:   strategyID,
			StrategyName: strategyName,
			MatchCount:   1,
		}
	}
}

// RecordStockMatch records a stock match
func (s *MatchingStats) RecordStockMatch(stockCode int64, symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if stats, exists := s.TopStocks[stockCode]; exists {
		atomic.AddInt64(&stats.MatchCount, 1)
	} else {
		s.TopStocks[stockCode] = &StockStats{
			StockCode:  stockCode,
			Symbol:     symbol,
			MatchCount: 1,
		}
	}
}

// GetAvgEvaluationTime returns average evaluation time
func (s *MatchingStats) GetAvgEvaluationTime() float64 {
	count := atomic.LoadInt64(&s.EvaluationCount)
	if count == 0 {
		return 0
	}
	total := atomic.LoadInt64(&s.TotalEvaluationTimeMs)
	return float64(total) / float64(count)
}

// GetCacheHitRate returns cache hit rate percentage
func (s *MatchingStats) GetCacheHitRate() float64 {
	hits := atomic.LoadInt64(&s.CacheHits)
	misses := atomic.LoadInt64(&s.CacheMisses)
	total := hits + misses
	if total == 0 {
		return 0
	}
	return (float64(hits) / float64(total)) * 100
}

// GetMatchRate returns match rate percentage
func (s *MatchingStats) GetMatchRate() float64 {
	processed := atomic.LoadInt64(&s.TotalEventsProcessed)
	if processed == 0 {
		return 0
	}
	matches := atomic.LoadInt64(&s.TotalMatchesFound)
	return (float64(matches) / float64(processed)) * 100
}

// GetTopStrategies returns top N strategies by match count
func (s *MatchingStats) GetTopStrategies(n int) []*StrategyStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	strategies := make([]*StrategyStats, 0, len(s.TopStrategies))
	for _, stats := range s.TopStrategies {
		strategies = append(strategies, stats)
	}

	// Sort by match count (descending)
	for i := 0; i < len(strategies); i++ {
		for j := i + 1; j < len(strategies); j++ {
			if strategies[j].MatchCount > strategies[i].MatchCount {
				strategies[i], strategies[j] = strategies[j], strategies[i]
			}
		}
	}

	if len(strategies) > n {
		return strategies[:n]
	}
	return strategies
}

// GetTopStocks returns top N stocks by match count
func (s *MatchingStats) GetTopStocks(n int) []*StockStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stocks := make([]*StockStats, 0, len(s.TopStocks))
	for _, stats := range s.TopStocks {
		stocks = append(stocks, stats)
	}

	// Sort by match count (descending)
	for i := 0; i < len(stocks); i++ {
		for j := i + 1; j < len(stocks); j++ {
			if stocks[j].MatchCount > stocks[i].MatchCount {
				stocks[i], stocks[j] = stocks[j], stocks[i]
			}
		}
	}

	if len(stocks) > n {
		return stocks[:n]
	}
	return stocks
}

// Reset resets all statistics
func (s *MatchingStats) Reset() {
	atomic.StoreInt64(&s.TotalEventsProcessed, 0)
	atomic.StoreInt64(&s.TotalMatchesFound, 0)
	atomic.StoreInt64(&s.TotalOrdersGenerated, 0)
	atomic.StoreInt64(&s.TotalEvaluationTimeMs, 0)
	atomic.StoreInt64(&s.EvaluationCount, 0)
	atomic.StoreInt64(&s.CacheHits, 0)
	atomic.StoreInt64(&s.CacheMisses, 0)
	atomic.StoreInt64(&s.EvaluationErrors, 0)
	atomic.StoreInt64(&s.KafkaErrors, 0)

	s.mu.Lock()
	s.StartTime = time.Now()
	s.TopStrategies = make(map[string]*StrategyStats)
	s.TopStocks = make(map[int64]*StockStats)
	s.mu.Unlock()
}
