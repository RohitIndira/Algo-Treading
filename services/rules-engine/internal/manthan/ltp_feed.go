package manthan

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// LTPFeed polls an external Redis instance for live LTP data and feeds it
// to the TickHandler for trailing SL processing.
//
// Data path:
//  1. ISIN → isin:{ISIN} → get NSE token
//  2. Token → market:nse:{token} → get LTP
//  3. Feed LTP to TickHandler.ProcessTick()
//
// External Redis schema (already populated by Indira's websocket feed):
//
//	isin:INE297H01019 → {"nsecode":"13337", ...}
//	market:nse:13337   → {"ltp":916.15, "symbol":"GALLANTT", ...}
type LTPFeed struct {
	extRedis     *redis.Client
	tickHandler  *TickHandler
	portfolioMgr *PortfolioManager
	pollInterval time.Duration
	logger       *zap.Logger

	// ISIN → NSE token cache (doesn't change during market hours).
	// Accessed concurrently: poll() runs in the LTP Start goroutine,
	// FetchLiveLTP is called from Consumer's allocation goroutine. The
	// mutex guards both reads and writes — a plain map here would
	// eventually trip Go's concurrent-map panic.
	tokenMu    sync.RWMutex
	tokenCache map[string]string

	// lastRedisErrLog is the unix-nano timestamp of the last rate-limited
	// log for "external Redis Get failed". Loaded/stored atomically; only
	// one Warn per redisErrLogInterval keeps the log readable during a
	// sustained outage instead of one Warn per (poll × symbol).
	lastRedisErrLog int64
}

// redisErrLogInterval rate-limits the "external Redis Get failed" warning.
// Picked to be loud enough to notice but quiet enough that a 1-second poll
// across 50 symbols during a 5-minute Redis outage doesn't drown the log.
const redisErrLogInterval = 30 * time.Second

// LTPFeedConfig configures the external Redis connection.
type LTPFeedConfig struct {
	Addr         string
	Password     string
	DB           int
	PollInterval time.Duration // default 1s
}

func NewLTPFeed(
	cfg LTPFeedConfig,
	tickHandler *TickHandler,
	portfolioMgr *PortfolioManager,
	logger *zap.Logger,
) (*LTPFeed, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     20,
		MinIdleConns: 5,
		ReadTimeout:  2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	interval := cfg.PollInterval
	if interval <= 0 {
		interval = 1 * time.Second
	}

	logger.Info("LTP feed connected to external Redis",
		zap.String("addr", cfg.Addr),
		zap.Int("db", cfg.DB), // 0 = live; 1 = mock universe (2026-09-04 incident)
		zap.Duration("poll_interval", interval))

	return &LTPFeed{
		extRedis:     rdb,
		tickHandler:  tickHandler,
		portfolioMgr: portfolioMgr,
		pollInterval: interval,
		logger:       logger,
		tokenCache:   make(map[string]string),
	}, nil
}

// Start begins polling. Blocks until ctx is cancelled.
func (f *LTPFeed) Start(ctx context.Context) {
	f.logger.Info("LTP feed started")
	ticker := time.NewTicker(f.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			f.logger.Info("LTP feed stopped")
			return
		case <-ticker.C:
			f.poll(ctx)
		}
	}
}

func (f *LTPFeed) poll(ctx context.Context) {
	// Collect ISINs from active positions in a single pass — the previous
	// implementation called ActiveSymbols() just to test for empty and
	// then re-walked AllPortfolios, wasting the first call's result.
	type posInfo struct {
		symbol string
		isin   string
	}
	var positions []posInfo
	for _, portfolio := range f.portfolioMgr.AllPortfolios() {
		// portfolio.Positions is a plain map mutated by the fill consumer
		// goroutine. Take the per-Portfolio RLock so iteration doesn't race
		// with a concurrent AddPosition/ProcessFillEvent (would panic
		// otherwise). Released before the broker round-trip below.
		portfolio.Mu.RLock()
		for sym, pos := range portfolio.Positions {
			if pos.Active && pos.ISIN != "" {
				positions = append(positions, posInfo{symbol: sym, isin: pos.ISIN})
			}
		}
		portfolio.Mu.RUnlock()
	}
	if len(positions) == 0 {
		return
	}

	for _, p := range positions {
		ltp, ok := f.fetchLTP(ctx, p.isin, p.symbol)
		if !ok {
			continue
		}
		f.tickHandler.ProcessTick(ctx, p.symbol, ltp)
	}
}

// fetchLTP resolves ISIN → token → market data → LTP.
//
// symbol is used purely for log context — kept even when blank
// (FetchLiveLTP allocation path) so the rate-limited error message
// stays readable.
func (f *LTPFeed) fetchLTP(ctx context.Context, isin, symbol string) (float64, bool) {
	token, ok := f.resolveToken(ctx, isin)
	if !ok {
		return 0, false
	}

	key := "market:nse:" + token
	raw, err := f.extRedis.Get(ctx, key).Result()
	if err != nil {
		f.warnRedisErr("market Get failed", key, symbol, err)
		return 0, false
	}

	var data struct {
		LTP float64 `json:"ltp"`
	}
	if json.Unmarshal([]byte(raw), &data) != nil || data.LTP <= 0 {
		return 0, false
	}

	return data.LTP, true
}

func (f *LTPFeed) resolveToken(ctx context.Context, isin string) (string, bool) {
	// Cache check under a read lock — vast majority of calls take this
	// path during a normal trading day.
	f.tokenMu.RLock()
	if token, ok := f.tokenCache[isin]; ok {
		f.tokenMu.RUnlock()
		return token, true
	}
	f.tokenMu.RUnlock()

	raw, err := f.extRedis.Get(ctx, "isin:"+isin).Result()
	if err != nil {
		f.warnRedisErr("isin Get failed", "isin:"+isin, "", err)
		return "", false
	}

	var data struct {
		NSECode string `json:"nsecode"`
	}
	if json.Unmarshal([]byte(raw), &data) != nil || data.NSECode == "" {
		return "", false
	}

	f.tokenMu.Lock()
	f.tokenCache[isin] = data.NSECode
	f.tokenMu.Unlock()
	return data.NSECode, true
}

// warnRedisErr emits a Warn for an external-Redis failure, but only once
// per redisErrLogInterval. During a sustained outage the operator gets
// one line every 30 seconds instead of one per (symbol × poll cycle),
// which can be hundreds per second on a busy book.
func (f *LTPFeed) warnRedisErr(msg, key, symbol string, err error) {
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&f.lastRedisErrLog)
	if now-last < int64(redisErrLogInterval) {
		return
	}
	if !atomic.CompareAndSwapInt64(&f.lastRedisErrLog, last, now) {
		// Another goroutine just logged; let it win.
		return
	}
	f.logger.Warn("LTP feed: external Redis "+msg,
		zap.String("key", key),
		zap.String("symbol", symbol),
		zap.Error(err))
}

// FetchLiveLTP is a convenience method for the consumer to get live LTP
// for a symbol by ISIN. Used during allocation for accurate entry price.
func (f *LTPFeed) FetchLiveLTP(ctx context.Context, isin string) (float64, bool) {
	return f.fetchLTP(ctx, isin, "")
}

// Close closes the external Redis connection.
func (f *LTPFeed) Close() error {
	return f.extRedis.Close()
}
