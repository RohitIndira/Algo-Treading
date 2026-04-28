package manthan

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// LTPFeed polls an external Redis instance for live LTP data and feeds it
// to the TickHandler for trailing SL processing.
//
// Data path:
//   1. ISIN → isin:{ISIN} → get NSE token
//   2. Token → market:nse:{token} → get LTP
//   3. Feed LTP to TickHandler.ProcessTick()
//
// External Redis schema (already populated by Indira's websocket feed):
//   isin:INE297H01019 → {"nsecode":"13337", ...}
//   market:nse:13337   → {"ltp":916.15, "symbol":"GALLANTT", ...}
type LTPFeed struct {
	extRedis     *redis.Client
	tickHandler  *TickHandler
	portfolioMgr *PortfolioManager
	pollInterval time.Duration
	logger       *zap.Logger

	// ISIN → NSE token cache (doesn't change during market hours)
	tokenCache map[string]string
}

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
	activeSymbols := f.portfolioMgr.ActiveSymbols()
	if len(activeSymbols) == 0 {
		return
	}

	// Collect ISINs from active positions
	type posInfo struct {
		symbol string
		isin   string
	}
	var positions []posInfo
	for _, portfolio := range f.portfolioMgr.AllPortfolios() {
		for sym, pos := range portfolio.Positions {
			if pos.Active && pos.ISIN != "" {
				positions = append(positions, posInfo{symbol: sym, isin: pos.ISIN})
			}
		}
	}

	for _, p := range positions {
		ltp, ok := f.fetchLTP(ctx, p.isin, p.symbol)
		if !ok {
			continue
		}
		f.tickHandler.ProcessTick(ctx, p.symbol, ltp)
	}
}

// fetchLTP resolves ISIN → token → market data → LTP
func (f *LTPFeed) fetchLTP(ctx context.Context, isin, symbol string) (float64, bool) {
	token, ok := f.resolveToken(ctx, isin)
	if !ok {
		return 0, false
	}

	key := "market:nse:" + token
	raw, err := f.extRedis.Get(ctx, key).Result()
	if err != nil {
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
	// Check cache first
	if token, ok := f.tokenCache[isin]; ok {
		return token, true
	}

	raw, err := f.extRedis.Get(ctx, "isin:"+isin).Result()
	if err != nil {
		return "", false
	}

	var data struct {
		NSECode string `json:"nsecode"`
	}
	if json.Unmarshal([]byte(raw), &data) != nil || data.NSECode == "" {
		return "", false
	}

	f.tokenCache[isin] = data.NSECode
	return data.NSECode, true
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
