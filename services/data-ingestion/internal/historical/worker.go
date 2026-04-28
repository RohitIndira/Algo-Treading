package historical

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/ema"
)

// Worker handles daily 52W API sync, EMA computation, and Redis management.
type Worker struct {
	repo       *Repository
	redis      *redis.Client
	apiFetcher *APIFetcher
	emaSvc     *ema.Service
	logger     *zap.Logger
}

// NewWorker creates a new worker.
func NewWorker(repo *Repository, redisClient *redis.Client, logger *zap.Logger) (*Worker, error) {
	apiFetcher, err := NewAPIFetcher(repo, redisClient, logger)
	if err != nil {
		return nil, err
	}

	emaSvc := ema.NewService(redisClient, logger)

	return &Worker{
		repo:       repo,
		redis:      redisClient,
		apiFetcher: apiFetcher,
		emaSvc:     emaSvc,
		logger:     logger,
	}, nil
}

// RunAPISync runs a one-time NSE + BSE API sync. Used for --mode=apisync.
func (w *Worker) RunAPISync(ctx context.Context) error {
	return w.apiFetcher.SyncAll(ctx)
}

// SeedEMAs fetches 6 months of close prices from Yahoo Finance and computes
// initial EMA 21/50/100 for all NSE stocks. Run once on first setup.
func (w *Worker) SeedEMAs(ctx context.Context) error {
	symbols, err := w.repo.GetAllNSESymbols(ctx)
	if err != nil {
		return err
	}
	return w.emaSvc.SeedFromYahoo(ctx, symbols)
}

// UpdateEMAs downloads latest bhavcopy and updates all EMAs.
func (w *Worker) UpdateEMAs(ctx context.Context) error {
	return w.emaSvc.UpdateDailyFromBhavcopy(ctx)
}

// EnsureEMAsSeeded checks if EMAs exist in Redis. If not, seeds from Yahoo.
func (w *Worker) EnsureEMAsSeeded(ctx context.Context) error {
	if w.emaSvc.IsSeeded(ctx) {
		w.logger.Info("EMAs already seeded in Redis")
		return nil
	}
	w.logger.Warn("EMAs not found in Redis — seeding from Yahoo Finance")
	return w.SeedEMAs(ctx)
}

// EnsureRedisLoaded checks if Redis has 52W data. If empty, runs API sync.
// Called on startup to handle Redis crash recovery.
func (w *Worker) EnsureRedisLoaded(ctx context.Context) error {
	if w.redis == nil {
		return nil
	}

	keys, err := w.redis.Keys(ctx, "52w:*").Result()
	if err != nil {
		return err
	}

	if len(keys) > 0 {
		w.logger.Info("Redis has 52W data", zap.Int("keys", len(keys)))
		return nil
	}

	w.logger.Warn("Redis empty — running API sync to populate 52W data")
	return w.apiFetcher.SyncAll(ctx)
}

// RunDailyScheduler runs the daily 52W API sync at the scheduled time (default 6:00 AM IST).
// Fetches official 52W data from NSE and BSE APIs → stores in Redis.
func (w *Worker) RunDailyScheduler(ctx context.Context, scheduleHour, scheduleMinute int) {
	w.logger.Info("Daily API sync scheduler started",
		zap.Int("schedule_hour", scheduleHour),
		zap.Int("schedule_minute", scheduleMinute))

	for {
		now := time.Now()
		loc, _ := time.LoadLocation("Asia/Kolkata")
		nowIST := now.In(loc)

		next := time.Date(nowIST.Year(), nowIST.Month(), nowIST.Day(),
			scheduleHour, scheduleMinute, 0, 0, loc)
		if nowIST.After(next) {
			next = next.AddDate(0, 0, 1)
		}

		sleepDur := next.Sub(nowIST)
		w.logger.Info("Next API sync scheduled",
			zap.String("next_run", next.Format("2006-01-02 15:04 IST")),
			zap.Duration("sleep", sleepDur))

		select {
		case <-ctx.Done():
			w.logger.Info("Daily scheduler stopped")
			return
		case <-time.After(sleepDur):
		}

		today := time.Now().In(loc)
		if today.Weekday() == time.Saturday || today.Weekday() == time.Sunday {
			w.logger.Info("Weekend — skipping API sync")
			continue
		}

		w.logger.Info("Running daily sync",
			zap.String("date", today.Format("2006-01-02")))

		// Step 1: 52W data from NSE + BSE APIs → Redis
		if err := w.apiFetcher.SyncAll(ctx); err != nil {
			w.logger.Error("52W API sync failed", zap.Error(err))
		}

		// Step 2: EMA update from bhavcopy close prices
		if err := w.UpdateEMAs(ctx); err != nil {
			w.logger.Error("EMA update failed", zap.Error(err))
		}
	}
}
