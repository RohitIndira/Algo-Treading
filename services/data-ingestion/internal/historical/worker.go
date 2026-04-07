package historical

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Worker handles daily 52W API sync and Redis management.
// Simple flow: 6AM API sync (NSE+BSE) → Redis, 9:15 WebSocket overrides.
type Worker struct {
	repo       *Repository
	redis      *redis.Client
	apiFetcher *APIFetcher
	logger     *zap.Logger
}

// NewWorker creates a new worker.
func NewWorker(repo *Repository, redisClient *redis.Client, logger *zap.Logger) (*Worker, error) {
	apiFetcher, err := NewAPIFetcher(repo, redisClient, logger)
	if err != nil {
		return nil, err
	}

	return &Worker{
		repo:       repo,
		redis:      redisClient,
		apiFetcher: apiFetcher,
		logger:     logger,
	}, nil
}

// RunAPISync runs a one-time NSE + BSE API sync. Used for --mode=apisync.
func (w *Worker) RunAPISync(ctx context.Context) error {
	return w.apiFetcher.SyncAll(ctx)
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

		w.logger.Info("Running daily 52W API sync",
			zap.String("date", today.Format("2006-01-02")))

		if err := w.apiFetcher.SyncAll(ctx); err != nil {
			w.logger.Error("API sync failed", zap.Error(err))
		}
	}
}
