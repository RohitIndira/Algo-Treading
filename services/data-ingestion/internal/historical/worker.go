package historical

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Worker handles both historical backfill and daily scheduled ingestion.
type Worker struct {
	downloader *Downloader
	parser     *Parser
	repo       *Repository
	redis      *redis.Client
	logger     *zap.Logger
}

// NewWorker creates a new historical data worker.
func NewWorker(repo *Repository, redisClient *redis.Client, logger *zap.Logger) (*Worker, error) {
	dl, err := NewDownloader(logger)
	if err != nil {
		return nil, fmt.Errorf("create downloader: %w", err)
	}
	return &Worker{
		downloader: dl,
		parser:     NewParser(logger),
		repo:       repo,
		redis:      redisClient,
		logger:     logger,
	}, nil
}

// Backfill downloads and ingests historical data for a date range.
// Skips weekends and already-loaded dates. Safe to re-run (idempotent).
func (w *Worker) Backfill(ctx context.Context, from, to time.Time) error {
	w.logger.Info("Starting backfill",
		zap.String("from", from.Format("2006-01-02")),
		zap.String("to", to.Format("2006-01-02")))

	totalInserted := 0
	totalSkipped := 0
	daysProcessed := 0
	daysSkipped := 0

	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		if ctx.Err() != nil {
			w.logger.Info("Backfill cancelled", zap.Int("days_processed", daysProcessed))
			return ctx.Err()
		}

		// Skip weekends
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}

		ins, skip, err := w.IngestDate(ctx, d)
		if err != nil {
			w.logger.Warn("Backfill date failed, continuing",
				zap.String("date", d.Format("2006-01-02")),
				zap.Error(err))
			continue
		}

		if ins == 0 && skip == 0 {
			daysSkipped++
		} else {
			daysProcessed++
			totalInserted += ins
			totalSkipped += skip
		}

		// Progress log every 30 days
		if daysProcessed > 0 && daysProcessed%30 == 0 {
			w.logger.Info("Backfill progress",
				zap.String("current_date", d.Format("2006-01-02")),
				zap.Int("days_processed", daysProcessed),
				zap.Int("total_inserted", totalInserted))
		}
	}

	w.logger.Info("Backfill complete",
		zap.Int("days_processed", daysProcessed),
		zap.Int("days_skipped", daysSkipped),
		zap.Int("total_inserted", totalInserted),
		zap.Int("total_skipped", totalSkipped))

	return nil
}

// IngestDate downloads and stores bhavcopy data for a single date (both NSE and BSE).
// Returns total (inserted, skipped) across both exchanges.
// Skips if already loaded (idempotent).
func (w *Worker) IngestDate(ctx context.Context, date time.Time) (int, int, error) {
	totalInserted := 0
	totalSkipped := 0

	// NSE
	ins, skip, err := w.ingestExchange(ctx, date, "NSE")
	if err != nil {
		w.logger.Warn("NSE ingest failed",
			zap.String("date", date.Format("2006-01-02")),
			zap.Error(err))
	} else {
		totalInserted += ins
		totalSkipped += skip
	}

	// BSE
	ins, skip, err = w.ingestExchange(ctx, date, "BSE")
	if err != nil {
		w.logger.Warn("BSE ingest failed",
			zap.String("date", date.Format("2006-01-02")),
			zap.Error(err))
	} else {
		totalInserted += ins
		totalSkipped += skip
	}

	return totalInserted, totalSkipped, nil
}

func (w *Worker) ingestExchange(ctx context.Context, date time.Time, exchange string) (int, int, error) {
	// Check if already loaded
	loaded, err := w.repo.IsDateLoaded(ctx, date, exchange)
	if err != nil {
		return 0, 0, fmt.Errorf("check loaded: %w", err)
	}
	if loaded {
		return 0, 0, nil // Already done
	}

	// Download
	var bhavData *BhavData
	switch exchange {
	case "NSE":
		bhavData, err = w.downloader.DownloadNSE(date)
	case "BSE":
		bhavData, err = w.downloader.DownloadBSE(date)
	}
	if err != nil {
		if IsNoDataError(err) {
			return 0, 0, nil // Holiday — not an error
		}
		return 0, 0, fmt.Errorf("download %s: %w", exchange, err)
	}

	// Start load log
	loadID, err := w.repo.StartLoadLog(ctx, bhavData.Source, exchange, date, bhavData.FileName, int64(len(bhavData.CSV)))
	if err != nil {
		w.logger.Warn("Failed to create load log entry", zap.Error(err))
		// Continue anyway — load log is for auditing, not critical
	}

	start := time.Now()

	// Parse
	records, err := w.parser.Parse(bhavData)
	if err != nil {
		if loadID > 0 {
			w.repo.FailLoadLog(ctx, loadID, err.Error())
		}
		return 0, 0, fmt.Errorf("parse %s: %w", exchange, err)
	}

	// Bulk upsert
	inserted, skipped, err := w.repo.BulkUpsertOHLCV(ctx, records, bhavData.Source)
	if err != nil {
		if loadID > 0 {
			w.repo.FailLoadLog(ctx, loadID, err.Error())
		}
		return 0, 0, fmt.Errorf("upsert %s: %w", exchange, err)
	}

	durationMs := int(time.Since(start).Milliseconds())

	// Complete load log
	if loadID > 0 {
		w.repo.CompleteLoadLog(ctx, loadID, inserted, skipped, durationMs)
	}

	w.logger.Info("Ingested bhavcopy",
		zap.String("exchange", exchange),
		zap.String("date", date.Format("2006-01-02")),
		zap.String("source", bhavData.Source),
		zap.Int("inserted", inserted),
		zap.Int("skipped", skipped),
		zap.Int("duration_ms", durationMs))

	return inserted, skipped, nil
}

// --- Fix #5: Stock Split Detection ---

// DetectAndHandleSplits checks for stock splits/bonus/corporate actions
// by looking for >30% overnight gaps. Resets affected instruments' history.
func (w *Worker) DetectAndHandleSplits(ctx context.Context, date time.Time) {
	splits, err := w.repo.DetectSplits(ctx, date)
	if err != nil {
		w.logger.Error("Split detection query failed", zap.Error(err))
		return
	}

	if len(splits) == 0 {
		return
	}

	w.logger.Warn("Detected potential stock splits/corporate actions",
		zap.Int("count", len(splits)))

	for _, s := range splits {
		w.logger.Warn("Stock split detected — resetting history",
			zap.String("symbol", s.Symbol),
			zap.String("exchange", s.Exchange),
			zap.Float64("prev_close", s.PrevClose),
			zap.Float64("today_open", s.TodayOpen),
			zap.Float64("gap_pct", math.Round(s.GapPct*100)/100))

		// Delete all history BEFORE today — keep only today's data
		if err := w.repo.ResetInstrumentHistory(ctx, s.InstrumentID, date); err != nil {
			w.logger.Error("Failed to reset instrument history",
				zap.String("symbol", s.Symbol),
				zap.Error(err))
		}
	}
}

// --- Fix #3: Cleanup old data ---

// CleanupOldData removes OHLCV data older than 370 calendar days.
func (w *Worker) CleanupOldData(ctx context.Context) {
	deleted, err := w.repo.CleanupOldData(ctx)
	if err != nil {
		w.logger.Error("Cleanup failed", zap.Error(err))
		return
	}
	if deleted > 0 {
		w.logger.Info("Cleaned up old data",
			zap.Int64("rows_deleted", deleted))
	}
}

// --- Fix #7: Redis Recovery ---

// EnsureRedisLoaded checks if Redis has 52W data. If empty, reloads from Postgres.
// Called on startup to handle Redis crash recovery.
func (w *Worker) EnsureRedisLoaded(ctx context.Context) error {
	if w.redis == nil {
		return nil
	}

	// Check if Redis has any 52W keys
	keys, err := w.redis.Keys(ctx, "52w:*").Result()
	if err != nil {
		return fmt.Errorf("redis keys check: %w", err)
	}

	if len(keys) > 0 {
		w.logger.Info("Redis has 52W data", zap.Int("keys", len(keys)))
		return nil
	}

	// Redis is empty — check if Postgres has data to load
	hasData, err := w.repo.Has52WData(ctx)
	if err != nil {
		return fmt.Errorf("check matview: %w", err)
	}

	if !hasData {
		w.logger.Warn("No 52W data in Postgres either — will be available after first ingestion")
		return nil
	}

	w.logger.Warn("Redis empty but Postgres has 52W data — reloading")
	return w.syncToRedis(ctx)
}

// --- Materialized View + Redis Sync ---

// RefreshAndSync refreshes the 52W materialized view and syncs to Redis.
// Called after daily ingestion completes.
func (w *Worker) RefreshAndSync(ctx context.Context) error {
	w.logger.Info("Refreshing 52W materialized view")
	start := time.Now()

	if err := w.repo.RefreshMaterializedView(ctx); err != nil {
		return fmt.Errorf("refresh matview: %w", err)
	}
	w.logger.Info("Materialized view refreshed",
		zap.Duration("duration", time.Since(start)))

	// Sync to Redis
	return w.syncToRedis(ctx)
}

// syncToRedis reads all 52W data from the materialized view and writes to Redis.
// Fix #7: Redis TTL = 120 hours (5 days) to survive long holidays.
// Fix #4: Includes data_days count so rules-engine knows if data is partial (IPO).
func (w *Worker) syncToRedis(ctx context.Context) error {
	if w.redis == nil {
		w.logger.Warn("Redis client not configured, skipping 52W sync")
		return nil
	}

	data, err := w.repo.GetAll52WData(ctx)
	if err != nil {
		return fmt.Errorf("get 52W data: %w", err)
	}

	if len(data) == 0 {
		w.logger.Warn("No 52W data to sync")
		return nil
	}

	pipe := w.redis.Pipeline()
	ttl := 120 * time.Hour // Fix: 5 days TTL to survive long holidays (was 25h)

	for _, d := range data {
		key := fmt.Sprintf("52w:%d", d.InstrumentID)
		val, _ := json.Marshal(map[string]interface{}{
			"high":       d.Week52High,
			"low":        d.Week52Low,
			"last_close": d.LastClose,
			"symbol":     d.Symbol,
			"token":      d.Token,
			"exchange":   d.Exchange,
			"position":   d.PositionInRange,
			"data_days":  d.DataDays, // Fix #4: rules-engine can check if partial
			"as_of":      d.LastTradeDate.Format("2006-01-02"),
		})
		pipe.Set(ctx, key, val, ttl)

		// Also set by token for easy lookup from rules-engine
		if d.Token != "" {
			tokenKey := fmt.Sprintf("52w:token:%s", d.Token)
			pipe.Set(ctx, tokenKey, val, ttl)
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}

	w.logger.Info("52W data synced to Redis",
		zap.Int("instruments", len(data)))
	return nil
}

// --- Fix #8: Retry bhavcopy with polling ---

// RunDailyScheduler runs the daily ingestion + refresh cycle.
// Fix #8: Retries bhavcopy download every 10 min if not available (up to 22:00 IST).
func (w *Worker) RunDailyScheduler(ctx context.Context, scheduleHour, scheduleMinute int) {
	w.logger.Info("Daily scheduler started",
		zap.Int("schedule_hour", scheduleHour),
		zap.Int("schedule_minute", scheduleMinute))

	for {
		now := time.Now()
		loc, _ := time.LoadLocation("Asia/Kolkata")
		nowIST := now.In(loc)

		// Calculate next run time
		next := time.Date(nowIST.Year(), nowIST.Month(), nowIST.Day(),
			scheduleHour, scheduleMinute, 0, 0, loc)
		if nowIST.After(next) {
			next = next.AddDate(0, 0, 1) // Tomorrow
		}

		sleepDur := next.Sub(nowIST)
		w.logger.Info("Next daily run scheduled",
			zap.String("next_run", next.Format("2006-01-02 15:04 IST")),
			zap.Duration("sleep", sleepDur))

		select {
		case <-ctx.Done():
			w.logger.Info("Daily scheduler stopped")
			return
		case <-time.After(sleepDur):
		}

		// Skip weekends
		today := time.Now().In(loc)
		if today.Weekday() == time.Saturday || today.Weekday() == time.Sunday {
			w.logger.Info("Weekend — skipping daily ingestion")
			continue
		}

		w.logger.Info("Running daily ingestion",
			zap.String("date", today.Format("2006-01-02")))

		// Fix #8: Retry ingestion every 10 min until data available or 22:00 IST
		deadline := time.Date(today.Year(), today.Month(), today.Day(), 22, 0, 0, 0, loc)
		var ins, skip int
		var ingestErr error

		for attempt := 0; ; attempt++ {
			ins, skip, ingestErr = w.IngestDate(ctx, today)
			if ingestErr == nil && ins > 0 {
				break // Success
			}

			if time.Now().In(loc).After(deadline) {
				w.logger.Warn("Bhavcopy not available before deadline, giving up",
					zap.String("date", today.Format("2006-01-02")))
				break
			}

			if ctx.Err() != nil {
				return
			}

			w.logger.Info("Bhavcopy not ready yet, retrying in 10 min",
				zap.Int("attempt", attempt+1),
				zap.Error(ingestErr))

			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Minute):
			}
		}

		if ins > 0 {
			w.logger.Info("Daily ingestion complete",
				zap.Int("inserted", ins),
				zap.Int("skipped", skip))

			// Fix #5: Detect stock splits after ingestion
			w.DetectAndHandleSplits(ctx, today)

			// Fix #3: Cleanup old data (>370 calendar days)
			w.CleanupOldData(ctx)

			// Refresh materialized view + sync to Redis
			if err := w.RefreshAndSync(ctx); err != nil {
				w.logger.Error("Refresh/sync failed", zap.Error(err))
			}
		}
	}
}
