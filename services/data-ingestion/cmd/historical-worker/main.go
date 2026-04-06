// historical-worker: Standalone service for historical market data ingestion.
//
// Modes:
//   --mode=daily      Run daily scheduler (default, for PM2)
//   --mode=backfill   Backfill historical data for a date range
//
// Daily mode (PM2):
//   Runs 24/7, ingests today's bhavcopy at 18:30 IST, refreshes 52W matview,
//   syncs to Redis. Also does a catch-up on startup for any missed dates.
//
// Backfill mode:
//   --from 2006-01-01 --to 2026-04-01
//   Downloads and ingests historical data, then exits.
//
// Usage with PM2:
//   pm2 start ./bin/historical-worker --name "historical-worker" -- --mode=daily
//   pm2 start ./bin/historical-worker --name "backfill" -- --mode=backfill --from=2020-01-01 --to=2026-04-01

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/historical"
)

func main() {
	mode := flag.String("mode", "daily", "Mode: daily (scheduler), backfill, or monitor")
	fromStr := flag.String("from", "", "Backfill start date (YYYY-MM-DD)")
	toStr := flag.String("to", "", "Backfill end date (YYYY-MM-DD)")
	schedHour := flag.Int("hour", 18, "Daily schedule hour IST (default 18)")
	schedMin := flag.Int("min", 30, "Daily schedule minute IST (default 30)")
	catchupDays := flag.Int("catchup", 7, "Days to catch up on startup in daily mode")
	flag.Parse()

	// Logger
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting historical-worker",
		zap.String("mode", *mode))

	// Load config
	cfg := config.Load()

	// Connect to PostgreSQL (market_data)
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDatabase, cfg.PGSSLMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Fatal("Failed to open PostgreSQL", zap.Error(err))
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		logger.Fatal("Failed to ping PostgreSQL", zap.Error(err))
	}
	logger.Info("Connected to PostgreSQL",
		zap.String("database", cfg.PGDatabase),
		zap.String("host", cfg.PGHost))

	// Connect to Redis
	var redisClient *redis.Client
	if cfg.RedisURI != "" {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisURI,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})
		if err := redisClient.Ping(context.Background()).Err(); err != nil {
			logger.Warn("Redis not available, 52W sync will be skipped", zap.Error(err))
			redisClient = nil
		} else {
			logger.Info("Connected to Redis", zap.String("addr", cfg.RedisURI))
		}
	}

	// Create worker
	repo := historical.NewRepository(db, logger)
	worker, err := historical.NewWorker(repo, redisClient, logger)
	if err != nil {
		logger.Fatal("Failed to create worker", zap.Error(err))
	}

	// Context with graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		logger.Info("Shutdown signal received")
		cancel()
	}()

	// Fix #7: On startup, ensure Redis has 52W data (recover from Redis crash)
	if err := worker.EnsureRedisLoaded(ctx); err != nil {
		logger.Error("Redis recovery failed", zap.Error(err))
	}

	switch *mode {
	case "backfill":
		if *fromStr == "" || *toStr == "" {
			fmt.Fprintln(os.Stderr, "backfill mode requires --from and --to flags")
			os.Exit(1)
		}
		from, err := time.Parse("2006-01-02", *fromStr)
		if err != nil {
			logger.Fatal("Invalid --from date", zap.Error(err))
		}
		to, err := time.Parse("2006-01-02", *toStr)
		if err != nil {
			logger.Fatal("Invalid --to date", zap.Error(err))
		}

		if err := worker.Backfill(ctx, from, to); err != nil {
			logger.Fatal("Backfill failed", zap.Error(err))
		}

		// Refresh matview after backfill
		logger.Info("Backfill done, refreshing materialized view")
		if err := worker.RefreshAndSync(ctx); err != nil {
			logger.Error("Post-backfill refresh failed", zap.Error(err))
		}
		logger.Info("Backfill complete")

	case "daily":
		// Catch-up: ingest any missed dates from the last N days
		if *catchupDays > 0 {
			loc, _ := time.LoadLocation("Asia/Kolkata")
			now := time.Now().In(loc)
			from := now.AddDate(0, 0, -*catchupDays)
			to := now.AddDate(0, 0, -1) // Yesterday (today's data may not be ready yet)

			logger.Info("Running catch-up for missed dates",
				zap.String("from", from.Format("2006-01-02")),
				zap.String("to", to.Format("2006-01-02")))

			if err := worker.Backfill(ctx, from, to); err != nil {
				logger.Error("Catch-up failed", zap.Error(err))
			}

			// Refresh after catch-up
			if err := worker.RefreshAndSync(ctx); err != nil {
				logger.Error("Post-catchup refresh failed", zap.Error(err))
			}
		}

		// Start WebSocket monitor in background (real-time breakout detection)
		monitor := historical.NewWSMonitor(repo, redisClient, logger)
		go func() {
			if err := monitor.Start(ctx); err != nil {
				logger.Error("WebSocket monitor failed", zap.Error(err))
			}
		}()
		logger.Info("WebSocket monitor started in background")

		// Run daily scheduler (blocks until ctx cancelled)
		worker.RunDailyScheduler(ctx, *schedHour, *schedMin)

	case "monitor":
		// Monitor-only mode: just run WebSocket breakout detection
		monitor := historical.NewWSMonitor(repo, redisClient, logger)
		logger.Info("Starting WebSocket monitor (standalone mode)")
		if err := monitor.Start(ctx); err != nil {
			logger.Fatal("WebSocket monitor failed", zap.Error(err))
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s (use 'daily', 'backfill', or 'monitor')\n", *mode)
		os.Exit(1)
	}
}
