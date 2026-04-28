// historical-worker: Market data service for 52W high/low tracking.
//
// Modes:
//   --mode=daily      6AM API sync + WebSocket monitor (default, for PM2)
//   --mode=apisync    One-time NSE + BSE API sync, then exit
//   --mode=monitor    WebSocket breakout detection only
//
// Usage with PM2:
//   pm2 start ./bin/historical-worker --name "market-data" -- --mode=daily

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
	mode := flag.String("mode", "daily", "Mode: daily, apisync, or monitor")
	schedHour := flag.Int("hour", 6, "Daily API sync hour IST (default 6)")
	schedMin := flag.Int("min", 0, "Daily API sync minute IST (default 0)")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting historical-worker", zap.String("mode", *mode))

	cfg := config.Load()

	// PostgreSQL
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
	logger.Info("Connected to PostgreSQL", zap.String("database", cfg.PGDatabase))

	// Redis
	var redisClient *redis.Client
	if cfg.RedisURI != "" {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisURI,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})
		if err := redisClient.Ping(context.Background()).Err(); err != nil {
			logger.Warn("Redis not available", zap.Error(err))
			redisClient = nil
		} else {
			logger.Info("Connected to Redis", zap.String("addr", cfg.RedisURI))
		}
	}

	// Worker
	repo := historical.NewRepository(db, logger)
	worker, err := historical.NewWorker(repo, redisClient, logger)
	if err != nil {
		logger.Fatal("Failed to create worker", zap.Error(err))
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		logger.Info("Shutdown signal received")
		cancel()
	}()

	// On startup: ensure Redis has 52W data
	if err := worker.EnsureRedisLoaded(ctx); err != nil {
		logger.Error("Redis recovery failed", zap.Error(err))
	}

	// On startup: ensure EMAs are seeded (one-time Yahoo fetch if needed)
	if err := worker.EnsureEMAsSeeded(ctx); err != nil {
		logger.Error("EMA seed failed", zap.Error(err))
	}

	switch *mode {
	case "daily":
		// Start WebSocket monitor in background
		monitor := historical.NewWSMonitor(repo, redisClient, cfg.KafkaBrokers, logger)
		go func() {
			if err := monitor.Start(ctx); err != nil {
				logger.Error("WebSocket monitor failed", zap.Error(err))
			}
		}()
		logger.Info("WebSocket monitor started in background")

		// Run daily API sync scheduler (blocks until shutdown)
		worker.RunDailyScheduler(ctx, *schedHour, *schedMin)

	case "apisync":
		logger.Info("Running one-time API sync (NSE + BSE)")
		if err := worker.RunAPISync(ctx); err != nil {
			logger.Fatal("API sync failed", zap.Error(err))
		}
		logger.Info("API sync complete")

	case "emaseed":
		// One-time: fetch 6 months from Yahoo Finance → compute EMAs → store in Redis
		logger.Info("Seeding EMAs from Yahoo Finance (one-time)")
		if err := worker.SeedEMAs(ctx); err != nil {
			logger.Fatal("EMA seed failed", zap.Error(err))
		}
		logger.Info("EMA seed complete")

	case "emaupdate":
		// Manual: update EMAs from latest bhavcopy
		logger.Info("Updating EMAs from bhavcopy")
		if err := worker.UpdateEMAs(ctx); err != nil {
			logger.Fatal("EMA update failed", zap.Error(err))
		}
		logger.Info("EMA update complete")

	case "monitor":
		monitor := historical.NewWSMonitor(repo, redisClient, cfg.KafkaBrokers, logger)
		logger.Info("Starting WebSocket monitor (standalone mode)")
		if err := monitor.Start(ctx); err != nil {
			logger.Fatal("WebSocket monitor failed", zap.Error(err))
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s (use 'daily', 'apisync', or 'monitor')\n", *mode)
		os.Exit(1)
	}
}
