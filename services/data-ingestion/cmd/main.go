package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	redispkg "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	kafkapkg "github.com/RohitIndira/Algo-Treading/pkg/kafka"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/detector"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/subscriber"

	"go.uber.org/zap"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║      DATA INGESTION SERVICE - Event-Driven Architecture      ║")
	fmt.Println("║         52-Week Breakout Detection for Indian Markets        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	// Load configuration
	cfg := config.Load()
	fmt.Printf("✓ Configuration loaded successfully\n")

	// Initialize logger
	lgr, err := logger.NewWithDefaults("data-ingestion")
	if err != nil {
		panic(fmt.Errorf("failed to initialize logger: %w", err))
	}
	defer lgr.Sync()

	lgr.Info("Starting data-ingestion service (event-driven mode)",
		zap.Int("worker_pool_size", cfg.WorkerPoolSize))

	// Context for the entire application
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ========================================================================
	// KAFKA PRODUCER SETUP
	// ========================================================================
	lgr.Info("Initializing Kafka producer for 52-week breakouts")

	breakoutProdCfg := kafkapkg.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic52Week,
		BatchSize:   100,
		MaxAttempts: 3,
	}
	breakoutProducer, err := kafkapkg.NewProducer(breakoutProdCfg)
	if err != nil {
		lgr.Fatal("Failed to create Kafka producer", zap.Error(err))
	}
	defer breakoutProducer.Close()

	// Ensure Kafka topic exists with 7-day retention
	if err := kafkapkg.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTopic52Week, 1, 1); err != nil {
		lgr.Fatal("Failed to ensure Kafka topic exists", zap.Error(err))
	}

	lgr.Info("Connected to Kafka",
		zap.Strings("brokers", cfg.KafkaBrokers),
		zap.String("topic", cfg.KafkaTopic52Week))

	// Create Kafka publisher
	kafkaPub := publisher.NewKafkaPublisher(breakoutProducer, cfg.KafkaTopic52Week)
	defer kafkaPub.Close()

	// ========================================================================
	// REDIS CLIENT SETUP
	// ========================================================================
	lgr.Info("Connecting to Redis market data store")

	redisClient, err := redispkg.New(redispkg.Config{
		Address:      cfg.MarketRedisAddr,
		Password:     cfg.MarketRedisPassword,
		DB:           cfg.MarketRedisDB,
		PoolSize:     100,
		MinIdleConns: 10,
	})
	if err != nil {
		lgr.Fatal("Failed to connect to Redis", zap.Error(err))
	}
	defer redisClient.Close()

	lgr.Info("Connected to Redis",
		zap.String("addr", cfg.MarketRedisAddr),
		zap.Int("db", cfg.MarketRedisDB))

	// ========================================================================
	// BREAKOUT DETECTION SETUP
	// ========================================================================
	lgr.Info("Initializing breakout detection components")

	// State manager for persistent dedupe
	stateMgr := detector.NewStateManager(redisClient, lgr)

	// Breakout detector
	breakoutDetector := detector.NewBreakoutDetector(stateMgr, lgr)

	// Run cleanup of old breakout state on startup
	if err := stateMgr.CleanupOldBreakouts(ctx); err != nil {
		lgr.Warn("Failed to cleanup old breakouts", zap.Error(err))
	}

	// ========================================================================
	// START EVENT-DRIVEN 52-WEEK BREAKOUT DETECTION
	// ========================================================================
	lgr.Info("🚀 Starting event-driven 52-week breakout detection",
		zap.Int("workers", cfg.WorkerPoolSize))

	redisSub := subscriber.NewRedisSubscriber(
		redisClient,
		breakoutDetector,
		kafkaPub,
		lgr,
		subscriber.Config{
			WorkerCount: cfg.WorkerPoolSize,
		},
	)

	go func() {
		if err := redisSub.Start(ctx); err != nil && err != context.Canceled {
			lgr.Error("Redis subscriber stopped with error", zap.Error(err))
		}
	}()

	lgr.Info("✓ Event-driven subscriber started successfully")
	lgr.Info("📊 Listening for Redis keyspace notifications on pattern: __keyevent@0__:set")
	lgr.Info("🎯 Filtering keys: market:nse:*, market:bse:*")

	// ========================================================================
	// DAILY CLEANUP SCHEDULER
	// ========================================================================
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lgr.Info("Running daily cleanup of old breakout state")
				if err := stateMgr.CleanupOldBreakouts(ctx); err != nil {
					lgr.Error("Daily cleanup failed", zap.Error(err))
				} else {
					lgr.Info("Daily cleanup completed successfully")
				}
			}
		}
	}()

	// ========================================================================
	// SERVICE STATUS
	// ========================================================================
	lgr.Info("✓ Data-ingestion service started successfully")
	lgr.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	lgr.Info("Service Mode: EVENT-DRIVEN (Real-time Redis Keyspace Notifications)")
	lgr.Info("Workers", zap.Int("count", cfg.WorkerPoolSize))
	lgr.Info("Kafka Config",
		zap.String("topic", cfg.KafkaTopic52Week),
		zap.Strings("brokers", cfg.KafkaBrokers))
	lgr.Info("Redis Config",
		zap.String("addr", cfg.MarketRedisAddr),
		zap.Int("db", cfg.MarketRedisDB))
	lgr.Info("Timezone", zap.String("tz", "Asia/Kolkata (IST)"))
	lgr.Info("Retention", zap.String("kafka", "7 days"), zap.String("redis_state", "7 days"))
	lgr.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	lgr.Info("✓ Service ready - Press Ctrl+C to stop")

	// ========================================================================
	// GRACEFUL SHUTDOWN
	// ========================================================================
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	lgr.Info("Shutdown signal received, stopping service...")
	cancel()

	// Allow graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	// Wait for graceful shutdown
	<-shutdownCtx.Done()

	if shutdownCtx.Err() == context.DeadlineExceeded {
		lgr.Warn("Graceful shutdown timed out")
	} else {
		lgr.Info("All components shut down gracefully")
	}

	lgr.Info("Data-ingestion service stopped")
}
