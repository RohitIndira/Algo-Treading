package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/config"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	intkafka "github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/kafka"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/startup"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/utils"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func main() {
	loadEnv()

	// Create context bound to OS signals
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("failed to load configuration: %v", err))
	}

	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	logger.Info("Starting Rules Engine Service",
		zap.String("version", cfg.ServiceVersion),
		zap.String("environment", cfg.Environment),
		zap.Int("grpc_port", cfg.GRPCPort))

	// Step 2: Initialize empty config store
	store := configstore.New()

	// Step 3: Connect to User Config gRPC with retry (hard requirement)
	ucClient, err := startup.NewUserConfigClient(ctx, cfg.UserConfigGRPCAddr)
	if err != nil {
		logger.Fatal("FATAL: cannot connect to User Config gRPC", zap.Error(err))
	}
	defer ucClient.Close()

	// Step 4: BulkLoad all active strategies (blocks)
	bootstrapper := startup.NewBootstrapper(ucClient, store, logger)
	if err := bootstrapper.Run(ctx); err != nil {
		logger.Fatal("FATAL: bootstrap failed", zap.Error(err))
	}

	// Initialize matching statistics
	stats := models.NewMatchingStats()

	// Initialize PostgreSQL repository for trade signal tracking
	logger.Info("Initializing PostgreSQL trade signal repository...")
	signalRepo, err := repository.NewTradeSignalRepository(
		cfg.PostgreSQL.Host,
		cfg.PostgreSQL.Port,
		cfg.PostgreSQL.User,
		cfg.PostgreSQL.Password,
		cfg.PostgreSQL.Database,
		cfg.PostgreSQL.SSLMode,
		logger,
	)
	if err != nil {
		logger.Warn("Failed to initialize trade signal repository - orders won't be tracked in DB",
			zap.Error(err))
		signalRepo = nil // Continue without DB tracking
	} else {
		defer signalRepo.Close()
		logger.Info("Trade signal repository initialized successfully")
	}

	// Initialize Redis cache (kept for LTP lookup + Pub/Sub)
	logger.Info("Initializing Redis cache...")
	redisCache, err := cache.NewRedisCache(
		cfg.Redis.Addrs,
		cfg.Redis.Password,
		cfg.Redis.DB,
		cfg.Redis.PoolSize,
		cfg.Redis.MinIdleConns,
		cfg.Redis.CacheTTL,
		cfg.Redis.ClusterMode,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to initialize Redis cache", zap.Error(err))
	}
	defer redisCache.Close()
	logger.Info("Redis cache initialized successfully")

	// Initialize risk management client
	logger.Info("Initializing risk management client...")
	riskClient, err := risk.NewClient(risk.Config{
		Address:          cfg.GRPCClients.RiskManagement.Address,
		Timeout:          cfg.GRPCClients.RiskManagement.Timeout,
		MaxRetries:       cfg.GRPCClients.RiskManagement.MaxRetries,
		RetryBackoff:     cfg.GRPCClients.RiskManagement.RetryBackoff,
		KeepAlive:        cfg.GRPCClients.RiskManagement.KeepAlive,
		KeepAliveTimeout: cfg.GRPCClients.RiskManagement.KeepAliveTimeout,
	}, logger)
	if err != nil {
		logger.Warn("Failed to initialize risk management client - orders will be auto-approved",
			zap.Error(err))
		riskClient = nil // Continue without risk checks
	} else {
		defer riskClient.Close()
		logger.Info("Risk management client initialized successfully")
	}

	// Initialize Kafka publisher for trade-signals topic
	logger.Info("Initializing Kafka publisher for trade-signals...")
	kafkaPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		"trade-signals", // Topic for order signals
		logger,
	)
	defer kafkaPub.Close()
	logger.Info("Kafka trade-signals publisher initialized successfully")

	// Initialize market hours from configuration
	logger.Info("Initializing market hours configuration...",
		zap.Int("open_hour", cfg.MarketHours.OpenHour),
		zap.Int("open_minute", cfg.MarketHours.OpenMinute),
		zap.Int("close_hour", cfg.MarketHours.CloseHour),
		zap.Int("close_minute", cfg.MarketHours.CloseMinute),
		zap.String("timezone", cfg.MarketHours.Timezone),
		zap.Bool("enforce_hours", cfg.MarketHours.EnforceHours))

	marketHours := utils.NewMarketHours(
		cfg.MarketHours.OpenHour,
		cfg.MarketHours.OpenMinute,
		cfg.MarketHours.CloseHour,
		cfg.MarketHours.CloseMinute,
		cfg.MarketHours.Timezone,
	)
	logger.Info("Market hours initialized",
		zap.String("status", marketHours.GetMarketStatus()))

	// Initialize event handler
	eng := engine.New(store, engine.Config{Workers: cfg.Performance.WorkerCount}, logger)
	eng.Start(ctx)
	// RabbitMQ is intentionally not initialized (Kafka-only publishing).
	handler := consumer.NewHandler(eng, nil, kafkaPub, signalRepo, riskClient, redisCache, stats, logger, marketHours, cfg.MarketHours.EnforceHours)

	// Step 5: Start config consumer BEFORE news consumer
	configReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.UserConfigTopic,
		GroupID:        cfg.Kafka.UserConfigGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    startOffsetFor(cfg.Kafka.ConfigOffsetReset, kafka.FirstOffset),
	})
	configConsumer := intkafka.NewConfigConsumer(configReader, store)

	// Step 6: Start news consumer AFTER config consumer
	newsReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.Topic,
		GroupID:        cfg.Kafka.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       cfg.Kafka.MaxBytes,
		CommitInterval: cfg.Kafka.CommitInterval,
		StartOffset:    startOffsetFor(cfg.Kafka.StartOffset, kafka.LastOffset),
		MaxWait:        time.Second,
	})
	newsConsumer := intkafka.NewNewsConsumer(newsReader, handler, logger)

	lc := StartLive(ctx, eng, configConsumer, newsConsumer, configReader, logger)
	<-lc.ConfigConsumerStarted
	logger.Info("Config consumer started")
	<-lc.NewsConsumerStarted
	logger.Info("News consumer started — system is LIVE")

	// TODO: Start existing gRPC server (if any) - currently none in rules-engine.
	// TODO: Start existing Redis Pub/Sub publisher - handled in consumer handler via redisCache.Publish.

	// Log periodic statistics
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				logger.Info("Matching statistics",
					zap.Int64("events_processed", stats.TotalEventsProcessed),
					zap.Int64("matches_found", stats.TotalMatchesFound),
					zap.Int64("orders_generated", stats.TotalOrdersGenerated),
					zap.Int64("cache_hits", stats.CacheHits),
					zap.Int64("cache_misses", stats.CacheMisses))
			case <-ctx.Done():
				return
			}
		}
	}()

	logger.Info("Rules Engine Service started successfully",
		zap.Int("worker_count", cfg.Performance.WorkerCount))

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("Shutdown received — stopping news consumer then draining worker pool")

	// Stop news consumer first (stop submitting new jobs), drain engine, then stop config consumer.
	lc.StopNewsFirstThenDrainEngineThenStopConfig()
	logger.Info("Worker pool drained")
	logger.Info("Config consumer stopped")

	// Give some time for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Wait for shutdown with timeout
	<-shutdownCtx.Done()

	// Log final statistics
	logger.Info("Final statistics",
		zap.Int64("total_events_processed", stats.TotalEventsProcessed),
		zap.Int64("total_matches_found", stats.TotalMatchesFound),
		zap.Int64("total_orders_generated", stats.TotalOrdersGenerated),
		zap.Int64("total_errors", stats.EvaluationErrors+stats.KafkaErrors+stats.RabbitMQErrors))

	logger.Info("Rules Engine Service shutdown complete")
}

func loadEnv() {
	// Try current working directory first.
	if err := godotenv.Overload(".env"); err == nil {
		fmt.Printf("Loaded .env from %s\n", filepath.Join(".", ".env"))
		return
	}

	// Try directory of the running binary (so ./bin/rules-engine finds ../.env).
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), ".env")
		if err := godotenv.Overload(candidate); err == nil {
			fmt.Printf("Loaded .env from %s\n", candidate)
			return
		}
	}

	fmt.Printf("Note: .env file not found, using system environment variables\n")
}

func startOffsetFor(v string, defaultOffset int64) int64 {
	switch v {
	case "earliest":
		return kafka.FirstOffset
	case "latest":
		return kafka.LastOffset
	default:
		return defaultOffset
	}
}
