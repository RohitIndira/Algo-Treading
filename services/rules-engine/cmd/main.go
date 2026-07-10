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
	"database/sql"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	intkafka "github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/kafka"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/startup"

	_ "github.com/lib/pq"

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

	// Initialize Redis cache (LTP lookup + manthan Pub/Sub).
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

	// Legacy news-event path removed 2026-06-25 (Cat B trim). The engine +
	// matcher + news consumer + handler + RabbitMQ-shaped trade_signal
	// publisher + repository are all gone; rules-engine now drives Manthan
	// exclusively. signalRepo, riskClient, kafkaPub (trade-signals topic),
	// marketHours, holidayChecker, stats were all news-path only.

	// Step 5: Start config consumer — strategy events from user-config.
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

	// --- MANTHAN MODULE ---
	// PostgreSQL connection for manthan portfolio tracking.
	var manthanDB *sql.DB
	if cfg.PostgreSQL.Host != "" {
		manthanConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			cfg.PostgreSQL.Host, cfg.PostgreSQL.Port, cfg.PostgreSQL.User,
			cfg.PostgreSQL.Password, cfg.PostgreSQL.Database, cfg.PostgreSQL.SSLMode)
		manthanDB, err = sql.Open("postgres", manthanConnStr)
		if err != nil {
			logger.Warn("Manthan DB open failed — portfolio tracking disabled", zap.Error(err))
		} else {
			manthanDB.SetMaxOpenConns(10)
			manthanDB.SetMaxIdleConns(5)
			if err := manthanDB.Ping(); err != nil {
				logger.Warn("Manthan DB ping failed — portfolio tracking disabled", zap.Error(err))
				manthanDB = nil
			} else {
				defer manthanDB.Close()
				logger.Info("Manthan PostgreSQL connected")
			}
		}
	}

	// StartLive launches the strategy-events consumer BEFORE Manthan
	// wires its SetOnManthanCreated callback. That's safe because the
	// callback is a field set on configConsumer; if a message arrives
	// before Wire returns, the consumer's nil-check skips the catch-up
	// and the rehydrate step picks the strategy up on the next signal.
	lc := StartLive(ctx, configConsumer, configReader, logger)
	<-lc.ConfigConsumerStarted
	logger.Info("Config consumer started — system is LIVE")

	// Hand Manthan everything it needs and let the wire helper own the
	// rest of the boot order. Was ~290 LOC inline; now lives in
	// internal/manthan/wire.go.
	manthanStack, err := manthan.Wire(ctx, manthan.Deps{
		Logger:               logger,
		ManthanDB:            manthanDB,
		PostgresHost:         cfg.PostgreSQL.Host,
		PostgresPort:         cfg.PostgreSQL.Port,
		PostgresUser:         cfg.PostgreSQL.User,
		PostgresPassword:     cfg.PostgreSQL.Password,
		PostgresSSLMode:      cfg.PostgreSQL.SSLMode,
		SignalsDBName:        os.Getenv("MANTHAN_SIGNALS_DB"),
		Redis:                redisCache,
		KafkaBrokers:         cfg.Kafka.Brokers,
		Store:                store,
		AliveChecker:         ucClient,
		ConfigConsumer:       configConsumer,
		NotificationsEnabled: os.Getenv("MANTHAN_NOTIFICATIONS_ENABLED") == "true",
		ExtRedisAddr:         os.Getenv("EXT_REDIS_ADDR"),
		ExtRedisPassword:     os.Getenv("EXT_REDIS_PASSWORD"),
	})
	if err != nil {
		logger.Fatal("Manthan wire failed", zap.Error(err))
	}
	defer manthanStack.Close()
	manthanStack.Start(ctx)

	logger.Info("Rules Engine Service started successfully")

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("Shutdown received — stopping config consumer")

	lc.StopConfigConsumer()
	logger.Info("Config consumer stopped")

	// Give some time for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Wait for shutdown with timeout
	<-shutdownCtx.Done()

	logger.Info("Rules Engine Service shutdown complete")
}

func loadEnv() {
	// Try multiple locations so `go run ./services/rules-engine/cmd/` from the
	// repo root works without a cd. Priority:
	//   1. ./.env                        (CWD)
	//   2. ./services/rules-engine/.env  (CWD = repo root, common dev case)
	//   3. <exe-dir>/.env                (compiled binary alongside .env)
	//   4. <exe-dir>/../.env             (./bin/rules-engine → ../.env)
	candidates := []string{".env", filepath.Join("services", "rules-engine", ".env")}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, ".env"),
			filepath.Join(exeDir, "..", ".env"),
		)
	}
	for _, p := range candidates {
		if err := godotenv.Overload(p); err == nil {
			fmt.Printf("Loaded .env from %s\n", p)
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
