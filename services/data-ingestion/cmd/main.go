package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	redispkg "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	kafkapkg "github.com/RohitIndira/Algo-Treading/pkg/kafka"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/watcher"

	"go.uber.org/zap"
)

func main() {
	fmt.Println("In the main function of main.go file in data-ingestion service")
	cfg := config.Load() // calls `Load()` from `config` package
	fmt.Println("Configuration loaded successfully")

	lgr, err := logger.NewWithDefaults("data-ingestion")
	if err != nil {
		panic(err)
	}
	defer lgr.Sync()
	fmt.Println("2.Starting unified data-ingestion service (MongoDB News + Redis 52W + B2C Market Data)")
	lgr.Info("3. Starting unified data-ingestion service (MongoDB News + Redis 52W + B2C Market Data)")

	// Context for the entire application
	ctxRun, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	// ========================================================================
	// 1. MONGODB NEWS INGESTION SETUP
	// ========================================================================
	lgr.Info("Initializing MongoDB news ingestion pipeline")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.MongoConnectTimeout)
	mongoClient, err := mongodb.New(ctx, mongodb.Config{
		URI:            cfg.MongoURI,
		Database:       cfg.MongoDatabase,
		ConnectTimeout: cfg.MongoConnectTimeout,
	})
	cancel()

	if err != nil {
		lgr.Fatal("failed to connect to mongodb", zap.Error(err))
	}
	defer mongoClient.Close(context.Background())

	lgr.Info("Connected to MongoDB",
		zap.String("database", cfg.MongoDatabase),
		zap.String("collection", cfg.MongoCollection))

	// Kafka producer for news events
	newsProdCfg := kafkapkg.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic,
		BatchSize:   100,
		MaxAttempts: 3,
	}
	newsProducer, err := kafkapkg.NewProducer(newsProdCfg)
	if err != nil {
		lgr.Fatal("failed to create kafka producer for news", zap.Error(err))
	}
	defer newsProducer.Close()

	if err := kafkapkg.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTopic, 1, 1); err != nil {
		lgr.Fatal("failed to ensure news topic exists", zap.Error(err))
	}
	lgr.Info("Connected to Kafka (news)",
		zap.Strings("brokers", cfg.KafkaBrokers),
		zap.String("topic", cfg.KafkaTopic))

	newsPub := publisher.NewKafkaPublisher(newsProducer, cfg.KafkaTopic)

	// Start MongoDB -> Kafka watcher (news)
	mongoWatcher, err := watcher.NewMongoWatcher(mongoClient, cfg.MongoCollection, newsPub, lgr)
	if err != nil {
		lgr.Fatal("failed to create mongo watcher", zap.Error(err))
	}

	go func() {
		lgr.Info("Starting MongoDB news watcher")
		if err := mongoWatcher.Run(ctxRun); err != nil {
			lgr.Error("mongo watcher stopped with error", zap.Error(err))
		}
	}()

	// ========================================================================
	// 2. REDIS 52-WEEK HIGH BREAKOUT SETUP
	// ========================================================================
	lgr.Info("Initializing Redis 52-week high breakout pipeline")
	fmt.Println("Initializing Redis 52-week high breakout pipeline----------------------------------**********")

	// Kafka producer for 52-week breakout events
	breakoutProdCfg := kafkapkg.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic52Week,
		BatchSize:   100,
		MaxAttempts: 3,
	}
	breakoutProducer, err := kafkapkg.NewProducer(breakoutProdCfg)
	if err != nil {
		lgr.Fatal("failed to create 52w-breakout kafka producer", zap.Error(err))
	}
	defer breakoutProducer.Close()

	if err := kafkapkg.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTopic52Week, 1, 1); err != nil {
		lgr.Fatal("failed to ensure 52w-breakout topic exists", zap.Error(err))
	}
	lgr.Info("Connected to Kafka (52w breakouts)",
		zap.Strings("brokers", cfg.KafkaBrokers),
		zap.String("topic", cfg.KafkaTopic52Week))

	breakoutPub := publisher.NewKafkaPublisher(breakoutProducer, cfg.KafkaTopic52Week)

	// Initialize Redis client for market data (52-week highs)
	redisClient, err := redispkg.New(redispkg.Config{
		Address:      cfg.MarketRedisAddr,
		Password:     cfg.MarketRedisPassword,
		DB:           cfg.MarketRedisDB,
		PoolSize:     100,
		MinIdleConns: 10,
	})
	if err != nil {
		lgr.Fatal("failed to connect to market redis", zap.Error(err))
	}
	defer redisClient.Close()

	lgr.Info("Connected to market Redis for 52w highs",
		zap.String("addr", cfg.MarketRedisAddr))

	// Start Redis -> Kafka watcher for 52-week high breakouts
	redisWatcher := watcher.NewRedis52WWatcher(
		redisClient,
		breakoutPub,
		cfg.MarketRedisPollInterval,
		lgr,
	)

	go func() {
		lgr.Info("Starting Redis 52-week high watcher")
		if err := redisWatcher.Run(ctxRun); err != nil {
			lgr.Error("redis 52w watcher stopped with error", zap.Error(err))
		}
	}()

	// ========================================================================
	// 3. B2C LIVE MARKET DATA SETUP
	// ========================================================================
	lgr.Info("Initializing B2C live market data pipeline")

	// Validate B2C configuration
	if cfg.B2CBridgePath == "" {
		lgr.Warn("B2C_BRIDGE_PATH not set, skipping B2C market data ingestion")
	} else {
		// Kafka producer for live market data
		marketDataProdCfg := kafkapkg.ProducerConfig{
			Brokers:     cfg.KafkaBrokers,
			Topic:       cfg.KafkaTopicMarketData,
			BatchSize:   100,
			MaxAttempts: 3,
		}
		marketDataProducer, err := kafkapkg.NewProducer(marketDataProdCfg)
		if err != nil {
			lgr.Fatal("failed to create kafka producer for market data", zap.Error(err))
		}
		defer marketDataProducer.Close()

		if err := kafkapkg.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTopicMarketData, 1, 1); err != nil {
			lgr.Fatal("failed to ensure market data topic exists", zap.Error(err))
		}
		lgr.Info("Connected to Kafka (live market data)",
			zap.Strings("brokers", cfg.KafkaBrokers),
			zap.String("topic", cfg.KafkaTopicMarketData))

		marketDataPub := publisher.NewKafkaPublisher(marketDataProducer, cfg.KafkaTopicMarketData)

		lgr.Info("B2C Configuration loaded",
			zap.String("bridge_path", cfg.B2CBridgePath),
			zap.Strings("tokens_env", cfg.B2CTokens),
			zap.String("stocks_db_path", cfg.StocksDBPath),
		)

		// Start B2C watcher (will derive subscriptions from stocks.db when
		// available, falling back to cfg.B2CTokens if needed).
		b2cWatcher, err := watcher.NewB2CWatcher(
			cfg.B2CBridgePath,
			cfg.B2CTokens,
			cfg.StocksDBPath,
			marketDataPub,
			lgr,
		)
		if err != nil {
			lgr.Fatal("failed to create B2C watcher", zap.Error(err))
		}

		go func() {
			lgr.Info("Starting B2C market data watcher")
			if err := b2cWatcher.Run(ctxRun); err != nil {
				lgr.Error("B2C watcher stopped with error", zap.Error(err))
			}
		}()
	}

	// ========================================================================
	// GRACEFUL SHUTDOWN
	// ========================================================================
	lgr.Info("All data ingestion pipelines started successfully")
	lgr.Info("Service is running. Press Ctrl+C to stop.")

	// Wait for termination signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	lgr.Info("Shutdown signal received, stopping all pipelines...")

	// Cancel all running goroutines
	cancelRun()

	// Allow graceful shutdown with timeout
	tctx, tcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer tcancel()

	// Wait for graceful shutdown or timeout
	<-tctx.Done()

	if tctx.Err() == context.DeadlineExceeded {
		lgr.Warn("Graceful shutdown timed out")
	} else {
		lgr.Info("All components shut down gracefully")
	}

	lgr.Info("Data-ingestion service stopped")
}
