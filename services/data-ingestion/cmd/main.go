package main

import (
	"context"
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
	cfg := config.Load() //– calls `Load()` from `config` package

	lgr, err := logger.NewWithDefaults("data-ingestion")
	if err != nil {
		panic(err)
	}
	defer lgr.Sync()

	lgr.Info("Starting data-ingestion service")

	// Initialize MongoDB client (news ingestion) Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), cfg.MongoConnectTimeout)
	defer cancel()

	mongoClient, err := mongodb.New(ctx, mongodb.Config{URI: cfg.MongoURI, Database: cfg.MongoDatabase, ConnectTimeout: cfg.MongoConnectTimeout})
	if err != nil {
		lgr.Fatal("failed to connect to mongodb", zap.Error(err))
	}
	defer mongoClient.Close(context.Background())

	lgr.Info("Connected to MongoDB", zap.String("database", cfg.MongoDatabase), zap.String("collection", cfg.MongoCollection))

	// Initialize Kafka producer for news events
	prodCfg := kafkapkg.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic,
		BatchSize:   100,
		MaxAttempts: 3,
	}
	producer, err := kafkapkg.NewProducer(prodCfg)
	if err != nil {
		lgr.Fatal("failed to create kafka producer", zap.Error(err))
	}
	defer producer.Close()
	// Ensure Kafka topic exists (auto-create if missing)
	if err := kafkapkg.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTopic, 1, 1); err != nil {
		lgr.Fatal("failed to ensure topic exists", zap.Error(err))
	}
	lgr.Info("Connected to Kafka (news)", zap.Strings("brokers", cfg.KafkaBrokers), zap.String("topic", cfg.KafkaTopic))

	// Create publisher wrapper for news
	pub := publisher.NewKafkaPublisher(producer, cfg.KafkaTopic)

	// Initialize Kafka producer for 52-week breakout events
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
	lgr.Info("Connected to Kafka (52w breakouts)", zap.Strings("brokers", cfg.KafkaBrokers), zap.String("topic", cfg.KafkaTopic52Week))

	breakoutPub := publisher.NewKafkaPublisher(breakoutProducer, cfg.KafkaTopic52Week)

	// Start MongoDB -> Kafka watcher (news)
	ctxRun, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	w, err := watcher.NewMongoWatcher(mongoClient, cfg.MongoCollection, pub, lgr)
	if err != nil {
		lgr.Fatal("failed to create watcher", zap.Error(err))
	}

	go func() {
		if err := w.Run(ctxRun); err != nil {
			lgr.Error("mongo watcher stopped with error", zap.Error(err))
			cancelRun()
		}
	}()

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
		zap.String("service", "data-ingestion"),
		zap.String("addr", cfg.MarketRedisAddr))

	// Start Redis -> Kafka watcher for 52-week high breakouts
	redisWatcher := watcher.NewRedis52WWatcher(redisClient, breakoutPub, cfg.MarketRedisPollInterval, lgr)
	go func() {
		if err := redisWatcher.Run(ctxRun); err != nil {
			lgr.Error("redis 52w watcher stopped with error", zap.Error(err))
			cancelRun()
		}
	}()

	// Wait for termination
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	lgr.Info("shutting down data-ingestion service")
	// allow graceful shutdown
	tctx, tcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer tcancel()
	cancelRun()
	// give components time to cleanup
	<-tctx.Done()
}
