package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/config"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/index"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"

	"go.uber.org/zap"
)

func main() {
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

	// Initialize matching statistics
	stats := models.NewMatchingStats()

	// Initialize Redis cache
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

	strategyCache := cache.NewStrategyCache(redisCache, cfg.Redis.CacheTTL, logger)
	logger.Info("Redis cache initialized successfully")

	// Initialize Elasticsearch indexer
	logger.Info("Initializing Elasticsearch indexer...")
	indexer, err := index.NewIndexer(
		cfg.Elasticsearch.URLs,
		cfg.Elasticsearch.Username,
		cfg.Elasticsearch.Password,
		cfg.Elasticsearch.IndexName,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to initialize Elasticsearch indexer", zap.Error(err))
	}
	defer indexer.Close()
	logger.Info("Elasticsearch indexer initialized successfully")

	// Initialize query engine
	queryEngine := index.NewQueryEngine(
		indexer.GetClient(),
		cfg.Elasticsearch.IndexName,
		cfg.Elasticsearch.Timeout,
		logger,
	)

	// Initialize matcher
	logger.Info("Initializing matcher engine...")
	matcherEngine := matcher.NewMatcher(
		queryEngine,
		strategyCache,
		cfg.Performance.MinMatchScore,
		cfg.Performance.MaxConcurrentMatches,
		logger,
	)
	logger.Info("Matcher engine initialized successfully")

	// Initialize RabbitMQ publisher
	logger.Info("Initializing RabbitMQ publisher...")
	pub, err := publisher.NewPublisher(&cfg.RabbitMQ, logger)
	if err != nil {
		logger.Fatal("Failed to initialize RabbitMQ publisher", zap.Error(err))
	}
	defer pub.Close()
	logger.Info("RabbitMQ publisher initialized successfully")

	// Initialize event handler
	handler := consumer.NewHandler(matcherEngine, pub, stats, logger)

	// Initialize Kafka consumer
	logger.Info("Initializing Kafka consumer...")
	kafkaConsumer, err := consumer.NewConsumer(&cfg.Kafka, handler, stats, logger)
	if err != nil {
		logger.Fatal("Failed to initialize Kafka consumer", zap.Error(err))
	}
	defer kafkaConsumer.Close()
	logger.Info("Kafka consumer initialized successfully")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming messages
	go func() {
		logger.Info("Starting to consume messages from Kafka",
			zap.String("topic", cfg.Kafka.Topic),
			zap.String("consumer_group", cfg.Kafka.ConsumerGroup))
		
		if err := kafkaConsumer.Start(ctx); err != nil {
			logger.Error("Kafka consumer error", zap.Error(err))
		}
	}()

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
		zap.Int("worker_count", cfg.Performance.WorkerCount),
		zap.Float64("min_match_score", cfg.Performance.MinMatchScore))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	logger.Info("Received shutdown signal, starting graceful shutdown...")	// Cancel context to stop all goroutines
	cancel()

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
