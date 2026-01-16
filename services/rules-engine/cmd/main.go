// package main

// import (
// 	"context"
// 	"fmt"
// 	"os"
// 	"os/signal"
// 	"syscall"
// 	"time"

// 	"github.com/joho/godotenv"

// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/config"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cash52w"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/index"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/sync"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/userconfig"
// 	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/utils"

// 	"go.uber.org/zap"
// )

// func main() {
// 	// Load .env file if it exists
// 	if err := godotenv.Load(); err != nil {
// 		// .env file not found is not an error - we can use system env vars
// 		fmt.Printf("Note: .env file not found, using system environment variables\n")
// 	}

// 	// Load configuration
// 	cfg, err := config.LoadConfig()
// 	if err != nil {
// 		panic(fmt.Sprintf("failed to load configuration: %v", err))
// 	}

// 	// Initialize logger
// 	logger, err := zap.NewProduction()
// 	if err != nil {
// 		panic(fmt.Sprintf("failed to initialize logger: %v", err))
// 	}
// 	defer logger.Sync()

// 	logger.Info("Starting Rules Engine Service",
// 		zap.String("version", cfg.ServiceVersion),
// 		zap.String("environment", cfg.Environment),
// 		zap.Int("grpc_port", cfg.GRPCPort))

// 	// Initialize matching statistics
// 	stats := models.NewMatchingStats()

// 	// Initialize PostgreSQL repository for trade signal tracking
// 	logger.Info("Initializing PostgreSQL trade signal repository...")
// 	signalRepo, err := repository.NewTradeSignalRepository(
// 		cfg.PostgreSQL.Host,
// 		cfg.PostgreSQL.Port,
// 		cfg.PostgreSQL.User,
// 		cfg.PostgreSQL.Password,
// 		cfg.PostgreSQL.Database,
// 		cfg.PostgreSQL.SSLMode,
// 		logger,
// 	)
// 	if err != nil {
// 		logger.Warn("Failed to initialize trade signal repository - orders won't be tracked in DB",
// 			zap.Error(err))
// 		signalRepo = nil // Continue without DB tracking
// 	} else {
// 		defer signalRepo.Close()
// 		logger.Info("Trade signal repository initialized successfully")
// 	}

// 	// Initialize Redis cache
// 	logger.Info("Initializing Redis cache...")
// 	redisCache, err := cache.NewRedisCache(
// 		cfg.Redis.Addrs,
// 		cfg.Redis.Password,
// 		cfg.Redis.DB,
// 		cfg.Redis.PoolSize,
// 		cfg.Redis.MinIdleConns,
// 		cfg.Redis.CacheTTL,
// 		cfg.Redis.ClusterMode,
// 		logger,
// 	)
// 	if err != nil {
// 		logger.Fatal("Failed to initialize Redis cache", zap.Error(err))
// 	}
// 	defer redisCache.Close()

// 	strategyCache := cache.NewStrategyCache(redisCache, cfg.Redis.CacheTTL, logger)
// 	logger.Info("Redis cache initialized successfully")

// 	// Initialize Elasticsearch indexer
// 	logger.Info("Initializing Elasticsearch indexer...")
// 	indexer, err := index.NewIndexer(
// 		cfg.Elasticsearch.URLs,
// 		cfg.Elasticsearch.Username,
// 		cfg.Elasticsearch.Password,
// 		cfg.Elasticsearch.IndexName,
// 		logger,
// 	)
// 	if err != nil {
// 		logger.Fatal("Failed to initialize Elasticsearch indexer", zap.Error(err))
// 	}
// 	defer indexer.Close()
// 	logger.Info("Elasticsearch indexer initialized successfully")

// 	// Initialize strategy syncer (loads strategies from Kafka user-configs topic)
// 	logger.Info("Initializing strategy syncer...")
// 	strategySyncer := sync.NewStrategySyncer(
// 		cfg.Kafka.Brokers,
// 		"user-configs",                  // Topic where user-config service publishes strategy events
// 		"rules-engine-strategy-sync-v2", // Changed group to avoid replaying old DELETE events
// 		indexer,
// 		strategyCache,
// 		logger,
// 	)
// 	defer strategySyncer.Close()
// 	logger.Info("Strategy syncer initialized successfully")

// 	// Initialize query engine
// 	queryEngine := index.NewQueryEngine(
// 		indexer.GetClient(),
// 		cfg.Elasticsearch.IndexName,
// 		cfg.Elasticsearch.Timeout,
// 		logger,
// 	)

// 	// Initialize user-config client for fetching strategies
// 	logger.Info("Initializing user-config client...")
// 	userConfigClient, err := userconfig.NewClient(userconfig.Config{
// 		Address:          cfg.GRPCClients.UserConfigService.Address,
// 		Timeout:          cfg.GRPCClients.UserConfigService.Timeout,
// 		MaxRetries:       cfg.GRPCClients.UserConfigService.MaxRetries,
// 		RetryBackoff:     cfg.GRPCClients.UserConfigService.RetryBackoff,
// 		KeepAlive:        cfg.GRPCClients.UserConfigService.KeepAlive,
// 		KeepAliveTimeout: cfg.GRPCClients.UserConfigService.KeepAliveTimeout,
// 	}, logger)
// 	if err != nil {
// 		logger.Warn("Failed to initialize user-config client - will rely only on cache",
// 			zap.Error(err))
// 		userConfigClient = nil
// 	} else {
// 		defer userConfigClient.Close()
// 		logger.Info("User-config client initialized successfully")
// 	}

// 	// Initialize matcher
// 	logger.Info("Initializing matcher engine...")
// 	matcherEngine := matcher.NewMatcher(
// 		queryEngine,
// 		strategyCache,
// 		userConfigClient,
// 		cfg.Performance.MinMatchScore,
// 		cfg.Performance.MaxConcurrentMatches,
// 		logger,
// 	)
// 	logger.Info("Matcher engine initialized successfully")

// 	// Initialize risk management client (DISABLED for development).
// 	// For now we bypass risk checks entirely so that we can validate
// 	// end-to-end flow of all microservices. The Cash 52W engine and
// 	// news-based handler will auto-approve orders when riskClient is nil.
// 	//
// 	// To re-enable risk in future, restore the original initialization
// 	// using risk.NewClient and pass the resulting client into handlers.
// 	logger.Warn("Risk management client disabled for development - orders will be auto-approved")
// 	var riskClient *risk.Client = nil

// 	// Initialize RabbitMQ publisher
// 	logger.Info("Initializing RabbitMQ publisher...")
// 	rabbitPub, err := publisher.NewPublisher(&cfg.RabbitMQ, logger)
// 	if err != nil {
// 		logger.Fatal("Failed to initialize RabbitMQ publisher", zap.Error(err))
// 	}
// 	defer rabbitPub.Close()
// 	logger.Info("RabbitMQ publisher initialized successfully")

// 	// Initialize Kafka publisher for trade-signals topic
// 	logger.Info("Initializing Kafka publisher for trade-signals...")
// 	tradeSignalPub := publisher.NewKafkaPublisher(
// 		cfg.Kafka.Brokers,
// 		"trade-signals", // Topic for order signals
// 		logger,
// 	)
// 	defer tradeSignalPub.Close()
// 	logger.Info("Kafka trade-signals publisher initialized successfully")

// 	// Initialize Kafka publisher for portfolio allocation state events
// 	logger.Info("Initializing Kafka publisher for portfolio allocations...",
// 		zap.String("topic", cfg.PortfolioAllocTopic))
// 	allocationPub := publisher.NewKafkaPublisher(
// 		cfg.Kafka.Brokers,
// 		cfg.PortfolioAllocTopic,
// 		logger,
// 	)
// 	defer allocationPub.Close()
// 	logger.Info("Kafka portfolio allocations publisher initialized successfully")

// 	// Initialize Cash 52-week High strategy engine (if configured)
// 	var cash52wEngine *cash52w.Engine
// 	var breakoutConsumer *consumer.BreakoutConsumer
// 	if len(cfg.Cash52WUserIDs) > 0 && cfg.Cash52WTopic != "" {
// 		logger.Info("Initializing Cash 52-week High engine",
// 			zap.String("topic", cfg.Cash52WTopic),
// 			zap.Strings("user_ids", cfg.Cash52WUserIDs))

// 		engineCfg := cash52w.Config{
// 			UserIDs:         cfg.Cash52WUserIDs,
// 			CapitalPerStock: 20000,
// 			MaxPositions:    25,
// 			SLPercent:       10,
// 			TSLPercent:      20,
// 		}
// 		cash52wEngine = cash52w.NewEngine(engineCfg, riskClient, rabbitPub, tradeSignalPub, allocationPub, logger)

// 		breakoutConsumer, err = consumer.NewBreakoutConsumer(
// 			cfg.Kafka.Brokers,
// 			cfg.Cash52WTopic,
// 			"", // use default versioned group with earliest offsets for same-day backlog
// 			cash52wEngine,
// 			logger,
// 		)
// 		if err != nil {
// 			logger.Error("Failed to initialize 52w-breakout consumer", zap.Error(err))
// 		} else {
// 			defer breakoutConsumer.Close()
// 			logger.Info("52w-breakout consumer initialized successfully")
// 		}
// 	} else {
// 		logger.Info("Cash 52-week High engine disabled (no CASH52W_USER_IDS configured)")
// 	}

// 	// Initialize market hours from configuration
// 	logger.Info("Initializing market hours configuration...",
// 		zap.Int("open_hour", cfg.MarketHours.OpenHour),
// 		zap.Int("open_minute", cfg.MarketHours.OpenMinute),
// 		zap.Int("close_hour", cfg.MarketHours.CloseHour),
// 		zap.Int("close_minute", cfg.MarketHours.CloseMinute),
// 		zap.String("timezone", cfg.MarketHours.Timezone),
// 		zap.Bool("enforce_hours", cfg.MarketHours.EnforceHours))

// 	marketHours := utils.NewMarketHours(
// 		cfg.MarketHours.OpenHour,
// 		cfg.MarketHours.OpenMinute,
// 		cfg.MarketHours.CloseHour,
// 		cfg.MarketHours.CloseMinute,
// 		cfg.MarketHours.Timezone,
// 	)
// 	logger.Info("Market hours initialized",
// 		zap.String("status", marketHours.GetMarketStatus()))

// 	// Initialize event handler.
// 	// For now we disable market-hours enforcement so that orders can be
// 	// generated at any time during development/testing. Once the behaviour
// 	// is validated end-to-end, this can be switched back to
// 	// cfg.MarketHours.EnforceHours.
// 	handler := consumer.NewHandler(matcherEngine, rabbitPub, tradeSignalPub, signalRepo, riskClient, redisCache, strategyCache, stats, logger, marketHours, false)

// 	// Initialize Kafka consumer
// 	logger.Info("Initializing Kafka consumer...")
// 	kafkaConsumer, err := consumer.NewConsumer(&cfg.Kafka, handler, stats, logger)
// 	if err != nil {
// 		logger.Fatal("Failed to initialize Kafka consumer", zap.Error(err))
// 	}
// 	defer kafkaConsumer.Close()
// 	logger.Info("Kafka consumer initialized successfully")

// 	// Create context for graceful shutdown
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	// Start strategy syncer (loads from Kafka user-configs topic)
// 	go func() {
// 		logger.Info("Starting strategy syncer from Kafka user-configs topic...")
// 		if err := strategySyncer.Start(ctx); err != nil {
// 			logger.Error("Strategy syncer error", zap.Error(err))
// 		}
// 	}()

// 	// Start consuming 52-week breakout events if engine is enabled
// 	if breakoutConsumer != nil && cash52wEngine != nil {
// 		go func() {
// 			logger.Info("Starting 52w-breakout consumer",
// 				zap.String("topic", cfg.Cash52WTopic))
// 			if err := breakoutConsumer.Start(ctx); err != nil {
// 				logger.Error("52w-breakout consumer error", zap.Error(err))
// 			}
// 		}()
// 	}

// 	// Start consuming market events from Kafka
// 	go func() {
// 		logger.Info("Starting to consume market events from Kafka",
// 			zap.String("topic", cfg.Kafka.Topic),
// 			zap.String("consumer_group", cfg.Kafka.ConsumerGroup))

// 		if err := kafkaConsumer.Start(ctx); err != nil {
// 			logger.Error("Kafka consumer error", zap.Error(err))
// 		}
// 	}()

// 	// Log periodic statistics
// 	go func() {
// 		ticker := time.NewTicker(30 * time.Second)
// 		defer ticker.Stop()

// 		for {
// 			select {
// 			case <-ticker.C:
// 				syncStats := strategySyncer.GetStats()
// 				logger.Info("Matching statistics",
// 					zap.Int64("events_processed", stats.TotalEventsProcessed),
// 					zap.Int64("matches_found", stats.TotalMatchesFound),
// 					zap.Int64("orders_generated", stats.TotalOrdersGenerated),
// 					zap.Int64("cache_hits", stats.CacheHits),
// 					zap.Int64("cache_misses", stats.CacheMisses))
// 				logger.Info("Strategy sync statistics",
// 					zap.Int64("strategies_synced", syncStats.TotalProcessed),
// 					zap.Int64("created", syncStats.Created),
// 					zap.Int64("updated", syncStats.Updated),
// 					zap.Int64("deleted", syncStats.Deleted),
// 					zap.Int64("sync_errors", syncStats.Errors))
// 			case <-ctx.Done():
// 				return
// 			}
// 		}
// 	}()

// 	logger.Info("Rules Engine Service started successfully",
// 		zap.Int("worker_count", cfg.Performance.WorkerCount),
// 		zap.Float64("min_match_score", cfg.Performance.MinMatchScore))

// 	// Wait for interrupt signal
// 	sigCh := make(chan os.Signal, 1)
// 	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

// 	<-sigCh
// 	logger.Info("Received shutdown signal, starting graceful shutdown...") // Cancel context to stop all goroutines
// 	cancel()

// 	// Give some time for graceful shutdown
// 	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer shutdownCancel()

// 	// Wait for shutdown with timeout
// 	<-shutdownCtx.Done()

// 	// Log final statistics
// 	logger.Info("Final statistics",
// 		zap.Int64("total_events_processed", stats.TotalEventsProcessed),
// 		zap.Int64("total_matches_found", stats.TotalMatchesFound),
// 		zap.Int64("total_orders_generated", stats.TotalOrdersGenerated),
// 		zap.Int64("total_errors", stats.EvaluationErrors+stats.KafkaErrors+stats.RabbitMQErrors))

// 	logger.Info("Rules Engine Service shutdown complete")
// }

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/config"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cash52w"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/index"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/jobbing"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/sync"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/userconfig"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/utils"

	"go.uber.org/zap"
)

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// .env file not found is not an error - we can use system env vars
		fmt.Printf("Note: .env file not found, using system environment variables\n")
	}

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

	// Initialize strategy syncer (loads strategies from Kafka user-configs topic)
	logger.Info("Initializing strategy syncer...")
	strategySyncer := sync.NewStrategySyncer(
		cfg.Kafka.Brokers,
		"user-configs",                  // Topic where user-config service publishes strategy events
		"rules-engine-strategy-sync-v2", // Changed group to avoid replaying old DELETE events
		indexer,
		strategyCache,
		logger,
	)
	defer strategySyncer.Close()
	logger.Info("Strategy syncer initialized successfully")

	// Initialize query engine
	queryEngine := index.NewQueryEngine(
		indexer.GetClient(),
		cfg.Elasticsearch.IndexName,
		cfg.Elasticsearch.Timeout,
		logger,
	)

	// Initialize user-config client for fetching strategies
	logger.Info("Initializing user-config client...")
	userConfigClient, err := userconfig.NewClient(userconfig.Config{
		Address:          cfg.GRPCClients.UserConfigService.Address,
		Timeout:          cfg.GRPCClients.UserConfigService.Timeout,
		MaxRetries:       cfg.GRPCClients.UserConfigService.MaxRetries,
		RetryBackoff:     cfg.GRPCClients.UserConfigService.RetryBackoff,
		KeepAlive:        cfg.GRPCClients.UserConfigService.KeepAlive,
		KeepAliveTimeout: cfg.GRPCClients.UserConfigService.KeepAliveTimeout,
	}, logger)
	if err != nil {
		logger.Warn("Failed to initialize user-config client - will rely only on cache",
			zap.Error(err))
		userConfigClient = nil
	} else {
		defer userConfigClient.Close()
		logger.Info("User-config client initialized successfully")
	}

	// Initialize matcher
	logger.Info("Initializing matcher engine...")
	matcherEngine := matcher.NewMatcher(
		queryEngine,
		strategyCache,
		userConfigClient,
		cfg.Performance.MinMatchScore,
		cfg.Performance.MaxConcurrentMatches,
		logger,
	)
	logger.Info("Matcher engine initialized successfully")

	// Initialize risk management client (DISABLED for development).
	// For now we bypass risk checks entirely so that we can validate
	// end-to-end flow of all microservices. The Cash 52W engine and
	// news-based handler will auto-approve orders when riskClient is nil.
	//
	// To re-enable risk in future, restore the original initialization
	// using risk.NewClient and pass the resulting client into handlers.
	logger.Warn("Risk management client disabled for development - orders will be auto-approved")
	var riskClient *risk.Client = nil

	// Initialize RabbitMQ publisher
	logger.Info("Initializing RabbitMQ publisher...")
	rabbitPub, err := publisher.NewPublisher(&cfg.RabbitMQ, logger)
	if err != nil {
		logger.Fatal("Failed to initialize RabbitMQ publisher", zap.Error(err))
	}
	defer rabbitPub.Close()
	logger.Info("RabbitMQ publisher initialized successfully")

	// Initialize Kafka publisher for trade-signals topic
	logger.Info("Initializing Kafka publisher for trade-signals...")
	tradeSignalPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		"trade-signals", // Topic for order signals
		logger,
	)
	defer tradeSignalPub.Close()
	logger.Info("Kafka trade-signals publisher initialized successfully")

	// Initialize Kafka publisher for portfolio allocation state events
	logger.Info("Initializing Kafka publisher for portfolio allocations...",
		zap.String("topic", cfg.PortfolioAllocTopic))
	allocationPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		cfg.PortfolioAllocTopic,
		logger,
	)
	defer allocationPub.Close()
	logger.Info("Kafka portfolio allocations publisher initialized successfully")

	// Initialize Cash 52-week High strategy engine (if configured)
	var cash52wEngine *cash52w.Engine
	var breakoutConsumer *consumer.BreakoutConsumer
	if len(cfg.Cash52WUserIDs) > 0 && cfg.Cash52WTopic != "" {
		logger.Info("Initializing Cash 52-week High engine",
			zap.String("topic", cfg.Cash52WTopic),
			zap.Strings("user_ids", cfg.Cash52WUserIDs))

		engineCfg := cash52w.Config{
			UserIDs:         cfg.Cash52WUserIDs,
			CapitalPerStock: 20000,
			MaxPositions:    25,
			SLPercent:       10,
			TSLPercent:      20,
		}
		cash52wEngine = cash52w.NewEngine(engineCfg, riskClient, rabbitPub, tradeSignalPub, allocationPub, logger)

		breakoutConsumer, err = consumer.NewBreakoutConsumer(
			cfg.Kafka.Brokers,
			cfg.Cash52WTopic,
			"", // use default versioned group with earliest offsets for same-day backlog
			cash52wEngine,
			logger,
		)
		if err != nil {
			logger.Error("Failed to initialize 52w-breakout consumer", zap.Error(err))
		} else {
			defer breakoutConsumer.Close()
			logger.Info("52w-breakout consumer initialized successfully")
		}
	} else {
		logger.Info("Cash 52-week High engine disabled (no CASH52W_USER_IDS configured)")
	}

	// Initialize Jobbing strategy engine (if configured)
	var jobbingEngine *jobbing.Engine
	var jobbingConsumer *consumer.JobbingConsumer
	if len(cfg.JobbingUserIDs) > 0 && cfg.JobbingTopic != "" {
		logger.Info("Initializing Jobbing strategy engine",
			zap.String("topic", cfg.JobbingTopic),
			zap.Strings("user_ids", cfg.JobbingUserIDs),
			zap.Strings("tokens", cfg.JobbingTokens))

		jobbingCfg := jobbing.Config{
			UserIDs:          cfg.JobbingUserIDs,
			LowerRange:       cfg.JobbingLowerRange,
			HigherRange:      cfg.JobbingHigherRange,
			InitialBuyOffset: cfg.JobbingInitialOffset,
			DistanceContinue: cfg.JobbingDistanceContinue,
			QuantityPerOrder: cfg.JobbingQtyPerOrder,
			MaxQuantity:      cfg.JobbingMaxQty,
			Tokens:           cfg.JobbingTokens,
		}
		jobbingEngine = jobbing.NewEngine(jobbingCfg, riskClient, rabbitPub, tradeSignalPub, allocationPub, logger)

		// Load per-user, per-token jobbing configs dynamically from user-config
		// service. This allows one user to have multiple jobbing configs across
		// different symbols, with parameters stored in the strategies tables.
		if userConfigClient != nil {
			loadJobbingConfigsForUsers(context.Background(), logger, userConfigClient, jobbingEngine, cfg.JobbingUserIDs)
		} else {
			logger.Warn("User-config client not available, jobbing engine will use static env-based configuration only")
		}

		jobbingConsumer, err = consumer.NewJobbingConsumer(
			cfg.Kafka.Brokers,
			cfg.JobbingTopic,
			"", // use default group
			jobbingEngine,
			logger,
		)
		if err != nil {
			logger.Error("Failed to initialize jobbing consumer", zap.Error(err))
		} else {
			defer jobbingConsumer.Close()
			logger.Info("Jobbing consumer initialized successfully")
		}
	} else {
		logger.Info("Jobbing strategy engine disabled (no JOBBING_USER_IDS configured)")
	}

	// TODO: In a future enhancement we can also subscribe directly to the
	// jobbing.configs Kafka topic (emitted by user-config service) to
	// dynamically refresh jobbingEngine user/token configs in near real-time.
	// For now, configs are loaded once at startup via gRPC calls above.

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

	// Initialize event handler.
	// For now we disable market-hours enforcement so that orders can be
	// generated at any time during development/testing. Once the behaviour
	// is validated end-to-end, this can be switched back to
	// cfg.MarketHours.EnforceHours.
	handler := consumer.NewHandler(matcherEngine, rabbitPub, tradeSignalPub, signalRepo, riskClient, redisCache, strategyCache, stats, logger, marketHours, false)

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

	// Start strategy syncer (loads from Kafka user-configs topic)
	go func() {
		logger.Info("Starting strategy syncer from Kafka user-configs topic...")
		if err := strategySyncer.Start(ctx); err != nil {
			logger.Error("Strategy syncer error", zap.Error(err))
		}
	}()

	// Start consuming 52-week breakout events if engine is enabled
	if breakoutConsumer != nil && cash52wEngine != nil {
		go func() {
			logger.Info("Starting 52w-breakout consumer",
				zap.String("topic", cfg.Cash52WTopic))
			if err := breakoutConsumer.Start(ctx); err != nil {
				logger.Error("52w-breakout consumer error", zap.Error(err))
			}
		}()
	}

	// Start consuming jobbing market depth events if engine is enabled
	if jobbingConsumer != nil && jobbingEngine != nil {
		go func() {
			logger.Info("Starting jobbing consumer",
				zap.String("topic", cfg.JobbingTopic),
				zap.Strings("tokens", cfg.JobbingTokens))
			if err := jobbingConsumer.Start(ctx); err != nil {
				logger.Error("Jobbing consumer error", zap.Error(err))
			}
		}()
	}

	// Start consuming market events from Kafka
	go func() {
		logger.Info("Starting to consume market events from Kafka",
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
				syncStats := strategySyncer.GetStats()
				logger.Info("Matching statistics",
					zap.Int64("events_processed", stats.TotalEventsProcessed),
					zap.Int64("matches_found", stats.TotalMatchesFound),
					zap.Int64("orders_generated", stats.TotalOrdersGenerated),
					zap.Int64("cache_hits", stats.CacheHits),
					zap.Int64("cache_misses", stats.CacheMisses))
				logger.Info("Strategy sync statistics",
					zap.Int64("strategies_synced", syncStats.TotalProcessed),
					zap.Int64("created", syncStats.Created),
					zap.Int64("updated", syncStats.Updated),
					zap.Int64("deleted", syncStats.Deleted),
					zap.Int64("sync_errors", syncStats.Errors))

				// Log jobbing statistics if engine is active
				if jobbingEngine != nil {
					jobbingStats := jobbingEngine.GetStats()
					logger.Info("Jobbing strategy statistics",
						zap.Any("stats", jobbingStats))
				}
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
	logger.Info("Received shutdown signal, starting graceful shutdown...")

	// Cancel context to stop all goroutines
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
