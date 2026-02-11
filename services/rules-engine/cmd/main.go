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
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/jobbing"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
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

	logger.Info("Starting Rules Engine Service - Jobbing Strategy",
		zap.String("version", cfg.ServiceVersion),
		zap.String("environment", cfg.Environment),
		zap.Int("grpc_port", cfg.GRPCPort))

	// Initialize risk management client (DISABLED for development/paper trading)
	logger.Warn("Risk management client disabled - orders will be auto-approved for paper trading")
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

	// Initialize Kafka publisher for portfolio allocation state events (placeholder for now)
	logger.Info("Initializing Kafka publisher for portfolio allocations...",
		zap.String("topic", cfg.PortfolioAllocTopic))
	allocationPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		cfg.PortfolioAllocTopic,
		logger,
	)
	defer allocationPub.Close()
	logger.Info("Kafka portfolio allocations publisher initialized successfully")

	// Initialize user-config client for fetching jobbing configurations
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
		logger.Warn("Failed to initialize user-config client - will use static env-based config only",
			zap.Error(err))
		userConfigClient = nil
	} else {
		defer userConfigClient.Close()
		logger.Info("User-config client initialized successfully")
	}

	// Initialize market hours configuration
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

	// Initialize Jobbing strategy engine
	var jobbingEngine *jobbing.Engine
	var jobbingConsumer *consumer.JobbingConsumer
	var jobbingConfigConsumer *consumer.JobbingConfigConsumer

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

		// Dynamic jobbing configs will be loaded via JobbingConfigConsumer from Kafka
		// (user-configs.jobbing topic). Static fallback is env-based config above.
		logger.Info("Jobbing engine initialized - will receive configs via Kafka consumer")

		// Initialize market data consumer
		jobbingConsumer, err = consumer.NewJobbingConsumer(
			cfg.Kafka.Brokers,
			cfg.JobbingTopic,
			"rules-engine-jobbing-group-v1",
			jobbingEngine,
			logger,
		)
		if err != nil {
			logger.Error("Failed to initialize jobbing consumer", zap.Error(err))
		} else {
			defer jobbingConsumer.Close()
			logger.Info("Jobbing consumer initialized successfully")
		}

		// Initialize config consumer
		jobbingConfigConsumer, err = consumer.NewJobbingConfigConsumer(
			cfg.Kafka.Brokers,
			"jobbing.configs",
			"rules-engine-jobbing-config-v1",
			jobbingEngine,
			logger,
		)
		if err != nil {
			logger.Error("Failed to initialize jobbing config consumer", zap.Error(err))
		} else {
			defer jobbingConfigConsumer.Close()
			logger.Info("Jobbing config consumer initialized successfully")
		}

		// Load existing jobbing configurations from user-config service at startup
		if userConfigClient != nil {
			loadJobbingConfigsForUsers(context.Background(), logger, userConfigClient, jobbingEngine, cfg.JobbingUserIDs)
			logger.Info("Initial jobbing configs loaded from user-config service")
		}
	} else {
		logger.Warn("Jobbing strategy engine disabled - configure JOBBING_USER_IDS and JOBBING_TOPIC to enable")
	}

	// Initialize trade signal repository and execution consumer
	var executionConsumer *consumer.ExecutionConsumer
	logger.Info("Initializing trade signal repository and execution consumer...")
	tradeSignalRepo, err := repository.NewTradeSignalRepository(
		cfg.PostgreSQL.Host,
		cfg.PostgreSQL.Port,
		cfg.PostgreSQL.User,
		cfg.PostgreSQL.Password,
		cfg.PostgreSQL.Database,
		cfg.PostgreSQL.SSLMode,
		logger,
	)
	if err != nil {
		logger.Error("Failed to initialize trade signal repository", zap.Error(err))
	} else {
		defer tradeSignalRepo.Close()
		logger.Info("Trade signal repository initialized successfully")

		// Initialize execution consumer
		executionConsumer = consumer.NewExecutionConsumer(
			cfg.Kafka.Brokers,
			"rules-engine-execution-group-v1",
			tradeSignalRepo,
			jobbingEngine, // Pass jobbing engine as order fill handler
			logger,
		)
		defer executionConsumer.Close()
		logger.Info("Execution consumer initialized successfully")
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming jobbing market depth events
	if jobbingConsumer != nil && jobbingEngine != nil {
		go func() {
			logger.Info("Starting jobbing market data consumer",
				zap.String("topic", cfg.JobbingTopic),
				zap.Strings("tokens", cfg.JobbingTokens))
			if err := jobbingConsumer.Start(ctx); err != nil {
				logger.Error("Jobbing consumer error", zap.Error(err))
			}
		}()
	}

	// Start consuming jobbing config events
	if jobbingConfigConsumer != nil {
		go func() {
			logger.Info("Starting Jobbing config consumer",
				zap.String("topic", "user-configs.jobbing"))
			if err := jobbingConfigConsumer.Start(ctx); err != nil {
				logger.Error("Jobbing config consumer error", zap.Error(err))
			}
		}()
	}

	// Start consuming execution results
	if executionConsumer != nil {
		go func() {
			logger.Info("Starting execution consumer",
				zap.String("topic", "trade-executions"))
			if err := executionConsumer.Start(ctx); err != nil {
				logger.Error("Execution consumer error", zap.Error(err))
			}
		}()
	}

	// Log periodic statistics
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
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

	logger.Info("Rules Engine Service started successfully - Jobbing Strategy Active")

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

	<-shutdownCtx.Done()

	logger.Info("Rules Engine Service shutdown complete")
}
