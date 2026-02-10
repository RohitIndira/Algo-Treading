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
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"

	"go.uber.org/zap"
)

// ============================================================================
// PRODUCTION-READY RULES ENGINE - 52-WEEK STRATEGY ONLY
// ============================================================================
//
// This is a SIMPLIFIED, PRODUCTION-READY version of the rules-engine service
// that ONLY handles the Cash 52-Week High breakout strategy.
//
// REMOVED COMPONENTS (compared to original main.go):
// ✗ News/generic matching (matcher, elasticsearch, strategy syncer)
// ✗ Jobbing strategy engine
// ✗ Market hours enforcement
// ✗ Generic event handler
//
// ACTIVE COMPONENTS:
// ✓ Cash 52W Engine (core strategy logic)
// ✓ 52W Breakout Consumer (market:52w-breakouts topic)
// ✓ 52W Config Consumer (user-configs.cash52w topic)
// ✓ Config Store (in-memory Phase 1 configs)
// ✓ Redis cache (for live market data)
// ✓ PostgreSQL (trade signal tracking)
// ✓ RabbitMQ publisher (order execution)
// ✓ Kafka publishers (signals, allocations, portfolios)
// ✓ Risk management (optional, currently disabled)
//
// ============================================================================

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Note: .env file not found, using system environment variables\n")
	}

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load configuration: %v", err))
	}

	// Initialize production logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	logger.Info("=============================================================")
	logger.Info("Starting Rules Engine Service - 52W STRATEGY ONLY")
	logger.Info("=============================================================",
		zap.String("version", cfg.ServiceVersion),
		zap.String("environment", cfg.Environment))

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ========================================================================
	// STEP 1: Initialize PostgreSQL Repository (Trade Signal Tracking)
	// ========================================================================
	logger.Info("[1/8] Initializing PostgreSQL trade signal repository...")
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
		logger.Warn("PostgreSQL unavailable - continuing without DB tracking",
			zap.Error(err))
		signalRepo = nil
	} else {
		defer signalRepo.Close()
		logger.Info("✓ PostgreSQL repository initialized")
	}

	// ========================================================================
	// STEP 2: Initialize Redis Cache (Live Market Data)
	// ========================================================================
	logger.Info("[2/8] Initializing Redis cache...")
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
		logger.Fatal("Failed to initialize Redis cache (REQUIRED)", zap.Error(err))
	}
	defer redisCache.Close()
	logger.Info("✓ Redis cache initialized")

	// ========================================================================
	// STEP 3: Initialize Risk Management Client (OPTIONAL - Currently Disabled)
	// ========================================================================
	logger.Info("[3/8] Initializing risk management...")
	var riskClient *risk.Client = nil
	logger.Warn("✓ Risk management DISABLED - orders will be auto-approved")

	// ========================================================================
	// STEP 4: Initialize Publishers (RabbitMQ + Kafka)
	// ========================================================================
	logger.Info("[4/8] Initializing publishers...")

	// RabbitMQ for order execution
	rabbitPub, err := publisher.NewPublisher(&cfg.RabbitMQ, logger)
	if err != nil {
		logger.Fatal("Failed to initialize RabbitMQ publisher (REQUIRED)", zap.Error(err))
	}
	defer rabbitPub.Close()

	// Kafka publisher for trade signals
	tradeSignalPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		"trade-signals",
		logger,
	)
	defer tradeSignalPub.Close()

	// Kafka publisher for portfolio allocations
	allocationPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		cfg.PortfolioAllocTopic,
		logger,
	)
	defer allocationPub.Close()

	// Kafka publisher for realtime portfolios
	realtimePortfolioPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		cfg.PortfolioRealtimeTopic,
		logger,
	)
	defer realtimePortfolioPub.Close()

	// Kafka publisher for position state tracking
	positionStatesPub := publisher.NewKafkaPublisher(
		cfg.Kafka.Brokers,
		"position-states",
		logger,
	)
	defer positionStatesPub.Close()

	logger.Info("✓ All publishers initialized")

	// ========================================================================
	// STEP 5: Initialize Cash 52W Engine with Position Tracker
	// ========================================================================
	logger.Info("[5/8] Initializing Cash 52-Week High strategy engine...")

	if cfg.Cash52WTopic == "" {
		logger.Fatal("CASH52W_TOPIC not configured - cannot start 52W engine")
	}

	// Create config store (in-memory Phase 1 enhanced configs)
	cash52wConfigStore := cash52w.NewConfigStore()

	// Create position tracker with Kafka publisher
	positionTracker := cash52w.NewPositionTracker(logger, positionStatesPub)

	// Create engine with default config (will be overridden by config store)
	engineCfg := cash52w.Config{
		UserIDs:         []string{}, // Populated dynamically
		CapitalPerStock: 20000,      // Default (overridden by user configs)
		MaxPositions:    25,          // Default
		SLPercent:       10,          // Default
		TSLPercent:      20,          // Default
	}

	cash52wEngine := cash52w.NewEngine(
		engineCfg,
		cash52wConfigStore,
		riskClient,
		rabbitPub,
		tradeSignalPub,
		allocationPub,
		positionTracker,
		logger,
	)

	// Set backfill callback for when users enable strategy
	cash52wConfigStore.SetOnEnable(func(userID string, enabledSince time.Time) {
		logger.Info("User enabled 52W strategy - triggering backfill",
			zap.String("user_id", userID))
		time.Sleep(500 * time.Millisecond) // Allow config to propagate
		_ = cash52w.BackfillFromBreakouts(
			context.Background(),
			logger,
			cfg.Kafka.Brokers,
			cfg.Cash52WTopic,
			cash52wEngine,
			userID,
			15*time.Second,
		)
	})

	logger.Info("✓ Cash 52W engine initialized")

	// ========================================================================
	// STEP 6: Initialize 52W Config Consumer (user-configs.cash52w)
	// ========================================================================
	logger.Info("[6/8] Initializing Cash52W config consumer...")

	cash52wConfigConsumer, err := consumer.NewCash52WConfigConsumer(
		cfg.Kafka.Brokers,
		"user-configs.cash52w",
		"", // Use default group
		cash52wConfigStore,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to initialize Cash52W config consumer (REQUIRED)", zap.Error(err))
	}
	defer cash52wConfigConsumer.Close()

	logger.Info("✓ Cash52W config consumer initialized")

	// ========================================================================
	// STEP 7: Initialize 52W Breakout Consumer (market:52w-breakouts)
	// ========================================================================
	logger.Info("[7/8] Initializing 52W breakout consumer...")

	breakoutConsumer, err := consumer.NewBreakoutConsumer(
		cfg.Kafka.Brokers,
		cfg.Cash52WTopic,
		"", // Use default group
		cash52wEngine,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to initialize 52W breakout consumer (REQUIRED)", zap.Error(err))
	}
	defer breakoutConsumer.Close()

	logger.Info("✓ 52W breakout consumer initialized")

	// ========================================================================
	// STEP 8: Start All Background Services
	// ========================================================================
	logger.Info("[8/8] Starting background services...")

	// Start Cash52W config consumer
	go func() {
		logger.Info("→ Starting Cash52W config consumer",
			zap.String("topic", "user-configs.cash52w"))
		if err := cash52wConfigConsumer.Start(ctx); err != nil {
			logger.Error("Cash52W config consumer error", zap.Error(err))
		}
	}()

	// Start dynamic user/mode refresh loop
	go func() {
		refreshInterval := 15 * time.Second
		logger.Info("→ Starting 52W user refresh loop",
			zap.Duration("interval", refreshInterval))

		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("Stopping 52W user refresh loop")
				return
			case <-ticker.C:
				users, modes := cash52wConfigStore.Snapshot()
				cash52wEngine.SetUsers(users)
				cash52wEngine.SetUserModes(modes)

				if len(users) > 0 {
					logger.Debug("Refreshed active 52W users",
						zap.Strings("users", users),
						zap.Int("count", len(users)))
				}
			}
		}
	}()

	// Start 52W breakout consumer
	go func() {
		logger.Info("→ Starting 52W breakout consumer",
			zap.String("topic", cfg.Cash52WTopic))
		if err := breakoutConsumer.Start(ctx); err != nil {
			logger.Error("52W breakout consumer error", zap.Error(err))
		}
	}()

	// Start realtime portfolio publisher loop with exit monitoring
	go startRealtimePortfolioLoop(ctx, logger, cash52wEngine, redisCache, realtimePortfolioPub)

	// Start periodic statistics logger
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats := cash52wEngine.GetStats()
				logger.Info("📊 Cash 52W Strategy Statistics",
					zap.Any("stats", stats))
			case <-ctx.Done():
				return
			}
		}
	}()

	logger.Info("=============================================================")
	logger.Info("✅ Rules Engine Service Started Successfully")
	logger.Info("=============================================================")
	logger.Info("Listening for:")
	logger.Info("  • 52W breakouts:       market:52w-breakouts")
	logger.Info("  • User configurations: user-configs.cash52w")
	logger.Info("Publishing to:")
	logger.Info("  • Trade signals:       trade-signals")
	logger.Info("  • Allocations:         " + cfg.PortfolioAllocTopic)
	logger.Info("  • Realtime portfolios: " + cfg.PortfolioRealtimeTopic)
	logger.Info("  • Orders:              RabbitMQ (odin-api-wrapper)")
	logger.Info("=============================================================")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	logger.Info("🛑 Received shutdown signal, starting graceful shutdown...")

	// Cancel context to stop all goroutines
	cancel()

	// Give time for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	<-shutdownCtx.Done()

	// Log final statistics
	finalStats := cash52wEngine.GetStats()
	logger.Info("📊 Final Statistics", zap.Any("stats", finalStats))

	logger.Info("✅ Rules Engine Service shutdown complete")
}

// startRealtimePortfolioLoop periodically builds realtime marked-to-market
// portfolios for all active 52W users, evaluates exit signals (multi-level
// SL/profit), and publishes everything to Kafka.
func startRealtimePortfolioLoop(
	ctx context.Context,
	logger *zap.Logger,
	engine *cash52w.Engine,
	redisCache *cache.RedisCache,
	pub *publisher.KafkaPublisher,
) {
	interval := 5 * time.Second
	logger.Info("→ Starting realtime 52W portfolio publisher + exit monitor",
		zap.Duration("interval", interval))

	// Create exit manager for Phase 1 multi-level SL/profit
	exitManager := cash52w.NewExitManager(engine, logger)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping realtime 52W portfolio publisher + exit monitor")
			return
		case <-ticker.C:
			// Build realtime portfolios for all users with positions
			events := engine.BuildRealtimePortfolios(ctx, redisCache)
			if len(events) == 0 {
				continue
			}

			// Publish each portfolio event to Kafka
			for _, ev := range events {
				if err := pub.PublishRealtimePortfolio(ctx, ev); err != nil {
					logger.Error("Failed to publish realtime portfolio",
						zap.Error(err),
						zap.String("user_id", ev.UserID))
				}
			}

			// ================================================================
			// PHASE 1: Multi-Level Exit Evaluation
			// ================================================================
			// Evaluate all positions against Phase 1 exit levels:
			// - Profit L1: +15% → Exit 33%
			// - Profit L2: +30% → Exit 50%
			// - Profit L3: +50% → Exit 100% (trailing)
			// - SL L1: -10% → Exit 50%
			// - SL L2: -20% → Exit 100%
			// - Force exits (manual controls)
			exitSignals := exitManager.EvaluateExits(ctx, events)
			
			if len(exitSignals) > 0 {
				logger.Info("Exit signals generated",
					zap.Int("signal_count", len(exitSignals)))
				
				// Execute exit signals (SELL orders)
				// This publishes to both Kafka (trade-signals) and RabbitMQ
				if err := exitManager.ExecuteExitSignals(ctx, exitSignals); err != nil {
					logger.Error("Failed to execute exit signals",
						zap.Error(err))
				}
			}
		}
	}
}
