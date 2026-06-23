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
	"encoding/json"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/holiday"
	intkafka "github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/kafka"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/startup"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/utils"

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

	// Initialize trading holiday checker (MongoDB)
	logger.Info("Initializing trading holiday checker...")
	var holidayChecker *holiday.Checker
	holidayChecker, err = holiday.New(ctx, holiday.Config{
		MongoURI: cfg.MongoDB.URI,
		Timezone: cfg.MarketHours.Timezone,
	}, logger)
	if err != nil {
		logger.Warn("Failed to initialize holiday checker - orders will be placed on holidays too",
			zap.Error(err))
		holidayChecker = nil
	} else {
		holidayChecker.StartAutoRefresh(ctx)
		defer holidayChecker.Close(context.Background())
		logger.Info("Trading holiday checker initialized successfully")
	}

	// Initialize event handler
	eng := engine.New(store, engine.Config{Workers: cfg.Performance.WorkerCount}, logger)
	eng.Start(ctx)
	// RabbitMQ is intentionally not initialized (Kafka-only publishing).
	handler := consumer.NewHandler(eng, nil, kafkaPub, signalRepo, riskClient, redisCache, stats, logger, marketHours, cfg.MarketHours.EnforceHours, holidayChecker)

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

	// Wire MANTHAN catch-up: when new strategy created, replay today's signals
	// (set after manthanConsumer is created below)

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

	// --- MANTHAN MODULE ---
	// PostgreSQL connection for manthan portfolio tracking
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

	// MANTHAN_DECISION_LOG_ENABLED gates the new manthan_signal_decisions write
	// path. When true, entry orders go to the decisions table (status=PROPOSED)
	// instead of optimistically inserting into manthan_positions. manthan_positions
	// only fills in when broker confirms via FillConsumer. Default false so the
	// existing optimistic path stays in effect until we cut over end-to-end.
	decisionLogEnabled := os.Getenv("MANTHAN_DECISION_LOG_ENABLED") == "true"
	manthanPub := manthan.NewManthanPublisher(manthan.ManthanPublisherConfig{
		DB:                 manthanDB,
		Redis:              redisCache.Client(),
		KafkaBrokers:       cfg.Kafka.Brokers,
		DecisionLogEnabled: decisionLogEnabled,
	}, logger)
	defer manthanPub.Close()

	// Separate DB connection for `manthan_signals` — lives in `market_data` DB,
	// written by data-ingestion. Rules-engine treats it as the authoritative
	// source of truth for eligible signals; Redis is a rebuildable cache.
	var signalsDB *sql.DB
	if cfg.PostgreSQL.Host != "" {
		signalsDBName := os.Getenv("MANTHAN_SIGNALS_DB")
		if signalsDBName == "" {
			signalsDBName = "market_data"
		}
		signalsConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			cfg.PostgreSQL.Host, cfg.PostgreSQL.Port, cfg.PostgreSQL.User,
			cfg.PostgreSQL.Password, signalsDBName, cfg.PostgreSQL.SSLMode)
		signalsDB, err = sql.Open("postgres", signalsConnStr)
		if err != nil {
			logger.Warn("Manthan signals DB open failed — warm-up/fallback disabled",
				zap.String("db", signalsDBName), zap.Error(err))
			signalsDB = nil
		} else {
			signalsDB.SetMaxOpenConns(5)
			signalsDB.SetMaxIdleConns(2)
			if err := signalsDB.Ping(); err != nil {
				logger.Warn("Manthan signals DB ping failed — warm-up/fallback disabled",
					zap.String("db", signalsDBName), zap.Error(err))
				signalsDB = nil
			} else {
				defer signalsDB.Close()
				logger.Info("Manthan signals DB connected", zap.String("db", signalsDBName))
			}
		}
	}

	manthanPortfolioMgr := manthan.NewPortfolioManager(logger)
	manthanAllocator := manthan.NewAllocator(logger)
	// Wire trading_db so the allocator can consult manthan_signal_decisions
	// for active user-override cooldowns (set on MANUAL_EXIT_DETECTED).
	// Nil-safe — if manthanDB failed to connect above, allocator simply
	// skips the override check.
	manthanAllocator.SetDB(manthanDB)
	manthanOrderGen := manthan.NewOrderGenerator(logger)
	manthanSLMgr := manthan.NewTrailingSLManager(logger)

	// Helper: get active MANTHAN strategies from ConfigStore
	getManthanStrategies := func() []manthan.UserStrategy {
		snap := store.GetSnapshot()
		var out []manthan.UserStrategy
		for _, uv := range snap.ByUser {
			for _, st := range uv.Active {
				if st.StrategyType != "MANTHAN" {
					continue
				}
				out = append(out, manthan.UserStrategy{
					StrategyID:    st.StrategyID,
					UserID:        st.UserID,
					StrategyName:  st.StrategyName,
					TradingMode:   st.TradingMode,
					TotalCapital:  st.TradeConfig.TotalCapital,
					MaxPositions:  st.TradeConfig.MaxPositions,
					PerStockBase:  st.TradeConfig.PerStockAmount,
					StopLossPct:   st.TradeConfig.StopLossPct,
					TrailingSLPct: st.TradeConfig.TrailingSLPct,
					BearerToken:   st.BearerToken,
					AppId:         st.AppId,
					Source:        st.Source,
				})
			}
		}
		return out
	}

	// Helper: get strategy by ID for tick handler
	getManthanStrategy := func(strategyID string) *manthan.UserStrategy {
		for _, st := range getManthanStrategies() {
			if st.StrategyID == strategyID {
				return &st
			}
		}
		return nil
	}

	// Helper: get EMA allocations from local Redis (published by manthan-live pipeline)
	getEMAAllocations := func() map[string]float64 {
		fallback := map[string]float64{
			"NIFTY50":    0.30,
			"NFTYMCP150": 0.30,
			"NTYSLCP250": 0.30,
		}
		raw, err := redisCache.Client().Get(context.Background(), "manthan:ema:allocations").Result()
		if err != nil {
			logger.Warn("EMA allocations not in Redis — using fallback")
			return fallback
		}
		var ema map[string]float64
		if err := json.Unmarshal([]byte(raw), &ema); err != nil {
			logger.Warn("Failed to parse EMA allocations from Redis", zap.Error(err))
			return fallback
		}
		logger.Debug("EMA allocations loaded from Redis", zap.Any("ema", ema))
		return ema
	}

	// LTPFeed placeholder — set after TickHandler is created
	var manthanLTPFeed *manthan.LTPFeed

	manthanConsumer := manthan.NewConsumer(
		manthan.ConsumerConfig{
			KafkaBrokers: cfg.Kafka.Brokers,
			Topic:        "manthan.signals",
			GroupID:      "rule-engine-manthan",
		},
		redisCache.Client(),
		signalsDB,
		nil, // ltpFeed set below after TickHandler init
		manthanAllocator,
		manthanOrderGen,
		manthanPortfolioMgr,
		manthanSLMgr,
		manthanPub,
		getManthanStrategies,
		getEMAAllocations,
		logger,
	)

	manthanTickHandler := manthan.NewTickHandler(
		manthanSLMgr,
		manthanPortfolioMgr,
		manthanOrderGen,
		manthanPub,
		getManthanStrategy,
		logger,
	)
	// Wire external Redis LTP feed (Indira's production data)
	extRedisAddr := os.Getenv("EXT_REDIS_ADDR")
	extRedisPass := os.Getenv("EXT_REDIS_PASSWORD")
	if extRedisAddr != "" {
		var ltpErr error
		manthanLTPFeed, ltpErr = manthan.NewLTPFeed(manthan.LTPFeedConfig{
			Addr:         extRedisAddr,
			Password:     extRedisPass,
			PollInterval: 1 * time.Second,
		}, manthanTickHandler, manthanPortfolioMgr, logger)
		if ltpErr != nil {
			logger.Warn("External Redis LTP feed not available", zap.Error(ltpErr))
		} else {
			defer manthanLTPFeed.Close()
			manthanConsumer.SetLTPFeed(manthanLTPFeed)
		}
	} else {
		logger.Warn("EXT_REDIS_ADDR not set — live LTP feed disabled, using sheet prices")
	}

	lc := StartLive(ctx, eng, configConsumer, newsConsumer, configReader, logger)
	<-lc.ConfigConsumerStarted
	logger.Info("Config consumer started")
	<-lc.NewsConsumerStarted
	logger.Info("News consumer started — system is LIVE")

	// Warm Redis cache from the authoritative `manthan_signals` DB before any
	// catch-up callback can fire — so new strategies always see today's signals
	// even if Redis was flushed or offsets were committed by a prior run.
	manthanConsumer.WarmUpCache(ctx)

	// Rehydrate ACTIVE positions from Postgres (source of truth) into in-memory
	// portfolio and Redis cache. Without this, a service restart loses every
	// live position and the LTP feed has nothing to trail — silent TSL failure.
	// Orphan positions (strategy soft-deleted) get marked EXITED + Redis evicted.
	strategyResolver := func(sid string) *manthan.UserStrategy {
		for _, s := range getManthanStrategies() {
			if s.StrategyID == sid {
				s := s
				return &s
			}
		}
		return nil
	}
	// ucClient (user-config gRPC) doubles as the StrategyAliveChecker —
	// it implements IsStrategyAlive(ctx, userID, strategyID). Replaces the
	// legacy direct read of strategies.deleted_at; user-config is now the
	// single owner of that table (see docs/architecture/data-ownership.md).
	if restored, orphans, stale, err := manthanPortfolioMgr.RehydrateActivePositions(
		ctx, manthanDB, redisCache.Client(), ucClient, strategyResolver,
	); err != nil {
		logger.Warn("Manthan rehydration failed", zap.Error(err))
	} else {
		logger.Info("Manthan positions rehydrated from DB",
			zap.Int("restored", restored),
			zap.Int("orphans_cleaned", orphans),
			zap.Int("redis_stale_evicted", stale))
	}

	// Periodic orphan scanner — cleans up ACTIVE positions whose strategy
	// was soft-deleted while the position was live. Does NOT auto-sell or
	// cancel broker SL — data-integrity only. Complements the startup-time
	// rehydrate cleanup by running continuously (every 60s).
	go manthanPortfolioMgr.StartOrphanScanner(ctx, manthanDB, redisCache.Client(),
		ucClient, strategyResolver, 60*time.Second)

	// Wire MANTHAN catch-up callback
	configConsumer.SetOnManthanCreated(func(ctx context.Context, strategy *models.Strategy) {
		us := manthan.UserStrategy{
			StrategyID:    strategy.StrategyID,
			UserID:        strategy.UserID,
			StrategyName:  strategy.StrategyName,
			TradingMode:   strategy.TradingMode,
			TotalCapital:  strategy.TradeConfig.TotalCapital,
			MaxPositions:  strategy.TradeConfig.MaxPositions,
			PerStockBase:  strategy.TradeConfig.PerStockAmount,
			StopLossPct:   strategy.TradeConfig.StopLossPct,
			TrailingSLPct: strategy.TradeConfig.TrailingSLPct,
			BearerToken:   strategy.BearerToken,
			AppId:         strategy.AppId,
			Source:        strategy.Source,
		}
		manthanConsumer.CatchUpNewStrategy(ctx, us)
	})

	// Start Manthan signal consumer
	go func() {
		manthanConsumer.Start(ctx)
	}()
	logger.Info("Manthan signal consumer started")

	// Start Manthan fill consumer (syncs real fill prices from trade-execution).
	// In CQRS mode (MANTHAN_DECISION_LOG_ENABLED=true) the projector becomes
	// the sole writer to manthan_positions / manthan_position_events with
	// atomic idempotent transactions; legacy mode keeps the optimistic write.
	manthanProjector := manthan.NewPositionProjector(manthanDB, logger)

	// User-facing notification publisher — emits to manthan.notifications
	// when the projector sees MANUAL_* events. Off by default; enable with
	// MANTHAN_NOTIFICATIONS_ENABLED=true.
	notificationsEnabled := os.Getenv("MANTHAN_NOTIFICATIONS_ENABLED") == "true"
	manthanNotifier := manthan.NewNotificationPublisher(cfg.Kafka.Brokers, notificationsEnabled, logger)
	defer manthanNotifier.Close()
	manthanProjector.SetNotifier(manthanNotifier)
	if notificationsEnabled {
		logger.Info("Manthan notifications ENABLED — publishing to manthan.notifications topic")
	}
	manthanFillConsumer := manthan.NewFillConsumer(
		manthan.FillConsumerConfig{
			KafkaBrokers:       cfg.Kafka.Brokers,
			Topic:              "manthan.execution.events",
			DecisionLogEnabled: decisionLogEnabled,
		},
		manthanPortfolioMgr, manthanSLMgr, manthanPub, manthanProjector, 20.0, logger,
	)
	go func() {
		manthanFillConsumer.Start(ctx)
	}()
	logger.Info("Manthan fill consumer started — reality sync active")

	// Stale-decision reaper — flips manthan_signal_decisions rows that have
	// been PROPOSED/DISPATCHED for >2 minutes to TIMED_OUT. Catches the
	// pathological case where trade-execution crashed mid-flight or the
	// broker silently dropped our request without a REJECTED event. Only
	// active in CQRS mode.
	if decisionLogEnabled {
		go func() {
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					n, err := manthanProjector.MarkStaleDecisionsTimedOut(ctx, 120)
					if err != nil {
						logger.Warn("Reaper: stale decision sweep failed", zap.Error(err))
						continue
					}
					if n > 0 {
						logger.Info("Reaper: flipped stale decisions to TIMED_OUT",
							zap.Int64("count", n))
					}
				}
			}
		}()
		logger.Info("Stale-decision reaper started (60s tick, 2min cutoff)")
	}

	// Start LTP feed poller for real-time trailing SL
	if manthanLTPFeed != nil {
		go func() {
			manthanLTPFeed.Start(ctx)
		}()
		logger.Info("Manthan LTP feed started — trailing SL active")
	}

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
