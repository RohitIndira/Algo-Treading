package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	indiraPkg "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/lifecycle"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/paper"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/scheduler"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/server"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/statusservice"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	log.Println("========================================")
	log.Println("Starting Trade Execution Service...")
	log.Println("========================================")

	// Load configuration
	cfg := loadConfig()

	// Initialize PostgreSQL
	log.Println("Connecting to PostgreSQL...")
	db, err := initPostgres(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Println("✓ Connected to PostgreSQL")

	// Initialize repositories
	orderRepo := repository.NewOrderRepository(db)
	credsRepo := repository.NewCredentialsRepository(db, cfg.EncryptionKey)
	log.Println("✓ Repository layer initialized")

	// Initialize Indira client (stateless, supports multiple users)
	indiraClient := indira.NewExecutionClient()
	log.Println("✓ Indira API client initialized")

	// Initialize Kafka publisher for trade-executions and order-updates topics
	log.Println("Initializing Kafka publisher...")
	logger, _ := initLogger()
	kafkaPub := publisher.NewKafkaPublisher(cfg.KafkaBrokers, logger)
	log.Println("✓ Kafka publisher initialized")

	// Initialize Order Status Service (WebSocket-based real-time order updates)
	// The backend opens one WS connection per user to Indira after placing their first order.
	log.Println("Initializing WebSocket Order Status Service...")
	statusService := statusservice.NewOrderStatusService(indiraClient, orderRepo, credsRepo, kafkaPub, logger)
	log.Println("✓ Order Status Service initialized")

	// Initialize executor with credentials repository, Kafka publisher, and status service.
	// The executor owns: retries, WS subscription start, and Kafka order-update publishing.
	orderExecutor := executor.NewOrderExecutor(
		orderRepo,
		credsRepo,
		indiraClient,
		kafkaPub,
		statusService,
		cfg.MaxRetries,
		cfg.RetryDelay,
	)
	log.Println("✓ Order executor initialized")

	// Pre-warm credentials cache with active live-trading users so the first
	// order per user hits memory instead of DB + decrypt.
	go func() {
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer warmCancel()
		userIDs, err := orderRepo.GetDistinctActiveUserIDs(warmCtx)
		if err != nil {
			log.Printf("[credentials] Cache warm-up skipped: %v", err)
			return
		}
		if len(userIDs) > 0 {
			orderExecutor.CredentialsCache().Warm(warmCtx, userIDs)
			log.Printf("✓ Credentials cache warmed for %d active users", len(userIDs))
		}
	}()

	// Initialize Kafka signal consumer (trade-signals topic → SignalProcessor)
	log.Println("Initializing Kafka consumer for trade-signals...")
	signalProcessor := executor.NewSignalProcessor(orderExecutor, orderRepo, kafkaPub, statusService, logger)
	kafkaConsumer := consumer.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, signalProcessor, logger, cfg.WorkerCount)
	log.Println("✓ Kafka consumer initialized")


	// Initialize strategy events consumer (user-config-events → close positions on deactivate/delete)
	log.Println("Initializing Kafka consumer for user-config-events...")
	strategyEventsConsumer := consumer.NewStrategyEventsConsumer(cfg.KafkaBrokers, orderRepo, orderExecutor, logger)
	log.Println("✓ Strategy events consumer initialized")

	// Initialize gRPC server
	grpcServer := server.NewServer(orderRepo, orderExecutor, cfg.GRPCPort)
	log.Println("✓ gRPC server initialized")

	// ── Paper Trading Layer ────────────────────────────────────────────────────
	log.Println("Initializing Paper Trading layer...")
	paperWSServer := paper.NewPaperWSServer(orderRepo)

	// Wire Indira positions fetcher — used by the /ws/live-orders/indira-positions endpoint.
	// Converts pkg/indira.Position → paper.BrokerPosition so the paper package stays decoupled.
	paperWSServer.SetPositionsFetcher(func(ctx context.Context, bearerToken, appId, userId, source string) ([]paper.BrokerPosition, error) {
		auth := &indiraPkg.AuthContext{
			UserId:      userId,
			AppId:       appId,
			Source:      source,
			BearerToken: bearerToken,
		}
		positions, err := indiraClient.GetPositions(ctx, auth)
		if err != nil {
			return nil, err
		}
		// Deduplicate: Indira returns both DAILY and EXPIRY rows per symbol.
		// Keep only DAILY to avoid showing the same position twice.
		seen := make(map[string]bool, len(positions))
		result := make([]paper.BrokerPosition, 0, len(positions))
		for _, p := range positions {
			if p.Type != "DAILY" {
				continue // skip EXPIRY rows — DAILY has the same data
			}
			// Use dispSym for display; fall back to baseSym or full symbol string.
			sym := p.Symbol.DispSym
			if sym == "" {
				sym = p.Symbol.BaseSym
			}
			if sym == "" {
				sym = p.Symbol.Symbol
			}
			key := sym + "|" + p.Symbol.Exc + "|" + p.PrdType
			if seen[key] {
				continue
			}
			seen[key] = true

			// Calculate P&L when broker returns 0.
			pnl := p.NetPnL
			if pnl == 0 && p.BuyQty > 0 && p.SellQty > 0 {
				// Closed position: realized P&L = (sellAvg - buyAvg) * traded qty
				tradedQty := p.SellQty
				if p.BuyQty < tradedQty {
					tradedQty = p.BuyQty
				}
				pnl = (p.SellAvgPrice - p.BuyAvgPrice) * float64(tradedQty)
			} else if pnl == 0 && p.NetQty != 0 && p.LTP > 0 {
				// Open position: unrealized P&L using LTP
				if p.NetQty > 0 {
					pnl = (p.LTP - p.BuyAvgPrice) * float64(p.NetQty)
				} else {
					pnl = (p.SellAvgPrice - p.LTP) * float64(-p.NetQty)
				}
			}
			pnlPct := p.PnLPerc
			if pnlPct == 0 && pnl != 0 && p.BuyAvgPrice > 0 {
				tradedQty := p.BuyQty
				if p.SellQty > 0 && p.SellQty < tradedQty {
					tradedQty = p.SellQty
				}
				pnlPct = (pnl / (p.BuyAvgPrice * float64(tradedQty))) * 100
			}

			result = append(result, paper.BrokerPosition{
				Symbol:        sym,
				Exchange:      p.Symbol.Exc,
				ProductType:   p.PrdType,
				NetQty:        p.NetQty,
				BuyQty:        p.BuyQty,
				SellQty:       p.SellQty,
				BuyAvgPrice:   p.BuyAvgPrice,
				SellAvgPrice:  p.SellAvgPrice,
				CurrentPrice:  p.LTP,
				PnL:           pnl,
				PnLPercentage: pnlPct,
				ExcTkn:        p.Symbol.ExcTkn,
			})
		}
		return result, nil
	})

	// Link OrderExecutor → live orders WS so the frontend gets real-time order events.
	// This is called only for LIVE (non-paper) orders after broker placement.
	orderExecutor.SetWSBroadcaster(func(userID string, eventType string, order *models.Order) {
		paperWSServer.BroadcastLiveOrder(userID, paper.LiveOrderUpdate{
			Type:   eventType,
			UserID: userID,
			Order:  order,
		})
	})

	// Link OrderStatusService → live orders WS so every status change received from
	// the Indira broker WebSocket (SUBMITTED→FILLED, PARTIALLY_FILLED, REJECTED, etc.)
	// is pushed to the frontend immediately without requiring a page refresh.
	statusService.SetWSBroadcaster(func(userID string, order *models.Order) {
		paperWSServer.BroadcastLiveOrder(userID, paper.LiveOrderUpdate{
			Type:   "order_update",
			UserID: userID,
			Order:  order,
		})
	})

	// Wire StatusService.StartSubscription into PaperWSServer so the
	// POST /ws/live-orders/subscribe-broker-ws endpoint can start the Indira
	// WS subscription when a strategy is created/activated — before the first order fires.
	paperWSServer.SetStatusService(func(ctx context.Context, userID, bearerToken, appID, source string) error {
		return statusService.StartSubscription(ctx, userID, &indiraPkg.AuthContext{
			UserId:      userID,
			AppId:       appID,
			Source:      source,
			BearerToken: bearerToken,
		})
	})

	// Initialize Redis price client — used for accurate order fill prices, PnL fallback,
	// and dynamic tick size lookup for limit order price rounding.
	// Non-fatal: if Redis is unavailable the service still runs with hardcoded tick sizes.
	redisPrices, redisErr := paper.NewRedisPriceClient(cfg.RedisAddr, cfg.RedisPassword)
	var priceLookup executor.PriceLookup
	if redisErr != nil {
		log.Printf("[paper] Redis price client unavailable (non-fatal): %v", redisErr)
		log.Println("[paper] Order fills will use signal price; PnL shown only when WSS is live")
		log.Println("[paper] Tick size will use hardcoded NSE defaults (0.05/0.01)")
	} else {
		log.Printf("✓ Redis price client connected (%s)", cfg.RedisAddr)
		priceLookup = redisPrices
		// Wire Redis tick size lookup into the Indira execution client
		indiraClient.SetTickSizeLookup(redisPrices)
		indiraClient.SetDPRLookup(redisPrices)
		log.Println("✓ Dynamic tick size + DPR lookup enabled (Redis market data)")
	}

	paperExec := executor.NewPaperOrderExecutor(orderRepo, kafkaPub, priceLookup)
	orderExecutor.SetPaperExecutor(paperExec)

	var paperMonitorRef *paper.PaperTradeMonitor
	
	paperExec.OnPaperFilled = func(order *models.Order) {
		if paperMonitorRef != nil {
			paperMonitorRef.AddOrder(order)
		}
	}

	paperMarketClient := paper.NewPaperMarketClient(
		cfg.PaperMarketWSURL,
		func(symbol string, ltp float64) {
			if paperMonitorRef != nil {
				paperMonitorRef.OnPriceUpdate(symbol, ltp)
			}
		},
	)
	paperMonitor := paper.NewPaperTradeMonitor(orderRepo, paperExec, paperWSServer, paperMarketClient, redisPrices)
	paperMonitorRef = paperMonitor
	paperWSServer.SetMonitor(paperMonitor)
	log.Println("✓ Paper trading layer initialized")
	// ─────────────────────────────────────────────────────────────────────────

	// Initialize PriceMonitor for below_min orders (Case 2).
	// Monitors Redis LTP and triggers bracket order placement when target price is reached.
	var priceMonitorRef *scheduler.PriceMonitor
	if redisPrices != nil {
		priceMonitorRef = scheduler.NewPriceMonitor(
			redisPrices,   // satisfies scheduler.LTPProvider (GetLTP + GetLTPs)
			orderRepo,
			kafkaPub,
			orderExecutor, // satisfies scheduler.OrderExecutorFunc interface
			100*time.Millisecond, // poll interval (reduced from 500ms for lower latency)
		)
		signalProcessor.SetPriceMonitor(priceMonitorRef)
		strategyEventsConsumer.SetPriceMonitor(priceMonitorRef)
		paperWSServer.SetPriceMonitor(priceMonitorRef)
		priceMonitorRef.SetOnTickDone(paperWSServer.BroadcastPriceWatches)
		log.Println("✓ Price Monitor initialized (100ms poll interval)")
	} else {
		log.Println("⚠ Price Monitor disabled (Redis not available) — below_min orders will be placed immediately")
	}

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Lifecycle: ordered graceful shutdown ──────────────────────────────────
	lc := lifecycle.New(cancel, 15*time.Second)

	// Register components in shutdown order:
	// 1. Stop accepting new signals (Kafka consumers)
	lc.Register(lifecycle.Component{
		Name:    "Kafka consumer (trade-signals)",
		StopFn:  kafkaConsumer.Stop,
		CloseFn: kafkaConsumer.Close,
	})
	lc.RegisterCloseable("Kafka consumer (user-config-events)", strategyEventsConsumer)
	// 2. gRPC server (drain RPCs)
	lc.RegisterStoppable("gRPC server", grpcServer)
	// 3. Kafka publisher (flush writes)
	lc.RegisterCloseable("Kafka publisher", kafkaPub)
	// 4. PostgreSQL (release connections)
	lc.Register(lifecycle.Component{
		Name:    "PostgreSQL",
		CloseFn: db.Close,
	})

	// ── Prometheus metrics HTTP server ───────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: metricsMux,
	}
	go func() {
		log.Printf("Starting Prometheus metrics server on :%d/metrics", cfg.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()
	lc.Register(lifecycle.Component{
		Name: "Metrics HTTP server",
		StopFn: func() {
			shutCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
			defer c()
			metricsServer.Shutdown(shutCtx)
		},
	})

	// Start market data WSS client (paper trading price feed)
	go func() {
		log.Println("Starting paper market WSS client...")
		paperMarketClient.Start(ctx)
	}()

	// Load active paper orders and subscribe symbols
	go func() {
		time.Sleep(2 * time.Second) // wait for WSS to connect
		if err := paperMonitor.Initialize(ctx); err != nil {
			log.Printf("[paper] Monitor init error (non-fatal): %v", err)
		}
	}()

	// Start paper trading WebSocket server for frontend
	go func() {
		paperWSAddr := fmt.Sprintf(":%d", cfg.PaperWSPort)
		log.Printf("Starting paper trading WS server on %s", paperWSAddr)
		if err := paperWSServer.StartHTTPServer(ctx, paperWSAddr); err != nil {
			log.Printf("Paper WS server stopped: %v", err)
		}
	}()

	// Start PriceMonitor for below_min orders
	if priceMonitorRef != nil {
		go func() {
			log.Println("Starting Price Monitor...")
			if err := priceMonitorRef.Start(ctx); err != nil {
				log.Printf("Price Monitor error: %v", err)
			}
		}()
	}

	// Start Kafka consumer — primary intake path (rules-engine trade-signals)
	go func() {
		log.Println("Starting Kafka consumer (trade-signals)...")
		if err := kafkaConsumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	// Start strategy events consumer — closes positions on strategy deactivate/delete
	go func() {
		log.Println("Starting Kafka consumer (user-config-events)...")
		if err := strategyEventsConsumer.Start(ctx); err != nil {
			log.Printf("Strategy events consumer error: %v", err)
		}
	}()

	// Give consumer time to start
	time.Sleep(1 * time.Second)

	// Start gRPC server
	go func() {
		log.Println("Starting gRPC server...")
		if err := grpcServer.Start(); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	log.Println("========================================")
	log.Println("✓ Trade Execution Service Started")
	log.Printf("  - gRPC Server: localhost:%d", cfg.GRPCPort)
	log.Printf("  - Metrics:     localhost:%d/metrics", cfg.MetricsPort)
	log.Printf("  - Kafka Topic: %s (Group: %s)", cfg.KafkaTopic, cfg.KafkaGroupID)
	log.Printf("  - Workers: %d (bounded pool)", cfg.WorkerCount)
	log.Println("========================================")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	log.Printf("\nReceived signal: %v", sig)

	// Ordered graceful shutdown via lifecycle manager
	lc.Shutdown()

	log.Println("========================================")
	log.Println("Trade Execution Service stopped")
	log.Println("========================================")
}

// Config holds service configuration
type Config struct {
	GRPCPort         int
	RabbitMQURL      string
	QueueName        string
	Exchange         string
	RoutingKey       string
	PrefetchCount    int
	WorkerCount      int
	KafkaBrokers     []string
	KafkaGroupID     string
	KafkaTopic       string
	MaxRetries       int
	RetryDelay       time.Duration
	PostgresURL      string
	EncryptionKey    string
	// Paper Trading
	PaperWSPort      int
	PaperMarketWSURL string
	// Redis (market price feed)
	RedisAddr     string
	RedisPassword string
	// Observability
	MetricsPort int
}

func loadConfig() Config {
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaBrokers := []string{}
	for _, broker := range splitAndTrim(kafkaBrokersStr, ",") {
		if broker != "" {
			kafkaBrokers = append(kafkaBrokers, broker)
		}
	}

	return Config{
		GRPCPort:         getEnvInt("SERVICE_PORT", 9004),
		RabbitMQURL:      getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/"),
		QueueName:        getEnv("RABBITMQ_QUEUE", "trade.executions"),
		Exchange:         getEnv("RABBITMQ_EXCHANGE", "trade.execution"),
		RoutingKey:       getEnv("RABBITMQ_ROUTING_KEY", "order.new"),
		PrefetchCount:    getEnvInt("RABBITMQ_PREFETCH", 10),
		WorkerCount:      getEnvInt("WORKER_COUNT", 100),
		KafkaBrokers:     kafkaBrokers,
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "trade-execution-service"),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "trade-signals"),
		MaxRetries:       getEnvInt("MAX_RETRIES", 3),
		RetryDelay:       time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
		PostgresURL:      buildPostgresURL(),
		EncryptionKey:    getEnv("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef"),
		PaperWSPort:      getEnvInt("PAPER_WS_PORT", 8081),
		PaperMarketWSURL: getEnv("PAPER_MARKET_WS_URL", "wss://stockkaskwebsocket.indiratrade.com/enhanced-stream"),
		RedisAddr:        getEnv("REDIS_HOST", "localhost") + ":" + getEnv("REDIS_PORT", "6379"),
		RedisPassword:    getEnv("REDIS_PASSWORD", ""),
		MetricsPort:      getEnvInt("METRICS_PORT", 9090),
	}
}

func initPostgres(cfg Config) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", cfg.PostgresURL)
	if err != nil {
		return nil, err
	}

	// Configure connection pool — defaults should be >= WORKER_COUNT to avoid
	// worker threads blocking on DB connection acquisition.
	db.SetMaxOpenConns(getEnvInt("MAX_OPEN_CONNS", 120))
	db.SetMaxIdleConns(getEnvInt("MAX_IDLE_CONNS", 60))
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Verify required tables exist and give actionable errors if migrations haven't been applied
	if err := checkRequiredTables(db); err != nil {
		return nil, err
	}

	return db, nil
}

func buildPostgresURL() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "postgres"),
		getEnv("POSTGRES_PASSWORD", "postgres"),
		getEnv("POSTGRES_DB", "trading_execution"),
		getEnv("POSTGRES_SSL_MODE", "disable"),
	)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intVal int
		if _, err := fmt.Sscanf(value, "%d", &intVal); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func splitAndTrim(s, sep string) []string {
	if s == "" {
		return []string{}
	}
	parts := []string{}
	for _, part := range split(s, sep) {
		trimmed := trim(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func split(s, sep string) []string {
	result := []string{}
	current := ""
	for _, char := range s {
		if string(char) == sep {
			result = append(result, current)
			current = ""
		} else {
			current += string(char)
		}
	}
	result = append(result, current)
	return result
}

func trim(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

func initLogger() (*zap.Logger, error) {
	return zap.NewProduction()
}

// checkRequiredTables ensures critical tables for this service exist.
// If they don't, return a helpful error message explaining how to run migrations.
func checkRequiredTables(db *sqlx.DB) error {
	var exists bool
	query := `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'orders'
	)`

	if err := db.Get(&exists, query); err != nil {
		return fmt.Errorf("failed to check database schema: %w", err)
	}

	if !exists {
		// Give an actionable error pointing to migration SQL and setup script
		return fmt.Errorf("required table 'orders' does not exist in the database. " +
			"Run the migration: `psql -h <host> -U <user> -d <db> -f services/trade-execution/migrations/001_create_orders_table.sql` " +
			"or run `scripts/setup_all_databases.sh` to create databases and run migrations.")
	}

	return nil
}
