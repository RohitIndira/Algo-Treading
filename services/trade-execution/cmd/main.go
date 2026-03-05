package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	indiraPkg "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/paper"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
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
	defer db.Close()
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
	defer kafkaPub.Close()
	log.Println("✓ Kafka publisher initialized")

	// Initialize Order Status Service (WebSocket-based real-time order updates)
	// The backend opens one WS connection per user to Indira after placing their first order.
	log.Println("Initializing WebSocket Order Status Service...")
	statusService := statusservice.NewOrderStatusService(indiraClient, orderRepo, kafkaPub, logger)
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

	// Initialize Kafka signal consumer (trade-signals topic → SignalProcessor)
	log.Println("Initializing Kafka consumer for trade-signals...")
	signalProcessor := executor.NewSignalProcessor(orderExecutor, orderRepo, kafkaPub, statusService)
	kafkaConsumer := consumer.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, signalProcessor, logger)
	defer kafkaConsumer.Close()
	log.Println("✓ Kafka consumer initialized")

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
		result := make([]paper.BrokerPosition, len(positions))
		for i, p := range positions {
			result[i] = paper.BrokerPosition{
				Symbol:        p.Symbol,
				Exchange:      p.Exc,
				ProductType:   p.PrdType,
				NetQty:        p.NetQty,
				BuyQty:        p.BuyQty,
				SellQty:       p.SellQty,
				BuyAvgPrice:   p.BuyAvgPrice,
				SellAvgPrice:  p.SellAvgPrice,
				CurrentPrice:  p.CurrentPrice,
				PnL:           p.PnL,
				PnLPercentage: p.PnLPercentage,
			}
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

	// Initialize Redis price client — used for accurate order fill prices and PnL fallback.
	// Non-fatal: if Redis is unavailable the service still runs, just without the Redis fallback.
	redisPrices, redisErr := paper.NewRedisPriceClient(cfg.RedisAddr, cfg.RedisPassword)
	var priceLookup executor.PriceLookup
	if redisErr != nil {
		log.Printf("[paper] Redis price client unavailable (non-fatal): %v", redisErr)
		log.Println("[paper] Order fills will use signal price; PnL shown only when WSS is live")
	} else {
		log.Printf("✓ Redis price client connected (%s)", cfg.RedisAddr)
		priceLookup = redisPrices
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

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Start Kafka consumer — primary intake path (rules-engine trade-signals)
	go func() {
		log.Println("Starting Kafka consumer (trade-signals)...")
		if err := kafkaConsumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
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
	log.Printf("  - RabbitMQ Queue: %s", cfg.QueueName)
	log.Printf("  - Kafka Topic: %s (Group: %s)", cfg.KafkaTopic, cfg.KafkaGroupID)
	log.Printf("  - Workers: %d", cfg.WorkerCount)
	log.Println("========================================")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	log.Printf("\nReceived signal: %v", sig)
	log.Println("Initiating graceful shutdown...")

	cancel()

	// Give time for graceful shutdown
	time.Sleep(5 * time.Second)

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
		WorkerCount:      getEnvInt("WORKER_COUNT", 10),
		KafkaBrokers:     kafkaBrokers,
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "trade-execution-service"),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "trade-signals"),
		MaxRetries:       getEnvInt("MAX_RETRIES", 3),
		RetryDelay:       time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
		PostgresURL:      buildPostgresURL(),
		EncryptionKey:    getEnv("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef"),
		PaperWSPort:      getEnvInt("PAPER_WS_PORT", 8081),
		PaperMarketWSURL: getEnv("PAPER_MARKET_WS_URL", "wss://stockkaskwebsocket.indiratrade.com/enhanced-stream"),
		RedisAddr:        getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    getEnv("REDIS_PASSWORD", "R3d1s@Prod"),
	}
}

func initPostgres(cfg Config) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", cfg.PostgresURL)
	if err != nil {
		return nil, err
	}

	// Configure connection pool
	db.SetMaxOpenConns(getEnvInt("MAX_OPEN_CONNS", 25))
	db.SetMaxIdleConns(getEnvInt("MAX_IDLE_CONNS", 5))
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
