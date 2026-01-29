package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/odin"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/paper"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/server"
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

	// Initialize separate PostgreSQL connection for credentials (user-login DB).
	// This ensures we always read broker credentials from the same
	// trading_db used by the user-login service, instead of duplicating
	// tables in the trade-execution database.
	credsDB, err := initCredsPostgres()
	if err != nil {
		log.Fatalf("Failed to connect to credentials PostgreSQL (trading_db): %v", err)
	}
	defer credsDB.Close()
	log.Println("✓ Connected to PostgreSQL (credentials DB)")

	// Initialize repositories
	orderRepo := repository.NewOrderRepository(db)
	paperPosRepo := repository.NewPaperPositionRepository(db)
	// CredentialsRepository now points to the user-login database
	// (trading_db) so any user created via user-login immediately has
	// usable broker credentials for trade execution.
	credsRepo := repository.NewCredentialsRepository(credsDB)
	log.Println("✓ Repository layer initialized")

	// Initialize Redis for paper trading
	log.Println("Connecting to Redis...")
	redisClient := initRedis()
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Printf("⚠️ Redis connection failed: %v (paper trading will not work)", err)
		log.Println("Continuing without Redis...")
	} else {
		log.Println("✓ Connected to Redis")
	}

	// Initialize Odin client
	odinClient := odin.NewExecutionClient(cfg.OdinBaseURL)
	log.Println("✓ Odin API client initialized")

	// Initialize executor with credentials repository
	orderExecutor := executor.NewOrderExecutor(
		orderRepo,
		credsRepo,
		odinClient,
		cfg.MaxRetries,
		cfg.RetryDelay,
	)
	log.Println("✓ Order executor initialized")

	// Initialize RabbitMQ consumer
	log.Println("Connecting to RabbitMQ...")
	consumerCfg := consumer.Config{
		URL:           cfg.RabbitMQURL,
		QueueName:     cfg.QueueName,
		Exchange:      cfg.Exchange,
		ExchangeType:  "topic",
		RoutingKey:    cfg.RoutingKey,
		PrefetchCount: cfg.PrefetchCount,
		WorkerCount:   cfg.WorkerCount,
		Durable:       true,
	}

	rabbitConsumer, err := consumer.NewRabbitMQConsumer(consumerCfg, orderExecutor, orderRepo)
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ consumer: %v", err)
	}
	defer rabbitConsumer.Shutdown()
	log.Println("✓ RabbitMQ consumer initialized")

	// Initialize RabbitMQ publisher for odin-api-wrapper
	log.Println("Initializing RabbitMQ publisher for odin-api-wrapper...")
	logger, _ := initLogger()
	rabbitPublisher, err := publisher.NewRabbitMQPublisher(
		cfg.RabbitMQURL,
		cfg.Exchange,
		cfg.RoutingKey,
		logger,
	)
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ publisher: %v", err)
	}
	defer rabbitPublisher.Close()
	log.Println("✓ RabbitMQ publisher initialized")

	// Initialize paper trading components
	log.Println("Initializing paper trading system...")
	priceProvider := paper.NewRedisPriceProvider(redisClient)
	positionManager := paper.NewPositionManager(
		paperPosRepo,
		orderRepo,
		priceProvider,
		cfg.PaperPositionCheckInterval,
	)
	paperTradeHandler := executor.NewPaperTradeHandler(positionManager)
	log.Println("✓ Paper trading system initialized")

	// Initialize Kafka consumer for trade-signals
	log.Println("Initializing Kafka consumer...")
	signalProcessor := executor.NewSignalProcessor(orderExecutor, orderRepo, rabbitPublisher, paperTradeHandler)
	kafkaConsumer := consumer.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, signalProcessor, logger)
	defer kafkaConsumer.Close()
	log.Println("✓ Kafka consumer initialized")

	// Initialize gRPC server
	grpcServer := server.NewServer(orderRepo, orderExecutor, cfg.GRPCPort)
	log.Println("✓ gRPC server initialized")

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start paper position manager for SL/TP monitoring
	go func() {
		log.Println("Starting paper position manager...")
		if err := positionManager.Start(ctx); err != nil {
			log.Printf("⚠️ Failed to start position manager: %v", err)
		} else {
			log.Println("✓ Paper position manager started")
		}
	}()

	// Start Kafka consumer for trade-signals
	go func() {
		log.Println("Starting Kafka consumer...")
		if err := kafkaConsumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	// Start RabbitMQ consumer
	go func() {
		log.Println("Starting RabbitMQ consumer...")
		if err := rabbitConsumer.Start(ctx); err != nil {
			log.Fatalf("RabbitMQ consumer error: %v", err)
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
	GRPCPort                   int
	RabbitMQURL                string
	QueueName                  string
	Exchange                   string
	RoutingKey                 string
	PrefetchCount              int
	WorkerCount                int
	KafkaBrokers               []string
	KafkaGroupID               string
	KafkaTopic                 string
	OdinBaseURL                string
	MaxRetries                 int
	RetryDelay                 time.Duration
	PostgresURL                string
	RedisAddr                  string
	RedisPassword              string
	RedisDB                    int
	PaperPositionCheckInterval time.Duration
}

func loadConfig() Config {
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaBrokers := []string{}
	for _, broker := range splitAndTrim(kafkaBrokersStr, ",") {
		if broker != "" {
			kafkaBrokers = append(kafkaBrokers, broker)
		}
	}

	redisAddr := fmt.Sprintf("%s:%s",
		getEnv("REDIS_HOST", "localhost"),
		getEnv("REDIS_PORT", "6379"),
	)

	return Config{
		GRPCPort:                   getEnvInt("SERVICE_PORT", 9004),
		RabbitMQURL:                getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/"),
		QueueName:                  getEnv("RABBITMQ_QUEUE", "trade.executions"),
		Exchange:                   getEnv("RABBITMQ_EXCHANGE", "trade.execution"),
		RoutingKey:                 getEnv("RABBITMQ_ROUTING_KEY", "order.new"),
		PrefetchCount:              getEnvInt("RABBITMQ_PREFETCH", 10),
		WorkerCount:                getEnvInt("WORKER_COUNT", 10),
		KafkaBrokers:               kafkaBrokers,
		KafkaGroupID:               getEnv("KAFKA_GROUP_ID", "trade-execution-service"),
		KafkaTopic:                 getEnv("KAFKA_TOPIC", "trade-signals"),
		OdinBaseURL:                getEnv("ODIN_BASE_URL", ""),
		MaxRetries:                 getEnvInt("MAX_RETRIES", 3),
		RetryDelay:                 time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
		PostgresURL:                buildPostgresURL(),
		RedisAddr:                  redisAddr,
		RedisPassword:              getEnv("REDIS_PASSWORD", ""),
		RedisDB:                    getEnvInt("REDIS_DB", 0),
		PaperPositionCheckInterval: time.Duration(getEnvInt("PAPER_POSITION_CHECK_INTERVAL_SEC", 10)) * time.Second,
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

	return db, nil
}

// initCredsPostgres initializes a separate PostgreSQL connection for
// reading broker/user credentials. By default this points to the same
// trading_db used by the user-login service, so any user created via
// user-login automatically has credentials available for trade-execution.
//
// You can override these defaults with CRED_DB_* environment variables
// if needed:
//
//	CRED_DB_HOST, CRED_DB_PORT, CRED_DB_USER,
//	CRED_DB_PASSWORD, CRED_DB_NAME, CRED_DB_SSLMODE
func initCredsPostgres() (*sqlx.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("CRED_DB_HOST", "localhost"),
		getEnv("CRED_DB_PORT", "5432"),
		getEnv("CRED_DB_USER", "postgres"),
		getEnv("CRED_DB_PASSWORD", "postgres"),
		getEnv("CRED_DB_NAME", "trading_db"),
		getEnv("CRED_DB_SSLMODE", "disable"),
	)

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}

	// Keep a modest pool for credentials lookups
	db.SetMaxOpenConns(getEnvInt("CRED_DB_MAX_OPEN_CONNS", 10))
	db.SetMaxIdleConns(getEnvInt("CRED_DB_MAX_IDLE_CONNS", 5))
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping credentials database: %w", err)
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
		getEnv("POSTGRES_DB", "trading_db"),
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

func initRedis() *redis.Client {
	cfg := loadConfig()
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}
