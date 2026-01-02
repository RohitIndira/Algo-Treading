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

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
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

	// Initialize repositories
	orderRepo := repository.NewOrderRepository(db)
	credsRepo := repository.NewCredentialsRepository(db)
	log.Println("✓ Repository layer initialized")

	// Initialize Indira client (stateless, supports multiple users)
	indiraClient := indira.NewExecutionClient()
	log.Println("✓ Indira API client initialized")

	// Initialize executor with credentials repository
	orderExecutor := executor.NewOrderExecutor(
		orderRepo,
		credsRepo,
		indiraClient,
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

	// Initialize Kafka consumer for trade-signals
	log.Println("Initializing Kafka consumer...")
	signalProcessor := executor.NewSignalProcessor(orderExecutor, orderRepo, rabbitPublisher, cfg.SkipDBSave)
	kafkaConsumer := consumer.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, signalProcessor, logger)
	defer kafkaConsumer.Close()
	log.Println("✓ Kafka consumer initialized")

	// Initialize gRPC server
	grpcServer := server.NewServer(orderRepo, orderExecutor, cfg.GRPCPort)
	log.Println("✓ gRPC server initialized")

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	GRPCPort      int
	RabbitMQURL   string
	QueueName     string
	Exchange      string
	RoutingKey    string
	PrefetchCount int
	WorkerCount   int
	KafkaBrokers  []string
	KafkaGroupID  string
	KafkaTopic    string
	MaxRetries    int
	RetryDelay    time.Duration
	PostgresURL   string
	SkipDBSave    bool `env:"SKIP_DB_SAVE" envDefault:"true"`
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
		GRPCPort:      getEnvInt("SERVICE_PORT", 9004),
		RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/"),
		QueueName:     getEnv("RABBITMQ_QUEUE", "trade.executions"),
		Exchange:      getEnv("RABBITMQ_EXCHANGE", "trade.execution"),
		RoutingKey:    getEnv("RABBITMQ_ROUTING_KEY", "order.new"),
		PrefetchCount: getEnvInt("RABBITMQ_PREFETCH", 10),
		WorkerCount:   getEnvInt("WORKER_COUNT", 10),
		KafkaBrokers:  kafkaBrokers,
		KafkaGroupID:  getEnv("KAFKA_GROUP_ID", "trade-execution-service"),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "trade-signals"),
		MaxRetries:    getEnvInt("MAX_RETRIES", 3),
		RetryDelay:    time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
		PostgresURL:   buildPostgresURL(),
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
		getEnv("POSTGRES_PORT", "55432"),
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
