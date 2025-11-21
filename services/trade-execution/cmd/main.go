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

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/odin"
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

	// Initialize repository
	orderRepo := repository.NewOrderRepository(db)
	log.Println("✓ Repository layer initialized")

	// Initialize Odin client
	odinClient := odin.NewExecutionClient(cfg.OdinBaseURL)
	log.Println("✓ Odin API client initialized")

	// Initialize executor
	orderExecutor := executor.NewOrderExecutor(
		orderRepo,
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
		RoutingKey:    cfg.RoutingKey,
		PrefetchCount: cfg.PrefetchCount,
		WorkerCount:   cfg.WorkerCount,
	}

	rabbitConsumer, err := consumer.NewRabbitMQConsumer(consumerCfg, orderExecutor, orderRepo)
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ consumer: %v", err)
	}
	defer rabbitConsumer.Shutdown()
	log.Println("✓ RabbitMQ consumer initialized")

	// Initialize gRPC server
	grpcServer := server.NewServer(orderRepo, orderExecutor, cfg.GRPCPort)
	log.Println("✓ gRPC server initialized")

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	OdinBaseURL   string
	MaxRetries    int
	RetryDelay    time.Duration
	PostgresURL   string
}

func loadConfig() Config {
	return Config{
		GRPCPort:      getEnvInt("SERVICE_PORT", 9004),
		RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://guest:guest123@localhost:5672/"),
		QueueName:     getEnv("RABBITMQ_QUEUE", "order.execution.queue"),
		Exchange:      getEnv("RABBITMQ_EXCHANGE", "order.execution.exchange"),
		RoutingKey:    getEnv("RABBITMQ_ROUTING_KEY", "order.execution"),
		PrefetchCount: getEnvInt("RABBITMQ_PREFETCH", 10),
		WorkerCount:   getEnvInt("WORKER_COUNT", 10),
		OdinBaseURL:   getEnv("ODIN_BASE_URL", ""),
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
