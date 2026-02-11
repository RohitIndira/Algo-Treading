package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the rules engine service
type Config struct {
	// Service Configuration
	ServiceName    string
	ServiceVersion string
	Environment    string
	GRPCPort       int
	MetricsPort    int

	// Kafka Configuration
	Kafka KafkaConfig

	// RabbitMQ Configuration
	RabbitMQ RabbitMQConfig

	// PostgreSQL Configuration
	PostgreSQL PostgreSQLConfig

	// gRPC Client Configuration
	GRPCClients GRPCClientsConfig

	// Logging Configuration
	Logging LoggingConfig

	// Market Hours Configuration
	MarketHours MarketHoursConfig

	// Portfolio allocation state topic (generic for all strategies)
	PortfolioAllocTopic string

	// Jobbing Strategy Configuration
	JobbingTopic            string
	JobbingUserIDs          []string
	JobbingTokens           []string
	JobbingLowerRange       float64
	JobbingHigherRange      float64
	JobbingInitialOffset    float64
	JobbingDistanceContinue float64
	JobbingQtyPerOrder      int32
	JobbingMaxQty           int32
}

// KafkaConfig holds Kafka-specific configuration
type KafkaConfig struct {
	Brokers           []string
	Topic             string
	ConsumerGroup     string
	StartOffset       string // "earliest" or "latest"
	CommitInterval    time.Duration
	MaxBytes          int
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration
}

// PostgreSQLConfig holds PostgreSQL-specific configuration
type PostgreSQLConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
}

// ElasticsearchConfig holds Elasticsearch-specific configuration
type ElasticsearchConfig struct {
	URLs                []string
	Username            string
	Password            string
	IndexName           string
	MaxRetries          int
	RetryBackoff        time.Duration
	HealthCheckInterval time.Duration
	Timeout             time.Duration
}

// RedisConfig holds Redis-specific configuration
type RedisConfig struct {
	Addrs        []string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolTimeout  time.Duration
	CacheTTL     time.Duration
	ClusterMode  bool
}

// RabbitMQConfig holds RabbitMQ-specific configuration
type RabbitMQConfig struct {
	URL                  string
	Exchange             string
	ExchangeType         string
	Queue                string
	RoutingKey           string
	Durable              bool
	AutoDelete           bool
	Exclusive            bool
	NoWait               bool
	PrefetchCount        int
	ReconnectDelay       time.Duration
	MaxReconnectAttempts int
}

// GRPCClientsConfig holds gRPC client configuration
type GRPCClientsConfig struct {
	UserConfigService GRPCClientConfig
	RiskManagement    GRPCClientConfig
}

// GRPCClientConfig holds individual gRPC client configuration
type GRPCClientConfig struct {
	Address          string
	Timeout          time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string
	Format     string // "json" or "console"
	OutputPath string
	ErrorPath  string
}

// MarketHoursConfig holds market trading hours configuration
type MarketHoursConfig struct {
	OpenHour     int    // Market opening hour (0-23)
	OpenMinute   int    // Market opening minute (0-59)
	CloseHour    int    // Market closing hour (0-23)
	CloseMinute  int    // Market closing minute (0-59)
	Timezone     string // Timezone (e.g., "Asia/Kolkata")
	EnforceHours bool   // Whether to enforce market hours check
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	config := &Config{
		ServiceName:    getEnv("SERVICE_NAME", "rules-engine"),
		ServiceVersion: getEnv("SERVICE_VERSION", "1.0.0"),
		Environment:    getEnv("ENVIRONMENT", "development"),
		GRPCPort:       getEnvAsInt("GRPC_PORT", 9003),
		MetricsPort:    getEnvAsInt("METRICS_PORT", 9103),

		Kafka: KafkaConfig{
			Brokers:           getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			Topic:             getEnv("KAFKA_TOPIC", "market.data.live"),
			ConsumerGroup:     getEnv("KAFKA_CONSUMER_GROUP", "rules-engine-group"),
			StartOffset:       getEnv("KAFKA_START_OFFSET", "latest"),
			CommitInterval:    getEnvAsDuration("KAFKA_COMMIT_INTERVAL", 1*time.Second),
			MaxBytes:          getEnvAsInt("KAFKA_MAX_BYTES", 10485760), // 10MB
			SessionTimeout:    getEnvAsDuration("KAFKA_SESSION_TIMEOUT", 10*time.Second),
			HeartbeatInterval: getEnvAsDuration("KAFKA_HEARTBEAT_INTERVAL", 3*time.Second),
			RebalanceTimeout:  getEnvAsDuration("KAFKA_REBALANCE_TIMEOUT", 60*time.Second),
		},

		RabbitMQ: RabbitMQConfig{
			URL:                  getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/"),
			Exchange:             getEnv("RABBITMQ_EXCHANGE", "orders"),
			ExchangeType:         getEnv("RABBITMQ_EXCHANGE_TYPE", "direct"),
			Queue:                getEnv("RABBITMQ_QUEUE", "trade_signals"),
			RoutingKey:           getEnv("RABBITMQ_ROUTING_KEY", "order.new"),
			Durable:              getEnvAsBool("RABBITMQ_DURABLE", true),
			AutoDelete:           getEnvAsBool("RABBITMQ_AUTO_DELETE", false),
			Exclusive:            getEnvAsBool("RABBITMQ_EXCLUSIVE", false),
			NoWait:               getEnvAsBool("RABBITMQ_NO_WAIT", false),
			PrefetchCount:        getEnvAsInt("RABBITMQ_PREFETCH_COUNT", 10),
			ReconnectDelay:       getEnvAsDuration("RABBITMQ_RECONNECT_DELAY", 5*time.Second),
			MaxReconnectAttempts: getEnvAsInt("RABBITMQ_MAX_RECONNECT_ATTEMPTS", 10),
		},

		PostgreSQL: PostgreSQLConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     getEnv("POSTGRES_PORT", "5432"),
			User:     getEnv("POSTGRES_USER", "algo_user"),
			Password: getEnv("POSTGRES_PASSWORD", "algopass123"),
			Database: getEnv("POSTGRES_DB", "algotrading"),
			SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
		},

		GRPCClients: GRPCClientsConfig{
			UserConfigService: GRPCClientConfig{
				Address:          getEnv("USER_CONFIG_SERVICE_ADDR", "localhost:50051"),
				Timeout:          getEnvAsDuration("USER_CONFIG_SERVICE_TIMEOUT", 5*time.Second),
				MaxRetries:       getEnvAsInt("USER_CONFIG_SERVICE_MAX_RETRIES", 3),
				RetryBackoff:     getEnvAsDuration("USER_CONFIG_SERVICE_RETRY_BACKOFF", 1*time.Second),
				KeepAlive:        getEnvAsDuration("USER_CONFIG_SERVICE_KEEP_ALIVE", 30*time.Second),
				KeepAliveTimeout: getEnvAsDuration("USER_CONFIG_SERVICE_KEEP_ALIVE_TIMEOUT", 10*time.Second),
			},
			RiskManagement: GRPCClientConfig{
				Address:          getEnv("RISK_MANAGEMENT_SERVICE_ADDR", "localhost:9005"),
				Timeout:          getEnvAsDuration("RISK_MANAGEMENT_SERVICE_TIMEOUT", 3*time.Second),
				MaxRetries:       getEnvAsInt("RISK_MANAGEMENT_SERVICE_MAX_RETRIES", 2),
				RetryBackoff:     getEnvAsDuration("RISK_MANAGEMENT_SERVICE_RETRY_BACKOFF", 500*time.Millisecond),
				KeepAlive:        getEnvAsDuration("RISK_MANAGEMENT_SERVICE_KEEP_ALIVE", 30*time.Second),
				KeepAliveTimeout: getEnvAsDuration("RISK_MANAGEMENT_SERVICE_KEEP_ALIVE_TIMEOUT", 10*time.Second),
			},
		},

		Logging: LoggingConfig{
			Level:      getEnv("LOG_LEVEL", "info"),
			Format:     getEnv("LOG_FORMAT", "json"),
			OutputPath: getEnv("LOG_OUTPUT_PATH", "stdout"),
			ErrorPath:  getEnv("LOG_ERROR_PATH", "stderr"),
		},

		MarketHours: MarketHoursConfig{
			OpenHour:     getEnvAsInt("MARKET_OPEN_HOUR", 9),
			OpenMinute:   getEnvAsInt("MARKET_OPEN_MINUTE", 15),
			CloseHour:    getEnvAsInt("MARKET_CLOSE_HOUR", 15),
			CloseMinute:  getEnvAsInt("MARKET_CLOSE_MINUTE", 30),
			Timezone:     getEnv("MARKET_TIMEZONE", "Asia/Kolkata"),
			EnforceHours: getEnvAsBool("MARKET_ENFORCE_HOURS", true),
		},

		PortfolioAllocTopic: getEnv("KAFKA_TOPIC_PORTFOLIO_ALLOCATIONS", "portfolio.allocations"),

		// Jobbing Strategy Configuration
		JobbingTopic:            getEnv("JOBBING_TOPIC", "market.data.live"),
		JobbingUserIDs:          getEnvAsSlice("JOBBING_USER_IDS", []string{}),
		JobbingTokens:           getEnvAsSlice("JOBBING_TOKENS", []string{}),
		JobbingLowerRange:       getEnvAsFloat("JOBBING_LOWER_RANGE", 10.0),
		JobbingHigherRange:      getEnvAsFloat("JOBBING_HIGHER_RANGE", 15.0),
		JobbingInitialOffset:    getEnvAsFloat("JOBBING_INITIAL_OFFSET", 0.01),
		JobbingDistanceContinue: getEnvAsFloat("JOBBING_DISTANCE_CONTINUE", 0.01),
		JobbingQtyPerOrder:      int32(getEnvAsInt("JOBBING_QTY_PER_ORDER", 1)),
		JobbingMaxQty:           int32(getEnvAsInt("JOBBING_MAX_QTY", 10)),
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers cannot be empty")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic cannot be empty")
	}
	if c.Kafka.ConsumerGroup == "" {
		return fmt.Errorf("kafka consumer group cannot be empty")
	}
	if c.RabbitMQ.URL == "" {
		return fmt.Errorf("rabbitmq URL cannot be empty")
	}
	if c.GRPCClients.UserConfigService.Address == "" {
		return fmt.Errorf("user config service address cannot be empty")
	}

	// Validate Jobbing configuration if enabled
	if len(c.JobbingUserIDs) > 0 {
		if c.JobbingTopic == "" {
			return fmt.Errorf("jobbing topic cannot be empty when users are configured")
		}
		if c.JobbingLowerRange <= 0 {
			return fmt.Errorf("jobbing lower range must be positive")
		}
		if c.JobbingHigherRange <= c.JobbingLowerRange {
			return fmt.Errorf("jobbing higher range must be greater than lower range")
		}
		if c.JobbingInitialOffset <= 0 {
			return fmt.Errorf("jobbing initial offset must be positive")
		}
		if c.JobbingDistanceContinue <= 0 {
			return fmt.Errorf("jobbing distance continue must be positive")
		}
		if c.JobbingQtyPerOrder <= 0 {
			return fmt.Errorf("jobbing quantity per order must be positive")
		}
		if c.JobbingMaxQty <= 0 {
			return fmt.Errorf("jobbing max quantity must be positive")
		}
		if c.JobbingMaxQty < c.JobbingQtyPerOrder {
			return fmt.Errorf("jobbing max quantity must be >= quantity per order")
		}
	}

	return nil
}

// Helper functions to get environment variables with defaults

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsFloat(key string, defaultValue float64) float64 {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := time.ParseDuration(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsSlice(key string, defaultValue []string) []string {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	return strings.Split(valueStr, ",")
}
