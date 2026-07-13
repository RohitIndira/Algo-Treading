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
	GRPCPort        int
	MetricsPort     int
	PreviewHTTPPort int // lightweight HTTP server for AMN preview (default 8082)
	// PreviewSnapshotRefreshSecs is how often the shared AMN preview snapshot
	// (news + company + live market data) is rebuilt. Requests filter the
	// in-memory snapshot, so this bounds the staleness of preview LTP/%change.
	PreviewSnapshotRefreshSecs int

	// Kafka Configuration
	Kafka KafkaConfig

	// User Config gRPC (bootstrap source-of-truth)
	UserConfigGRPCAddr string

	// PostgreSQL Configuration (for loading strategies)
	PostgreSQL PostgreSQLConfig

	// Redis Configuration
	Redis RedisConfig

	// RabbitMQ Configuration
	RabbitMQ RabbitMQConfig

	// gRPC Client Configuration
	GRPCClients GRPCClientsConfig

	// Performance Configuration
	Performance PerformanceConfig

	// Logging Configuration
	Logging LoggingConfig

	// Market Hours Configuration
	MarketHours MarketHoursConfig

	// MongoDB Configuration (for trading holidays)
	MongoDB MongoDBConfig

	// BypassHolidayCheck, when true, processes today's news even if today is a
	// trading holiday (SEBI Saturday mock/demo). From BYPASS_HOLIDAY_CHECK.
	BypassHolidayCheck bool
}

// MongoDBConfig holds MongoDB connection configuration
type MongoDBConfig struct {
	URI string
}

// KafkaConfig holds Kafka-specific configuration
type KafkaConfig struct {
	Brokers           []string
	Topic             string // news topic
	ConsumerGroup     string // news group
	UserConfigTopic   string // config topic
	UserConfigGroup   string // config group
	ConfigOffsetReset string // earliest/latest
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

// RedisConfig holds Redis-specific configuration
type RedisConfig struct {
	Addrs        []string
	Password     string
	DB           int
	// TickstoreDB is the Redis DB where trade-execution's tickstore writes the
	// recent tick stream (ticks:{exch}:{token}). Read by the market-price-
	// protection (velocity) check. Default 1.
	TickstoreDB  int
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

// PerformanceConfig holds performance tuning configuration
type PerformanceConfig struct {
	WorkerCount             int
	MaxBatchSize            int
	ProcessingTimeout       time.Duration
	MaxConcurrentMatches    int
	CacheRefreshInterval    time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   time.Duration
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
	OpenHour      int    // Market opening hour (0-23)
	OpenMinute    int    // Market opening minute (0-59)
	CloseHour     int    // Market closing hour (0-23)
	CloseMinute   int    // Market closing minute (0-59)
	Timezone      string // Timezone (e.g., "Asia/Kolkata")
	EnforceHours  bool   // Whether to enforce market hours check
	AllowSaturday bool   // Treat Saturday as a trading day (SEBI mock session)
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	config := &Config{
		ServiceName:    getEnv("SERVICE_NAME", "rules-engine"),
		ServiceVersion: getEnv("SERVICE_VERSION", "1.0.0"),
		Environment:    getEnv("ENVIRONMENT", "development"),
		GRPCPort:        getEnvAsInt("GRPC_PORT", 9003),
		MetricsPort:     getEnvAsInt("METRICS_PORT", 9103),
		PreviewHTTPPort:            getEnvAsInt("PREVIEW_HTTP_PORT", 8082),
		PreviewSnapshotRefreshSecs: getEnvAsInt("PREVIEW_SNAPSHOT_REFRESH_SECS", 30),

		Kafka: KafkaConfig{
			Brokers:           getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			Topic:             getEnv("KAFKA_TOPIC", "news-events"),
			ConsumerGroup:     getEnv("NEWS_CONSUMER_GROUP_ID", getEnv("KAFKA_CONSUMER_GROUP", "rule-engine-news-processor")),
			UserConfigTopic:   getEnv("CONFIG_KAFKA_TOPIC", "user-config-events"),
			UserConfigGroup:   getEnv("CONFIG_CONSUMER_GROUP_ID", "rule-engine-config-sync"),
			ConfigOffsetReset: getEnv("CONFIG_KAFKA_OFFSET_RESET", "earliest"),
			StartOffset:       getEnv("KAFKA_START_OFFSET", "latest"),
			CommitInterval:    getEnvAsDuration("KAFKA_COMMIT_INTERVAL", 1*time.Second),
			MaxBytes:          getEnvAsInt("KAFKA_MAX_BYTES", 10485760), // 10MB
			SessionTimeout:    getEnvAsDuration("KAFKA_SESSION_TIMEOUT", 10*time.Second),
			HeartbeatInterval: getEnvAsDuration("KAFKA_HEARTBEAT_INTERVAL", 3*time.Second),
			RebalanceTimeout:  getEnvAsDuration("KAFKA_REBALANCE_TIMEOUT", 60*time.Second),
		},

		PostgreSQL: PostgreSQLConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     getEnv("POSTGRES_PORT", "5432"),
			User:     getEnv("POSTGRES_USER", "postgres"),
			Password: getEnv("POSTGRES_PASSWORD", "postgres"),
			Database: getEnv("POSTGRES_DB", "trading_db"),
			SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
		},

		Redis: RedisConfig{
			Addrs: func() []string {
				// Backward compatible:
				// - REDIS_ADDRS=host:port,host2:port2
				// - REDIS_URI=host:port (or comma-separated)
				if uri := getEnv("REDIS_URI", ""); uri != "" {
					return strings.Split(uri, ",")
				}
				return getEnvAsSlice("REDIS_ADDRS", []string{"localhost:6379"})
			}(),
			Password:     getEnv("REDIS_PASSWORD", ""),
			DB:           getEnvAsInt("REDIS_DB", 0),
			TickstoreDB:  getEnvAsInt("TICKSTORE_REDIS_DB", 1),
			PoolSize:     getEnvAsInt("REDIS_POOL_SIZE", 100),
			MinIdleConns: getEnvAsInt("REDIS_MIN_IDLE_CONNS", 10),
			MaxRetries:   getEnvAsInt("REDIS_MAX_RETRIES", 3),
			DialTimeout:  getEnvAsDuration("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  getEnvAsDuration("REDIS_READ_TIMEOUT", 3*time.Second),
			WriteTimeout: getEnvAsDuration("REDIS_WRITE_TIMEOUT", 3*time.Second),
			PoolTimeout:  getEnvAsDuration("REDIS_POOL_TIMEOUT", 4*time.Second),
			CacheTTL:     getEnvAsDuration("REDIS_CACHE_TTL", 5*time.Minute),
			ClusterMode:  getEnvAsBool("REDIS_CLUSTER_MODE", false),
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

		GRPCClients: GRPCClientsConfig{
			UserConfigService: GRPCClientConfig{
				Address:          getEnv("USER_CONFIG_SERVICE_ADDR", "localhost:9001"),
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

		Performance: PerformanceConfig{
			WorkerCount:             getEnvAsInt("WORKER_COUNT", 50),
			MaxBatchSize:            getEnvAsInt("MAX_BATCH_SIZE", 100),
			ProcessingTimeout:       getEnvAsDuration("PROCESSING_TIMEOUT", 30*time.Second),
			MaxConcurrentMatches:    getEnvAsInt("MAX_CONCURRENT_MATCHES", 100),
			CacheRefreshInterval:    getEnvAsDuration("CACHE_REFRESH_INTERVAL", 1*time.Minute),
			CircuitBreakerThreshold: getEnvAsInt("CIRCUIT_BREAKER_THRESHOLD", 5),
			CircuitBreakerTimeout:   getEnvAsDuration("CIRCUIT_BREAKER_TIMEOUT", 60*time.Second),
		},

		Logging: LoggingConfig{
			Level:      getEnv("LOG_LEVEL", "info"),
			Format:     getEnv("LOG_FORMAT", "json"),
			OutputPath: getEnv("LOG_OUTPUT_PATH", "stdout"),
			ErrorPath:  getEnv("LOG_ERROR_PATH", "stderr"),
		},

		MarketHours: MarketHoursConfig{
			OpenHour:      getEnvAsInt("MARKET_OPEN_HOUR", 9),
			OpenMinute:    getEnvAsInt("MARKET_OPEN_MINUTE", 15),
			CloseHour:     getEnvAsInt("MARKET_CLOSE_HOUR", 15),
			CloseMinute:   getEnvAsInt("MARKET_CLOSE_MINUTE", 30),
			Timezone:      getEnv("MARKET_TIMEZONE", "Asia/Kolkata"),
			EnforceHours:  getEnvAsBool("MARKET_ENFORCE_HOURS", true),
			AllowSaturday: getEnvAsBool("ALLOW_SATURDAY_MOCK", false),
		},

		UserConfigGRPCAddr: getEnv("USER_CONFIG_GRPC_ADDR", getEnv("USER_CONFIG_SERVICE_ADDR", "localhost:50051")),

		MongoDB: MongoDBConfig{
			URI: getEnv("MONGODB_URI", "mongodb://localhost:27017"),
		},

		BypassHolidayCheck: getEnvAsBool("BYPASS_HOLIDAY_CHECK", false),
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
	if c.Kafka.UserConfigTopic == "" {
		return fmt.Errorf("config kafka topic cannot be empty")
	}
	if c.Kafka.UserConfigGroup == "" {
		return fmt.Errorf("config consumer group cannot be empty")
	}
	if c.UserConfigGRPCAddr == "" {
		return fmt.Errorf("USER_CONFIG_GRPC_ADDR cannot be empty")
	}
	if len(c.Redis.Addrs) == 0 {
		return fmt.Errorf("redis addresses cannot be empty")
	}
	if c.GRPCClients.UserConfigService.Address == "" {
		return fmt.Errorf("user config service address cannot be empty")
	}
	if c.GRPCClients.RiskManagement.Address == "" {
		return fmt.Errorf("risk management service address cannot be empty")
	}
	if c.Performance.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be positive")
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
