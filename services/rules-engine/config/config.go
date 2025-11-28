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

	// PostgreSQL Configuration (for loading strategies)
	PostgreSQL PostgreSQLConfig

	// Elasticsearch Configuration
	Elasticsearch ElasticsearchConfig

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

// PerformanceConfig holds performance tuning configuration
type PerformanceConfig struct {
	WorkerCount             int
	MaxBatchSize            int
	ProcessingTimeout       time.Duration
	MaxConcurrentMatches    int
	ESQueryTimeout          time.Duration
	CacheRefreshInterval    time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   time.Duration
	MinMatchScore           float64
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string
	Format     string // "json" or "console"
	OutputPath string
	ErrorPath  string
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
			Topic:             getEnv("KAFKA_TOPIC", "market.data.news"),
			ConsumerGroup:     getEnv("KAFKA_CONSUMER_GROUP", "rules-engine-group"),
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

		Elasticsearch: ElasticsearchConfig{
			URLs:                getEnvAsSlice("ELASTICSEARCH_URLS", []string{"http://localhost:9200"}),
			Username:            getEnv("ELASTICSEARCH_USERNAME", ""),
			Password:            getEnv("ELASTICSEARCH_PASSWORD", ""),
			IndexName:           getEnv("ELASTICSEARCH_INDEX", "user_strategies"),
			MaxRetries:          getEnvAsInt("ELASTICSEARCH_MAX_RETRIES", 3),
			RetryBackoff:        getEnvAsDuration("ELASTICSEARCH_RETRY_BACKOFF", 1*time.Second),
			HealthCheckInterval: getEnvAsDuration("ELASTICSEARCH_HEALTH_CHECK_INTERVAL", 30*time.Second),
			Timeout:             getEnvAsDuration("ELASTICSEARCH_TIMEOUT", 5*time.Second),
		},

		Redis: RedisConfig{
			Addrs:        getEnvAsSlice("REDIS_ADDRS", []string{"localhost:6379"}),
			Password:     getEnv("REDIS_PASSWORD", ""),
			DB:           getEnvAsInt("REDIS_DB", 0),
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
			ESQueryTimeout:          getEnvAsDuration("ES_QUERY_TIMEOUT", 2*time.Second),
			CacheRefreshInterval:    getEnvAsDuration("CACHE_REFRESH_INTERVAL", 1*time.Minute),
			CircuitBreakerThreshold: getEnvAsInt("CIRCUIT_BREAKER_THRESHOLD", 5),
			CircuitBreakerTimeout:   getEnvAsDuration("CIRCUIT_BREAKER_TIMEOUT", 60*time.Second),
			MinMatchScore:           getEnvAsFloat("MIN_MATCH_SCORE", 20.0),
		},

		Logging: LoggingConfig{
			Level:      getEnv("LOG_LEVEL", "info"),
			Format:     getEnv("LOG_FORMAT", "json"),
			OutputPath: getEnv("LOG_OUTPUT_PATH", "stdout"),
			ErrorPath:  getEnv("LOG_ERROR_PATH", "stderr"),
		},
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
	if len(c.Elasticsearch.URLs) == 0 {
		return fmt.Errorf("elasticsearch URLs cannot be empty")
	}
	if c.Elasticsearch.IndexName == "" {
		return fmt.Errorf("elasticsearch index name cannot be empty")
	}
	if len(c.Redis.Addrs) == 0 {
		return fmt.Errorf("redis addresses cannot be empty")
	}
	if c.RabbitMQ.URL == "" {
		return fmt.Errorf("rabbitmq URL cannot be empty")
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
	if c.Performance.MinMatchScore < 0 || c.Performance.MinMatchScore > 100 {
		return fmt.Errorf("min match score must be between 0 and 100")
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
