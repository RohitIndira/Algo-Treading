package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the rules engine service.
//
// Slimmed 2026-06-25 (post Cat A + Cat B trim). Removed sub-config types whose
// fields had zero readers after the news-event path was deleted:
//   - MongoDBConfig          (was: holiday checker)
//   - MarketHoursConfig      (was: news handler market-hours gating)
//   - LoggingConfig          (never wired)
//   - GRPCClientsConfig      (replaced by top-level UserConfigGRPCAddr;
//                             RiskManagement client was deleted in Cat B)
//   - PerformanceConfig      (only WorkerCount was read, in a misleading log line)
//   - Several KafkaConfig sub-fields tied to the news consumer
type Config struct {
	// Service identity
	ServiceName    string
	ServiceVersion string
	Environment    string
	GRPCPort       int
	MetricsPort    int

	// Kafka — only the live fields. Slimmed 2026-06-25.
	Kafka KafkaConfig

	// User Config gRPC — single source of truth for strategy state.
	UserConfigGRPCAddr string

	// PostgreSQL — manthan_positions / manthan_signal_decisions / etc.
	PostgreSQL PostgreSQLConfig

	// Redis — LTP feed + EMA target cache + manthan signal cache.
	Redis RedisConfig
}

// KafkaConfig — only the fields actually read by the live code paths.
//   - Brokers          → publisher init + config reader init
//   - UserConfigTopic  → config consumer reader topic
//   - UserConfigGroup  → config consumer reader group
//   - ConfigOffsetReset→ config consumer start offset
type KafkaConfig struct {
	Brokers           []string
	UserConfigTopic   string
	UserConfigGroup   string
	ConfigOffsetReset string // earliest/latest
}

// PostgreSQLConfig holds PostgreSQL-specific configuration.
type PostgreSQLConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
}

// RedisConfig holds Redis-specific configuration.
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

// LoadConfig loads configuration from environment variables.
func LoadConfig() (*Config, error) {
	config := &Config{
		ServiceName:    getEnv("SERVICE_NAME", "rules-engine"),
		ServiceVersion: getEnv("SERVICE_VERSION", "1.0.0"),
		Environment:    getEnv("ENVIRONMENT", "development"),
		GRPCPort:       getEnvAsInt("GRPC_PORT", 9003),
		MetricsPort:    getEnvAsInt("METRICS_PORT", 9103),

		Kafka: KafkaConfig{
			Brokers:           getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			UserConfigTopic:   getEnv("CONFIG_KAFKA_TOPIC", "user-config-events"),
			UserConfigGroup:   getEnv("CONFIG_CONSUMER_GROUP_ID", "rule-engine-config-sync"),
			ConfigOffsetReset: getEnv("CONFIG_KAFKA_OFFSET_RESET", "earliest"),
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
				//   - REDIS_ADDRS=host:port,host2:port2
				//   - REDIS_URI=host:port (or comma-separated)
				if uri := getEnv("REDIS_URI", ""); uri != "" {
					return strings.Split(uri, ",")
				}
				return getEnvAsSlice("REDIS_ADDRS", []string{"localhost:6379"})
			}(),
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

		UserConfigGRPCAddr: getEnv("USER_CONFIG_GRPC_ADDR", getEnv("USER_CONFIG_SERVICE_ADDR", "localhost:50051")),
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return config, nil
}

// Validate validates the configuration. Only checks the fields that LoadConfig
// still populates after the 2026-06-25 trim.
func (c *Config) Validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers cannot be empty")
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
	return nil
}

// Helper functions to get environment variables with defaults.

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
