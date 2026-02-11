package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

func init() {
	cwd, _ := os.Getwd()
	log.Println("Current working directory:", cwd)

	if err := godotenv.Load(); err != nil {
		log.Println("--------->No .env file found, using environment variables")
	}
}

// Config holds data-ingestion configuration
type Config struct {

	// Kafka configuration
	KafkaBrokers []string
	KafkaTopic   string

	// B2C API Bridge configuration
	B2CBridgePath string
	B2CTokens     []string

	// External Redis market data (52-week highs, etc.)
	MarketRedisAddr     string
	MarketRedisPassword string
	MarketRedisDB       int

	// StocksDBPath is the path to the SQLite database that contains
	// the stock_subscriptions table. This is used to dynamically
	// determine which tokens (and exchanges) should be subscribed to
	// via the B2C bridge, instead of hardcoding B2C_TOKENS in env.
	StocksDBPath string

	// Worker configuration
	WorkerCount int
	MaxRetries  int

	// Timeouts
	MongoConnectTimeout time.Duration
}

// Load loads configuration from environment variables with sensible defaults
func Load() *Config {
	fmt.Println("Loading data-ingestion configuration for Jobbing strategy")

	// Kafka Brokers
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	// Kafka Topic for Live Market Data (Jobbing)
	topicLive := os.Getenv("KAFKA_TOPIC_LIVE")
	if topicLive == "" {
		topicLive = "market.data.live"
	}

	// B2C Bridge Path
	bridgePath := os.Getenv("B2C_BRIDGE_PATH")
	if bridgePath == "" {
		// Default relative path when running from services/data-ingestion/
		// (can be overridden via .env if needed)
		bridgePath = filepath.Join("..", "..", "..", "b2c-api-python", "b2c_bridge.py")
	}

	// B2C Tokens as comma-separated list
	tokensStr := getEnv("B2C_TOKENS", "22,2475,25,14366, 9428")
	var tokens []string
	if tokensStr != "" {
		tokens = strings.Split(tokensStr, ",")
		// Trim whitespace from each token
		for i := range tokens {
			tokens[i] = strings.TrimSpace(tokens[i])
		}
	}

	// Worker configuration
	workerCount := 4
	if wc := os.Getenv("WORKER_COUNT"); wc != "" {
		if parsed, err := strconv.Atoi(wc); err == nil && parsed > 0 {
			workerCount = parsed
		}
	}

	maxRetries := 3
	if mr := os.Getenv("MAX_RETRIES"); mr != "" {
		if parsed, err := strconv.Atoi(mr); err == nil && parsed > 0 {
			maxRetries = parsed
		}
	}

	// Redis configuration for external market data store
	redisAddr := getEnv("MARKET_REDIS_ADDR", "localhost:6379")
	redisPassword := os.Getenv("MARKET_REDIS_PASSWORD")
	redisDB := 0
	if v := os.Getenv("MARKET_REDIS_DB"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed >= 0 {
			redisDB = parsed
		}
	}

	return &Config{
		// Kafka
		KafkaBrokers: strings.Split(brokers, ","),
		KafkaTopic:   topicLive,

		// B2C Bridge
		B2CBridgePath: bridgePath,
		B2CTokens:     tokens,
		// Default path assumes runtime from services/data-ingestion/; you
		// have already set the correct absolute path in .env, which will
		// override this.
		StocksDBPath: getEnv("STOCKS_DB_PATH", filepath.Join("..", "..", "..", "stocks.db")),

		// Workers
		WorkerCount: workerCount,
		MaxRetries:  maxRetries,

		// Redis
		MarketRedisAddr:     redisAddr,
		MarketRedisPassword: redisPassword,
		MarketRedisDB:       redisDB,
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
