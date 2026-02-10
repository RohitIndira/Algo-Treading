package config

import (
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
	// MongoDB configuration
	MongoURI        string
	MongoDatabase   string
	MongoCollection string

	// Kafka configuration
	KafkaBrokers []string
	KafkaTopic   string

	// Additional Kafka topic for 52-week high breakout events
	KafkaTopic52Week string

	// Additional Kafka topic for live market data
	KafkaTopicMarketData string

	// External Redis market data configuration (for 52-week highs, etc.)
	MarketRedisAddr     string
	MarketRedisPassword string
	MarketRedisDB       int

	// B2C API Bridge configuration
	B2CBridgePath string
	B2CTokens     []string

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

	// Event-driven worker pool configuration
	WorkerPoolSize int // Number of workers for processing events
}

// Load loads configuration from environment variables with sensible defaults
func Load() *Config {
	// Kafka Brokers
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	// Split and trim brokers so values like "host1:9092, host2:9092" work.
	kafkaBrokers := strings.Split(brokers, ",")
	filtered := kafkaBrokers[:0]
	for _, b := range kafkaBrokers {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		filtered = append(filtered, b)
	}
	kafkaBrokers = filtered

	// Kafka Topic for News
	topicNews := os.Getenv("KAFKA_TOPIC_NEWS")
	if topicNews == "" {
		topicNews = "market.data.news"
	}

	// Kafka Topic for 52-week breakouts
	breakoutTopic := os.Getenv("KAFKA_TOPIC_52W_BREAKOUT")
	if breakoutTopic == "" {
		breakoutTopic = "market.data.52w_breakouts"
	}

	// Kafka Topic for Live Market Data
	topicMarketData := os.Getenv("KAFKA_TOPIC_MARKET_DATA")
	if topicMarketData == "" {
		topicMarketData = "market.data.live"
	}

	// MongoDB configuration
	db := os.Getenv("MONGO_DATABASE")
	if db == "" {
		db = "CAG_CHATBOT"
	}

	coll := os.Getenv("MONGO_NEWS_COLLECTION")
	if coll == "" {
		coll = "NewsImpactDashboard"
	}

	// B2C Bridge Path
	bridgePath := os.Getenv("B2C_BRIDGE_PATH")
	if bridgePath == "" {
		// Default relative path when running from services/data-ingestion/
		// (can be overridden via .env if needed)
		bridgePath = filepath.Join("..", "..", "b2c-api-python", "b2c_bridge.py")
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

	// Redis DB (parse as int)
	redisDB := 0
	if redisDBStr := os.Getenv("MARKET_REDIS_DB"); redisDBStr != "" {
		if parsed, err := strconv.Atoi(redisDBStr); err == nil {
			redisDB = parsed
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

	// Event-driven worker pool configuration
	workerPoolSize := 10 // Default: 10 concurrent workers
	if wps := os.Getenv("WORKER_POOL_SIZE"); wps != "" {
		if parsed, err := strconv.Atoi(wps); err == nil && parsed > 0 {
			workerPoolSize = parsed
		}
	}

	return &Config{
		// MongoDB
		MongoURI:            getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:       db,
		MongoCollection:     coll,
		MongoConnectTimeout: 10 * time.Second,

		// Kafka
		KafkaBrokers:         kafkaBrokers,
		KafkaTopic:           topicNews,
		KafkaTopic52Week:     breakoutTopic,
		KafkaTopicMarketData: topicMarketData,

		// Redis
		MarketRedisAddr:     getEnv("MARKET_REDIS_ADDR", "15.207.203.46:6379"),
		MarketRedisPassword: os.Getenv("MARKET_REDIS_PASSWORD"),
		MarketRedisDB:       redisDB,

		// B2C Bridge
		B2CBridgePath: bridgePath,
		B2CTokens:     tokens,
		// Default path assumes runtime from services/data-ingestion/; you
		// have already set the correct absolute path in .env, which will
		// override this.
		StocksDBPath: getEnv("STOCKS_DB_PATH", filepath.Join("..", "..", "stocks.db")),

		// Workers
		WorkerCount: workerCount,
		MaxRetries:  maxRetries,

		// Event-driven worker pool
		WorkerPoolSize: workerPoolSize,
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
