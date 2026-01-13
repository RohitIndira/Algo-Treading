// package config

// import (
// 	"fmt"
// 	"log"
// 	"os"
// 	"strings"
// 	"time"

// 	"github.com/joho/godotenv"
// )

// /*
// 	func init() {
// 		// Load .env file if it exists
// 		if err := godotenv.Load(); err != nil {
// 			log.Println("No .env file found, using environment variables")
// 		}

// }
// */
// func init() {
// 	cwd, _ := os.Getwd()
// 	log.Println("Current working directory:", cwd)

// 	if err := godotenv.Load(); err != nil {
// 		log.Println("--------->No .env file found, using environment variables")
// 	}
// }

// // Config holds data-ingestion configuration
// type Config struct {
// 	MongoURI        string
// 	MongoDatabase   string
// 	MongoCollection string

// 	KafkaBrokers []string
// 	KafkaTopic   string

// 	// Additional Kafka topic for 52-week high breakout events
// 	KafkaTopic52Week string

// 	// External Redis market data configuration (for 52-week highs, etc.).
// 	MarketRedisAddr         string
// 	MarketRedisPassword     string
// 	MarketRedisDB           int
// 	MarketRedisPollInterval time.Duration

// 	WorkerCount int
// 	MaxRetries  int
// 	// timeouts
// 	MongoConnectTimeout time.Duration
// }

// // Load loads configuration from environment variables with sensible defaults
// func Load() *Config {
// 	fmt.Println("In the Load function of config.go file in data-ingestion service")

// 	brokers := os.Getenv("KAFKA_BROKERS")
// 	if brokers == "" {
// 		brokers = "localhost:9092"
// 	}

// 	topic := os.Getenv("KAFKA_TOPIC_NEWS")
// 	if topic == "" {
// 		// follow repo convention
// 		topic = "market.data.news"
// 	}

// 	// Separate topic for 52-week breakout events so downstream
// 	// consumers can subscribe independently of news.
// 	breakoutTopic := os.Getenv("KAFKA_TOPIC_52W_BREAKOUT")
// 	if breakoutTopic == "" {
// 		breakoutTopic = "market.data.52w_breakouts"
// 	}

// 	db := os.Getenv("MONGO_DATABASE")
// 	if db == "" {
// 		db = "CAG_CHATBOT"
// 	}

// 	coll := os.Getenv("MONGO_NEWS_COLLECTION")
// 	if coll == "" {
// 		coll = "NewsImpactDashboard"
// 	}

// 	workerCount := 4
// 	maxRetries := 3

// 	return &Config{
// 		MongoURI:                getEnv("MONGO_URI", "mongodb://localhost:27017"),
// 		MongoDatabase:           db,
// 		MongoCollection:         coll,
// 		KafkaBrokers:            strings.Split(brokers, ","),
// 		KafkaTopic:              topic,
// 		KafkaTopic52Week:        breakoutTopic,
// 		WorkerCount:             workerCount,
// 		MaxRetries:              maxRetries,
// 		MongoConnectTimeout:     10 * time.Second,
// 		MarketRedisAddr:         getEnv("MARKET_REDIS_ADDR", "65.20.83.31:6379"),
// 		MarketRedisPassword:     os.Getenv("MARKET_REDIS_PASSWORD"),
// 		MarketRedisDB:           0,
// 		MarketRedisPollInterval: 2 * time.Second,
// 	}
// }

// func getEnv(key, def string) string {
// 	v := os.Getenv(key)
// 	if v == "" {
// 		return def
// 	}
// 	return v
// }

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
	MarketRedisAddr         string
	MarketRedisPassword     string
	MarketRedisDB           int
	MarketRedisPollInterval time.Duration

	// B2C API Bridge configuration
	B2CBridgePath string
	B2CTokens     []string

	// Worker configuration
	WorkerCount int
	MaxRetries  int

	// Timeouts
	MongoConnectTimeout time.Duration
}

// Load loads configuration from environment variables with sensible defaults
func Load() *Config {
	fmt.Println("In the Load function of config.go file in data-ingestion service")

	// Kafka Brokers
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

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
		serviceDir := filepath.Join("..", "..", "..", "b2c-api-python", "b2c_bridge.py")
		bridgePath = serviceDir
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

	return &Config{
		// MongoDB
		MongoURI:            getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:       db,
		MongoCollection:     coll,
		MongoConnectTimeout: 10 * time.Second,

		// Kafka
		KafkaBrokers:         strings.Split(brokers, ","),
		KafkaTopic:           topicNews,
		KafkaTopic52Week:     breakoutTopic,
		KafkaTopicMarketData: topicMarketData,

		// Redis
		MarketRedisAddr:         getEnv("MARKET_REDIS_ADDR", "65.20.83.31:6379"),
		MarketRedisPassword:     os.Getenv("MARKET_REDIS_PASSWORD"),
		MarketRedisDB:           redisDB,
		MarketRedisPollInterval: 2 * time.Second,

		// B2C Bridge
		B2CBridgePath: bridgePath,
		B2CTokens:     tokens,

		// Workers
		WorkerCount: workerCount,
		MaxRetries:  maxRetries,
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
