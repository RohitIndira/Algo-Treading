package config

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

func init() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}
}

// Config holds data-ingestion configuration
type Config struct {
	MongoURI        string
	MongoDatabase   string
	MongoCollection string

	KafkaBrokers []string
	KafkaTopic   string

	// Additional Kafka topic for 52-week high breakout events
	KafkaTopic52Week string

	// External Redis market data configuration (for 52-week highs, etc.).
	MarketRedisAddr         string
	MarketRedisPassword     string
	MarketRedisDB           int
	MarketRedisPollInterval time.Duration

	WorkerCount int
	MaxRetries  int
	// timeouts
	MongoConnectTimeout time.Duration
}

// Load loads configuration from environment variables with sensible defaults
func Load() *Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	topic := os.Getenv("KAFKA_TOPIC_NEWS")
	if topic == "" {
		// follow repo convention
		topic = "market.data.news"
	}

	// Separate topic for 52-week breakout events so downstream
	// consumers can subscribe independently of news.
	breakoutTopic := os.Getenv("KAFKA_TOPIC_52W_BREAKOUT")
	if breakoutTopic == "" {
		breakoutTopic = "market.data.52w_breakouts"
	}

	db := os.Getenv("MONGO_DATABASE")
	if db == "" {
		db = "CAG_CHATBOT"
	}

	coll := os.Getenv("MONGO_NEWS_COLLECTION")
	if coll == "" {
		coll = "NewsImpactDashboard"
	}

	workerCount := 4
	maxRetries := 3

	return &Config{
		MongoURI:                getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:           db,
		MongoCollection:         coll,
		KafkaBrokers:            strings.Split(brokers, ","),
		KafkaTopic:              topic,
		KafkaTopic52Week:        breakoutTopic,
		WorkerCount:             workerCount,
		MaxRetries:              maxRetries,
		MongoConnectTimeout:     10 * time.Second,
		MarketRedisAddr:         getEnv("MARKET_REDIS_ADDR", "65.20.83.31:6379"),
		MarketRedisPassword:     os.Getenv("MARKET_REDIS_PASSWORD"),
		MarketRedisDB:           0,
		MarketRedisPollInterval: 2 * time.Second,
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
