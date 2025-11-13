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

	db := os.Getenv("MONGO_DATABASE")
	if db == "" {
		db = "trading_system"
	}

	coll := os.Getenv("MONGO_NEWS_COLLECTION")
	if coll == "" {
		coll = "news_impact_dashboard"
	}

	workerCount := 4
	maxRetries := 3

	return &Config{
		MongoURI:            getEnv("MONGO_URI", ""),
		MongoDatabase:       db,
		MongoCollection:     coll,
		KafkaBrokers:        strings.Split(brokers, ","),
		KafkaTopic:          topic,
		WorkerCount:         workerCount,
		MaxRetries:          maxRetries,
		MongoConnectTimeout: 10 * time.Second,
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
