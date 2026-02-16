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
	RedisURI      string
	RedisPassword string
	RedisDB       int

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
		db = "CAG_CHATBOT"
	}

	coll := os.Getenv("MONGO_NEWS_COLLECTION")
	if coll == "" {
		coll = "NewsImpactDashboard"
	}

	redisURI := os.Getenv("REDIS_URI")
	if redisURI == "" {
		redisURI = "localhost:6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	workerCount := 4
	maxRetries := 3

	return &Config{
		MongoURI:            getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:       db,
		MongoCollection:     coll,
		RedisURI:            redisURI,
		RedisPassword:       redisPassword,
		RedisDB:             0, // Default DB
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
