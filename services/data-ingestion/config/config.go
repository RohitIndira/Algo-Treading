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
	// Load .env file (best effort).
	//
	// We try multiple locations to avoid a common dev pitfall:
	// running the binary from a different working directory.
	//
	// Priority:
	//  1) ./.env               (current working dir)
	//  2) <serviceRoot>/.env   (parent of ./bin)
	//
	// If nothing is found, we continue with OS env vars + defaults.
	pathsToTry := []string{}

	if wd, err := os.Getwd(); err == nil {
		pathsToTry = append(pathsToTry, filepath.Join(wd, ".env"))
		// Repo-root case: when running `go run ./services/data-ingestion/cmd/...`
		// from the repo root, CWD is the repo root and the service .env lives
		// two levels deeper. Try that path too so MongoDB / Redis / Kafka
		// settings come through without requiring a cd into the service dir.
		pathsToTry = append(pathsToTry, filepath.Join(wd, "services", "data-ingestion", ".env"))
	}

	if exe, err := os.Executable(); err == nil {
		// exe is typically .../services/data-ingestion/bin/data-ingestion
		// service root is one level up from bin
		serviceRoot := filepath.Dir(filepath.Dir(exe))
		pathsToTry = append(pathsToTry, filepath.Join(serviceRoot, ".env"))
	}

	loaded := false
	for _, p := range pathsToTry {
		if err := godotenv.Overload(p); err == nil {
			log.Printf("Loaded .env from %s", p)
			loaded = true
			break
		}
	}

	if !loaded {
		log.Println("No .env file found in expected locations, using environment variables")
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

	// PostgreSQL (market_data database for historical OHLCV)
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDatabase string
	PGSSLMode  string
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
		topic = "news-events"
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

	pgPort, _ := strconv.Atoi(getEnv("MARKET_DATA_DB_PORT", "5432"))

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
		PGHost:              getEnv("MARKET_DATA_DB_HOST", "localhost"),
		PGPort:              pgPort,
		PGUser:              getEnv("MARKET_DATA_DB_USER", "postgres"),
		PGPassword:          getEnv("MARKET_DATA_DB_PASSWORD", "postgres"),
		PGDatabase:          getEnv("MARKET_DATA_DB_NAME", "market_data"),
		PGSSLMode:           getEnv("MARKET_DATA_DB_SSLMODE", "disable"),
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
