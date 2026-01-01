package config

import (
	"log"
	"os"
	"path/filepath"
	"strings"

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
	B2CBridgePath string
	B2CTokens     []string

	KafkaBrokers []string
	KafkaTopic   string

	WorkerCount int
	MaxRetries  int
}

// Load loads configuration from environment variables with sensible defaults
func Load() *Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	topic := os.Getenv("KAFKA_TOPIC_MARKET_DATA")
	if topic == "" {
		// follow repo convention for live market data
		topic = "market.data.live"
	}

	bridgePath := os.Getenv("B2C_BRIDGE_PATH")
	if bridgePath == "" {
		// Get the directory where this service is running from
		serviceDir := filepath.Join("..","..", "..", "b2c-api-python", "b2c_bridge.py")
		bridgePath = serviceDir
	}

	// B2C tokens as comma-separated list
	tokensStr := getEnv("B2C_TOKENS", "22,2475,25,14366") // default tokens for testing
	var tokens []string
	if tokensStr != "" {
		tokens = strings.Split(tokensStr, ",")
		// Trim whitespace from each token
		for i := range tokens {
			tokens[i] = strings.TrimSpace(tokens[i])
		}
	}

	workerCount := 4
	maxRetries := 3

	return &Config{
		B2CBridgePath: bridgePath,
		B2CTokens:     tokens,
		KafkaBrokers:  strings.Split(brokers, ","),
		KafkaTopic:    topic,
		WorkerCount:   workerCount,
		MaxRetries:    maxRetries,
	}
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
