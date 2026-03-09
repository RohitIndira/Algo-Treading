package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCPort string

	IndiraBaseURL string
	HTTPTimeout   time.Duration

	IndiraWSSURL string

	WSSInitialBackoff time.Duration
	WSSMaxBackoff     time.Duration

	WSSEventBufSize int
}

func Load() *Config {
	return &Config{
		// NOTE: 50052 is already used by data-ingestion in root docker-compose.
		// Use 50056 by default for indira-wrapper to avoid host port collisions.
		GRPCPort:      getEnv("GRPC_PORT", "50056"),
		IndiraBaseURL: getEnv("INDIRA_BASE_URL", "https://livemiddleware.indiratrade.com"),
		HTTPTimeout:   800 * time.Millisecond,
		IndiraWSSURL:  getEnv("INDIRA_WSS_URL", ""),

		WSSInitialBackoff: 1 * time.Second,
		WSSMaxBackoff:     30 * time.Second,
		WSSEventBufSize:   getEnvInt("WSS_EVENT_BUF_SIZE", 10000),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
