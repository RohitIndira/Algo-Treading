package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Server   ServerConfig
	Services ServicesConfig
	CORS     CORSConfig
	Logging  LoggingConfig
}

type ServerConfig struct {
	HTTPPort    int
	GRPCTimeout time.Duration
}

type ServicesConfig struct {
	UserConfigAddr      string
	UserLoginServiceURL string
}

type CORSConfig struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
}

type LoggingConfig struct {
	Level string
}

func Load() (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load()

	httpPort, _ := strconv.Atoi(getEnv("HTTP_PORT", "8080"))
	grpcTimeout, _ := time.ParseDuration(getEnv("GRPC_TIMEOUT", "30s"))

	return &Config{
		Server: ServerConfig{
			HTTPPort:    httpPort,
			GRPCTimeout: grpcTimeout,
		},
		Services: ServicesConfig{
			UserConfigAddr:      getEnv("USER_CONFIG_GRPC_ADDR", "localhost:50051"),
			UserLoginServiceURL: getEnv("USER_LOGIN_SERVICE_URL", "http://localhost:8002"),
		},
		CORS: CORSConfig{
			AllowedOrigins: strings.Split(getEnv("CORS_ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173"), ","),
			AllowedMethods: strings.Split(getEnv("CORS_ALLOWED_METHODS", "GET,POST,PUT,DELETE,OPTIONS"), ","),
			AllowedHeaders: strings.Split(getEnv("CORS_ALLOWED_HEADERS", "Content-Type,Authorization"), ","),
		},
		Logging: LoggingConfig{
			Level: getEnv("LOG_LEVEL", "INFO"),
		},
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
