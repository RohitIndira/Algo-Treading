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
	Auth     AuthConfig
	Logging  LoggingConfig
}

type ServerConfig struct {
	HTTPPort    int
	GRPCTimeout time.Duration
}

type ServicesConfig struct {
	UserConfigAddr         string
	TradeExecutionPaperURL string
	RulesEngineURL         string
}

type CORSConfig struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
}

type AuthConfig struct {
	VerifyURL string
	Timeout   time.Duration
	Bypass    bool // skip external verify call (UAT / dev)
}

type LoggingConfig struct {
	Level string
}

func Load() (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load()

	httpPort, _ := strconv.Atoi(getEnv("HTTP_PORT", "8081"))
	grpcTimeout, _ := time.ParseDuration(getEnv("GRPC_TIMEOUT", "30s"))

	authTimeout, _ := time.ParseDuration(getEnv("AUTH_TIMEOUT", "5s"))

	return &Config{
		Server: ServerConfig{
			HTTPPort:    httpPort,
			GRPCTimeout: grpcTimeout,
		},
		Services: ServicesConfig{
			UserConfigAddr:         getEnv("USER_CONFIG_GRPC_ADDR", "localhost:50051"),
			TradeExecutionPaperURL: getEnv("TRADE_EXECUTION_PAPER_URL", "http://localhost:8081"),
			RulesEngineURL:         getEnv("RULES_ENGINE_URL", "http://localhost:8082"),
		},
		CORS: CORSConfig{
			AllowedOrigins: strings.Split(getEnv("CORS_ALLOWED_ORIGINS", "*"), ","),
			AllowedMethods: strings.Split(getEnv("CORS_ALLOWED_METHODS", "GET,POST,PUT,DELETE,OPTIONS"), ","),
			AllowedHeaders: strings.Split(getEnv("CORS_ALLOWED_HEADERS", "Content-Type,Authorization,source,appId,userId"), ","),
		},
		Auth: AuthConfig{
			VerifyURL: getEnv("AUTH_VERIFY_URL", "https://trade.indiratrade.com/auth-services/api/auth/verify/token"),
			Timeout:   authTimeout,
			Bypass:    getEnv("AUTH_BYPASS", "false") == "true" || strings.Contains(getEnv("AUTH_VERIFY_URL", ""), "trade.indiratrade.com"),
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
