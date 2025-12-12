package config

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCPort string

	RedisHost string
	RedisPort string

	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string

	// Defaults
	MaxDailyTrades        int
	MaxDailyLoss          float64
	MaxPositionSize       float64
	MaxPerTradeRisk       float64
	MaxPortfolioExposure  float64
	MaxConcentrationPct   float64
	CircuitBreakerLossPct float64
}

func LoadConfig() *Config {
	return &Config{
		GRPCPort: getEnv("GRPC_PORT", "9005"),

		RedisHost: getEnv("REDIS_HOST", "65.20.83.31"),
		RedisPort: getEnv("REDIS_PORT", "6379"),

		DBHost: getEnv("DB_HOST", "localhost"),
		DBPort: getEnv("DB_PORT", "5432"),
		DBName: getEnv("DB_NAME", "trading_db"),
		DBUser: getEnv("DB_USER", "postgres"),

		MaxDailyTrades:        getEnvInt("MAX_DAILY_TRADES", 100),
		MaxDailyLoss:          getEnvFloat("MAX_DAILY_LOSS", 10000.0),
		MaxPositionSize:       getEnvFloat("MAX_POSITION_SIZE", 50000.0),
		MaxPerTradeRisk:       getEnvFloat("MAX_PER_TRADE_RISK", 2000.0),
		MaxPortfolioExposure:  getEnvFloat("MAX_PORTFOLIO_EXPOSURE_PCT", 50.0),
		MaxConcentrationPct:   getEnvFloat("MAX_CONCENTRATION_PCT", 30.0),
		CircuitBreakerLossPct: getEnvFloat("CIRCUIT_BREAKER_LOSS_PCT", 5.0),
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
