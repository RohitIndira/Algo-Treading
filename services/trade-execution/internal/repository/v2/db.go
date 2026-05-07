package v2

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// NewGormDB creates a new GORM database connection using environment variables.
func NewGormDB() (*gorm.DB, error) {
	dsn := buildDSN()

	logLevel := logger.Warn
	if os.Getenv("ENVIRONMENT") == "development" {
		logLevel = logger.Info
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:                 logger.Default.LogMode(logLevel),
		SkipDefaultTransaction: true, // Better performance: no implicit tx per query
		PrepareStmt:            true, // Cache prepared statements
	})
	if err != nil {
		return nil, fmt.Errorf("gorm.Open: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("db.DB(): %w", err)
	}

	maxOpen := envInt("MAX_OPEN_CONNS", 120)
	maxIdle := envInt("MAX_IDLE_CONNS", 60)

	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("db ping: %w", err)
	}

	log.Printf("[gorm] Connected to PostgreSQL (max_open=%d, max_idle=%d)", maxOpen, maxIdle)
	return db, nil
}

func buildDSN() string {
	host := envStr("POSTGRES_HOST", "localhost")
	port := envStr("POSTGRES_PORT", "5432")
	user := envStr("POSTGRES_USER", "trading_user")
	pass := envStr("POSTGRES_PASSWORD", "")
	dbname := envStr("POSTGRES_DB", "trading_execution")
	sslmode := envStr("POSTGRES_SSLMODE", "disable")

	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, pass, dbname, sslmode,
	)
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
