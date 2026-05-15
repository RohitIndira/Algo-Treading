// Package config loads all settings from environment variables into a
// single typed struct. Every other package gets a *Config passed in by
// main.go — no package calls os.Getenv() directly.
//
// Why one place: when a setting changes name or default, you fix it here
// once. When ops asks "what env vars does this service need?", point them
// at Load() and you're done.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every runtime setting the hft-engine needs.
// Fields are grouped by what they control.
type Config struct {
	// ── Process identity ──────────────────────────────────────────────
	ServiceName string // for logs / metrics tags. Default: "hft-engine"
	Env         string // "dev" | "staging" | "prod" — affects log verbosity

	// ── HTTP / gRPC ports ─────────────────────────────────────────────
	GRPCPort int // hft-engine's own gRPC listener. Default 9090.
	HTTPPort int // optional health-probe HTTP listener (livez/readyz). Default 9091.

	// ── trading_execution DB (shared with trade-execution) ────────────
	// hft-engine writes audit rows + reads broker creds from this DB.
	TradingExecDB DBConfig

	// ── trading_db (shared with rules-engine) ─────────────────────────
	// hft-engine reads strategy configs from this DB.
	TradingDB DBConfig

	// ── Redis (per-strategy state snapshots, used in Phase 7) ─────────
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// ── External services hft-engine depends on ───────────────────────
	IndiraBaseURL string // e.g. https://livemiddleware.indiratrade.com

	// ── ODIN broadcast feed (market data, read-only) ──────────────────
	OdinBcastHost   string // e.g. indirabcast.odin.trade
	OdinBcastPort   int    // e.g. 4515
	OdinBcastScheme string // "wss" (default) or "ws"
	OdinBcastUserID string // fill from env at deploy time
	OdinBcastAPIKey string // fill from env at deploy time

	// ── External (Indira) Redis — holds isin:{ISIN} symbol metadata ───
	// Reused with data-ingestion; same EXT_REDIS_ADDR/PASSWORD env vars.
	ExtRedisAddr     string
	ExtRedisPassword string
	ExtRedisDB       int

	// ── Defaults applied to strategies when their config doesn't set them
	DefaultMode string // "PAPER" or "LIVE" — engine-wide safety default

	// EncryptionKey is the AES key used to decrypt indira_bearer_token from
	// the user_credentials table. user-config and trade-execution store the
	// broker JWT *encrypted* (pkg/crypto); the hft-engine must decrypt it
	// with the SAME key or it hands Indira ciphertext and gets 401s.
	EncryptionKey string
}

// DBConfig is one Postgres connection's params.
// Tucked into its own type because the engine connects to TWO databases.
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
}

// DSN returns the Postgres connection string for sql.Open("postgres", dsn).
func (d DBConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode,
	)
}

// Load reads env vars and returns a Config. It NEVER reads .env files
// directly — that's main.go's job (via godotenv) before calling Load. This
// keeps Load testable: tests just set env vars in code.
//
// Defaults are chosen to match docker-compose dev so a fresh `go run`
// works without any .env at all.
func Load() *Config {
	return &Config{
		ServiceName: env("HFT_SERVICE_NAME", "hft-engine"),
		Env:         env("HFT_ENV", "dev"),

		GRPCPort: envInt("HFT_GRPC_PORT", 9090),
		HTTPPort: envInt("HFT_HTTP_PORT", 9091),

		TradingExecDB: DBConfig{
			Host:     env("POSTGRES_HOST", "localhost"),
			Port:     envInt("POSTGRES_PORT", 5432),
			User:     env("POSTGRES_USER", "postgres"),
			Password: env("POSTGRES_PASSWORD", "postgres"),
			Name:     env("TRADING_EXEC_DB", "trading_execution"),
			SSLMode:  env("POSTGRES_SSL_MODE", "disable"),
		},
		TradingDB: DBConfig{
			Host:     env("POSTGRES_HOST", "localhost"),
			Port:     envInt("POSTGRES_PORT", 5432),
			User:     env("POSTGRES_USER", "postgres"),
			Password: env("POSTGRES_PASSWORD", "postgres"),
			Name:     env("TRADING_DB", "trading_db"),
			SSLMode:  env("POSTGRES_SSL_MODE", "disable"),
		},

		RedisAddr:     env("REDIS_ADDR", env("REDIS_URI", "localhost:6379")),
		RedisPassword: env("REDIS_PASSWORD", ""),
		RedisDB:       envInt("REDIS_DB", 0),

		IndiraBaseURL: env("INDIRA_BASE_URL", "https://livemiddleware.indiratrade.com"),

		// ODIN broadcast — defaults come from the Python reference client.
		// USER_ID + API_KEY must be supplied via env at deploy time.
		OdinBcastHost:   env("ODIN_BCAST_HOST", "indirabcast.odin.trade"),
		OdinBcastPort:   envInt("ODIN_BCAST_PORT", 4515),
		OdinBcastScheme: env("ODIN_BCAST_SCHEME", "wss"),
		OdinBcastUserID: env("ODIN_BCAST_USER_ID", ""),
		OdinBcastAPIKey: env("ODIN_BCAST_API_KEY", ""),

		// External Indira Redis (Manthan's data-ingestion writes isin:{ISIN}
		// blobs here; we read them for ISIN→ODIN-token resolution).
		ExtRedisAddr:     env("EXT_REDIS_ADDR", env("EXT_REDIS_URI", "15.207.203.46:6379")),
		ExtRedisPassword: env("EXT_REDIS_PASSWORD", ""),
		ExtRedisDB:       envInt("EXT_REDIS_DB", 0),

		// Safety: default PAPER so a fresh deploy or a strategy that forgot
		// to set its own `mode` never accidentally places real orders.
		DefaultMode: env("HFT_DEFAULT_MODE", "PAPER"),

		// Must match user-config / trade-execution's ENCRYPTION_KEY — the
		// broker JWT in user_credentials is AES-encrypted at rest.
		EncryptionKey: env("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef"),
	}
}

// ── helpers ───────────────────────────────────────────────────────────────

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// envDuration parses values like "30s", "5m". Kept here for future use.
func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
