package config

import (
	"fmt"
	"log"
	"os"

	"github.com/RohitIndira/Algo-Treading/pkg/database/postgres"
)

// insecureDefaultEncryptionKey is the built-in placeholder AES key. It is a
// publicly-known value and MUST NOT be used to protect real broker tokens.
// Boot rejects it unless ALLOW_INSECURE_ENCRYPTION_KEY=true is set (local dev).
const insecureDefaultEncryptionKey = "0123456789abcdef0123456789abcdef"

// Config holds all configuration for the service
type Config struct {
	Server      ServerConfig
	Database    postgres.Config
	ExecutionDB postgres.Config // second connection → execution_db DB (for user_credentials)
	Kafka       KafkaConfig
	LogLevel    string
	EncryptionKey string
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port int
}

// KafkaConfig holds Kafka configuration
type KafkaConfig struct {
	Enabled bool
	Brokers []string
	Topic   string
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			Port: getEnvAsInt("GRPC_PORT", 50051),
		},
		Database: postgres.Config{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnvAsInt("DB_PORT", 5432),
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Database: getEnv("DB_NAME", "trading_db"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		ExecutionDB: postgres.Config{
			Host:     getEnv("EXECUTION_DB_HOST", "localhost"),
			Port:     getEnvAsInt("EXECUTION_DB_PORT", 5432),
			User:     getEnv("EXECUTION_DB_USER", "postgres"),
			Password: getEnv("EXECUTION_DB_PASSWORD", "postgres"),
			Database: getEnv("EXECUTION_DB_NAME", "execution_db"),
			SSLMode:  getEnv("EXECUTION_DB_SSLMODE", "disable"),
		},
		Kafka: KafkaConfig{
			Enabled: getEnvAsBool("KAFKA_ENABLED", true),
			Brokers: getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			Topic:   getEnv("KAFKA_TOPIC", "user-config-events"),
		},
		LogLevel:      getEnv("LOG_LEVEL", "INFO"),
		EncryptionKey: getEnv("ENCRYPTION_KEY", insecureDefaultEncryptionKey),
	}

	if err := validateEncryptionKey(cfg.EncryptionKey); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validateEncryptionKey enforces a usable, non-placeholder AES key at boot so
// the service hard-fails (via main's log.Fatal on Load error) rather than
// silently protecting real broker tokens with a weak/known key.
func validateEncryptionKey(key string) error {
	// 1) Length first — AES-128/192/256 require exactly 16/24/32 bytes. A
	//    short or empty key can never encrypt, so reject it regardless of the
	//    escape-hatch flag (checked before the placeholder check so a
	//    short-but-non-placeholder key still fails with a clear message).
	switch len(key) {
	case 16, 24, 32:
		// ok
	default:
		return fmt.Errorf("ENCRYPTION_KEY must be 16, 24, or 32 bytes (AES-128/192/256); got %d bytes", len(key))
	}

	// 2) Reject the built-in placeholder unless explicitly allowed for local
	//    dev. In production the key is public, so this would give zero at-rest
	//    protection.
	if key == insecureDefaultEncryptionKey {
		if getEnvAsBool("ALLOW_INSECURE_ENCRYPTION_KEY", false) {
			log.Printf("[user-config] WARN: insecure encryption key allowed via ALLOW_INSECURE_ENCRYPTION_KEY flag — DO NOT USE IN PRODUCTION")
			return nil
		}
		return fmt.Errorf("ENCRYPTION_KEY is the insecure built-in default; set a real 16/24/32-byte key, or set ALLOW_INSECURE_ENCRYPTION_KEY=true for local dev")
	}

	return nil
}

// Helper functions to read environment variables
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	var value int
	_, err := fmt.Sscanf(valueStr, "%d", &value)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	return valueStr == "true" || valueStr == "1" || valueStr == "yes"
	
}

func getEnvAsSlice(key string, defaultValue []string) []string {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	// Simple comma-separated parsing
	result := []string{}
	current := ""
	for _, char := range valueStr {
		if char == ',' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
