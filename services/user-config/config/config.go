package config

import (
	"fmt"
	"os"

	"github.com/RohitIndira/Algo-Treading/pkg/database/postgres"
)

// Config holds all configuration for the service
type Config struct {
	Server   ServerConfig
	Database postgres.Config
	Kafka    KafkaConfig
	LogLevel string
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
		Kafka: KafkaConfig{
			Enabled: getEnvAsBool("KAFKA_ENABLED", true),
			Brokers: getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			Topic:   getEnv("KAFKA_TOPIC", "user-config-events"),
		},
		LogLevel: getEnv("LOG_LEVEL", "INFO"),
	}

	return cfg, nil
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
