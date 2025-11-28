package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger wraps zap.Logger with additional functionality
type Logger struct {
	*zap.Logger
}

// Config holds logger configuration
type Config struct {
	Environment string // "development", "staging", "production"
	Level       string // "debug", "info", "warn", "error"
	ServiceName string
}

// New creates a new logger instance based on the environment
func New(config Config) (*Logger, error) {
	var zapConfig zap.Config

	// Set base configuration based on environment
	switch config.Environment {
	case "production":
		zapConfig = zap.NewProductionConfig()
		zapConfig.EncoderConfig.TimeKey = "timestamp"
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	case "staging":
		zapConfig = zap.NewProductionConfig()
		zapConfig.EncoderConfig.TimeKey = "timestamp"
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	default: // development
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	// Set log level
	level := parseLevel(config.Level)
	zapConfig.Level = zap.NewAtomicLevelAt(level)

	// Add service name to all logs
	zapConfig.InitialFields = map[string]interface{}{
		"service": config.ServiceName,
	}

	// Build the logger
	zapLogger, err := zapConfig.Build(
		zap.AddCallerSkip(1), // Skip one level to show correct caller
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return nil, err
	}

	return &Logger{Logger: zapLogger}, nil
}

// NewWithDefaults creates a logger with default settings
func NewWithDefaults(serviceName string) (*Logger, error) {
	env := os.Getenv("ENVIRONMENT")
	if env == "" {
		env = "development"
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return New(Config{
		Environment: env,
		Level:       logLevel,
		ServiceName: serviceName,
	})
}

// parseLevel converts string level to zapcore.Level
func parseLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

// WithFields adds structured fields to the logger
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	return &Logger{Logger: l.With(zapFields...)}
}

// WithError adds an error field to the logger
func (l *Logger) WithError(err error) *Logger {
	return &Logger{Logger: l.With(zap.Error(err))}
}

// WithRequestID adds a request ID to the logger
func (l *Logger) WithRequestID(requestID string) *Logger {
	return &Logger{Logger: l.With(zap.String("request_id", requestID))}
}

// WithUserID adds a user ID to the logger
func (l *Logger) WithUserID(userID string) *Logger {
	return &Logger{Logger: l.With(zap.String("user_id", userID))}
}

// WithComponent adds a component name to the logger
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{Logger: l.With(zap.String("component", component))}
}

// Sync flushes any buffered log entries
func (l *Logger) Sync() error {
	return l.Logger.Sync()
}
