package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger wraps zap.Logger with additional functionality
type Logger struct {
	*zap.Logger
	writer *DateRotatingWriter // kept to propagate to child loggers
}

// Config holds logger configuration
type Config struct {
	Environment string // "development", "staging", "production"
	Level       string // "debug", "info", "warn", "error", "fatal"
	ServiceName string
	// LogDir is the base directory for date-based log files.
	// Files are written to: <LogDir>/<YYYY-MM-DD>/<ServiceName>.log
	// Set to "" to disable file logging (console only).
	LogDir string
}

// ---------------------------------------------------------------------------
// DateRotatingWriter
// ---------------------------------------------------------------------------

// DateRotatingWriter is a zapcore.WriteSyncer that writes to a new file each
// calendar day.  Directory layout:
//
//	<LogDir>/
//	  2026-03-09/
//	    api-gateway.log
//	    user-config-service.log
//	  2026-03-10/
//	    api-gateway.log
//	    ...
type DateRotatingWriter struct {
	mu          sync.Mutex
	logDir      string
	serviceName string
	currentDate string
	file        *os.File
}

// NewDateRotatingWriter creates the writer and opens today's log file.
func NewDateRotatingWriter(logDir, serviceName string) (*DateRotatingWriter, error) {
	w := &DateRotatingWriter{
		logDir:      logDir,
		serviceName: serviceName,
	}
	if err := w.rotate(); err != nil {
		return nil, err
	}
	return w, nil
}

// rotate opens (or re-opens) the log file for today, creating the dated
// sub-directory if it does not yet exist.  Must be called with mu held or
// before the writer is shared across goroutines.
func (w *DateRotatingWriter) rotate() error {
	today := time.Now().Format("2006-01-02")
	if today == w.currentDate && w.file != nil {
		return nil // still the same day — nothing to do
	}

	// Create the dated directory, e.g. logs/2026-03-09/
	dir := filepath.Join(w.logDir, today)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("logger: create log directory %q: %w", dir, err)
	}

	// Open / create the service log file in append mode.
	filePath := filepath.Join(dir, w.serviceName+".log")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("logger: open log file %q: %w", filePath, err)
	}

	// Close the previous day's file before switching over.
	if w.file != nil {
		_ = w.file.Close()
	}

	w.file = f
	w.currentDate = today
	return nil
}

// Write implements io.Writer.  It rotates the file automatically when the
// calendar date changes.
func (w *DateRotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.rotate(); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}

// Sync implements zapcore.WriteSyncer.
func (w *DateRotatingWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

// Close closes the underlying file and releases its resources.
func (w *DateRotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// CurrentLogPath returns the absolute path of the file being written to right now.
func (w *DateRotatingWriter) CurrentLogPath() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Name()
	}
	return ""
}

// ---------------------------------------------------------------------------
// Logger construction
// ---------------------------------------------------------------------------

// New creates a fully configured logger.
//
// Console output:
//   - development/staging  → coloured, human-readable (console encoder)
//   - production           → structured JSON
//
// File output (when config.LogDir != ""):
//   - always structured JSON for easy parsing / ingestion
//   - written to  <LogDir>/<YYYY-MM-DD>/<ServiceName>.log
func New(config Config) (*Logger, error) {
	level := parseLevel(config.Level)
	atomicLevel := zap.NewAtomicLevelAt(level)

	// ── Console encoder ─────────────────────────────────────────────────────
	var consoleEncoder zapcore.Encoder
	if config.Environment == "production" {
		prodCfg := zap.NewProductionEncoderConfig()
		prodCfg.TimeKey = "timestamp"
		prodCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		consoleEncoder = zapcore.NewJSONEncoder(prodCfg)
	} else {
		devCfg := zap.NewDevelopmentEncoderConfig()
		devCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		consoleEncoder = zapcore.NewConsoleEncoder(devCfg)
	}

	// ── File encoder (always JSON) ───────────────────────────────────────────
	fileCfg := zap.NewProductionEncoderConfig()
	fileCfg.TimeKey = "timestamp"
	fileCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	fileEncoder := zapcore.NewJSONEncoder(fileCfg)

	// ── Build cores ──────────────────────────────────────────────────────────
	cores := []zapcore.Core{
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), atomicLevel),
	}

	var rotatingWriter *DateRotatingWriter
	if config.LogDir != "" {
		var err error
		rotatingWriter, err = NewDateRotatingWriter(config.LogDir, config.ServiceName)
		if err != nil {
			return nil, fmt.Errorf("logger: init date-rotating writer: %w", err)
		}
		cores = append(cores, zapcore.NewCore(fileEncoder, rotatingWriter, atomicLevel))
	}

	core := zapcore.NewTee(cores...)

	zapLogger := zap.New(core,
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zapcore.ErrorLevel),
		zap.Fields(zap.String("service", config.ServiceName)),
	)

	return &Logger{Logger: zapLogger, writer: rotatingWriter}, nil
}

// NewWithDefaults creates a logger driven by environment variables:
//
//	ENVIRONMENT  – development (default) | staging | production
//	LOG_LEVEL    – debug | info (default) | warn | error | fatal
//	LOG_DIR      – base log directory (default: "logs")
//
// Log files land at  <LOG_DIR>/<YYYY-MM-DD>/<serviceName>.log
func NewWithDefaults(serviceName string) (*Logger, error) {
	env := os.Getenv("ENVIRONMENT")
	if env == "" {
		env = "development"
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}

	return New(Config{
		Environment: env,
		Level:       logLevel,
		ServiceName: serviceName,
		LogDir:      logDir,
	})
}

// ---------------------------------------------------------------------------
// Helper constructors
// ---------------------------------------------------------------------------

// WithFields adds structured fields to the logger.
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	return &Logger{Logger: l.With(zapFields...), writer: l.writer}
}

// WithError adds an error field to the logger.
func (l *Logger) WithError(err error) *Logger {
	return &Logger{Logger: l.With(zap.Error(err)), writer: l.writer}
}

// WithRequestID adds a request_id field to the logger.
func (l *Logger) WithRequestID(requestID string) *Logger {
	return &Logger{Logger: l.With(zap.String("request_id", requestID)), writer: l.writer}
}

// WithUserID adds a user_id field to the logger.
func (l *Logger) WithUserID(userID string) *Logger {
	return &Logger{Logger: l.With(zap.String("user_id", userID)), writer: l.writer}
}

// WithComponent adds a component field to the logger.
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{Logger: l.With(zap.String("component", component)), writer: l.writer}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Sync flushes any buffered log entries.
func (l *Logger) Sync() error {
	return l.Logger.Sync()
}

// Close flushes and closes the logger, including the underlying rotating file
// writer.  Call this in your service's graceful-shutdown path.
func (l *Logger) Close() error {
	_ = l.Logger.Sync()
	if l.writer != nil {
		return l.writer.Close()
	}
	return nil
}

// CurrentLogPath returns the absolute path of the active log file, or an
// empty string when file logging is disabled.
func (l *Logger) CurrentLogPath() string {
	if l.writer != nil {
		return l.writer.CurrentLogPath()
	}
	return ""
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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
