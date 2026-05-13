// Package logger builds a *zap.Logger configured the same way the other
// services use, so grep across log files stays consistent.
//
// One function: New(env). Pass "dev" for human-readable colourised output,
// anything else ("staging", "prod") for JSON output that ships cleanly to
// CloudWatch / Loki / whatever.
//
// We deliberately do NOT expose Set() or a package-level "global" logger —
// main.go creates one and passes it into every constructor. This makes
// tests easy (each test makes its own logger), and stops hidden coupling.
package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New returns a zap.Logger. env="dev" → colourful console output;
// anything else → structured JSON.
//
// Common fields you should always add at call sites:
//   zap.String("strategy_id", "...")
//   zap.String("symbol", "...")
//   zap.String("user_id", "...")
// so the per-strategy timeline can be reconstructed from logs alone.
func New(env string) *zap.Logger {
	if env == "dev" {
		cfg := zap.NewDevelopmentConfig()
		// Coloured level keys read better in a terminal.
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		// ISO timestamps instead of zap's default millisecond-since-epoch.
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		l, _ := cfg.Build()
		return l.Named("hft-engine")
	}

	// staging/prod — JSON, no colour, includes caller for stack-trace use.
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.TimeKey = "ts"
	l, _ := cfg.Build()
	return l.Named("hft-engine")
}
