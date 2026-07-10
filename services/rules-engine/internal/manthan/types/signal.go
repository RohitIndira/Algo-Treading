// Package types holds the shared data structures used across the Manthan
// subsystem — signals, positions, allocation, and orders.
//
// This package is IMPORTS-ONLY-STDLIB: it must never import from any
// internal/manthan/* subpackage. That constraint is what allows signals/
// and positions/ sub-packages to both import types/ without cycles.
//
// Types are grouped by domain across a few files:
//   signal.go     — inputs to the signal engine
//   position.go   — position lifecycle state
//   allocation.go — allocator inputs, outputs, and cap-check helpers
package types

// ManthanSignal represents one eligible stock from the data-ingestion pipeline
// (consumed from manthan.signals Kafka topic).
type ManthanSignal struct {
	RunDate     string  `json:"run_date"`
	Symbol      string  `json:"symbol"`
	ISIN        string  `json:"isin"`
	Industry    string  `json:"industry"`
	MCapBucket  string  `json:"mcap_bucket"` // LARGE, MID, SMALL
	IndexName   string  `json:"index_name"`  // NIFTY50, NFTYMCP150, NTYSLCP250
	MarketCap   float64 `json:"market_cap"`
	PE          float64 `json:"pe"`
	FScore      float64 `json:"fscore"`
	PAT         float64 `json:"pat"`
	LatestPrice float64 `json:"latest_price"`
	ATHClose    float64 `json:"ath_close"`
	Week52High  float64 `json:"week52_high"`
	EmittedAt   string  `json:"emitted_at"`
}

// UserStrategy holds a user's MANTHAN strategy config (from user-config-events).
//
// Auth fields (BearerToken/AppId/Source) were removed 2026-06-25 — broker
// credentials are now fetched at-edge by trade-execution via user-config gRPC
// instead of flowing through this in-memory model and onto the Kafka wire.
type UserStrategy struct {
	StrategyID    string
	UserID        string
	StrategyName  string
	TradingMode   string // PAPER or LIVE
	TotalCapital  float64
	MaxPositions  int32
	PerStockBase  float64 // TotalCapital / MaxPositions
	StopLossPct   float64 // 20
	TrailingSLPct float64 // 2
}
