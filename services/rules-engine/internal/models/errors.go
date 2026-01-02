package models

import "errors"

// Common errors
var (
	// Event validation errors
	ErrInvalidEventID     = errors.New("invalid event ID")
	ErrInvalidEventType   = errors.New("invalid event type")
	ErrInvalidStockCode   = errors.New("invalid stock code")
	ErrInvalidExchange    = errors.New("invalid exchange")
	ErrMissingMarketDepth = errors.New("missing market depth data (bid/ask prices and quantities required)")

	// Strategy validation errors
	ErrInvalidStrategyID = errors.New("invalid strategy ID")
	ErrInvalidUserID     = errors.New("invalid user ID")
	ErrInvalidQuantity   = errors.New("invalid quantity")
	ErrInvalidOrderType  = errors.New("invalid order type")

	// Order validation errors
	ErrInvalidOrderID = errors.New("invalid order ID")
	ErrInvalidPrice   = errors.New("invalid price")

	// Processing errors
	ErrNoMatchFound       = errors.New("no match found")
	ErrRiskCheckFailed    = errors.New("risk check failed")
	ErrPublishFailed      = errors.New("failed to publish order")
	ErrCacheMiss          = errors.New("cache miss")
	ErrElasticsearchQuery = errors.New("elasticsearch query failed")
)
