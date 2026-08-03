package manthan

import "time"

// OrderType classifies the kind of broker order.
type OrderType string

const (
	OrderTypeLimitBuy   OrderType = "LIMIT_BUY"
	OrderTypeMarketBuy  OrderType = "MARKET_BUY" // guaranteed-fill topup emitted by entry_handler.handlePartialFill
	OrderTypeSLSell     OrderType = "SL_SELL"
	OrderTypeSLSellAMO  OrderType = "SL_SELL_AMO" // pending AMO+SL row from protective replayer; promoted to SL_SELL after Phase C conversion swap
	OrderTypeMarketSell OrderType = "MARKET_SELL"
	OrderTypeAMOSell    OrderType = "AMO_SELL"
	OrderTypeSLModify   OrderType = "SL_MODIFY"
)

// OrderStatus tracks the lifecycle of an order.
type OrderStatus string

const (
	StatusPending          OrderStatus = "PENDING"
	StatusPlaced           OrderStatus = "PLACED"
	StatusFilled           OrderStatus = "FILLED"
	StatusPartial          OrderStatus = "PARTIAL"
	StatusRejected         OrderStatus = "REJECTED"
	StatusCancelled        OrderStatus = "CANCELLED"
	StatusExpired          OrderStatus = "EXPIRED"
	StatusSLPlaced         OrderStatus = "SL_PLACED"
	StatusSLTriggered      OrderStatus = "SL_TRIGGERED"
	StatusSLFilled         OrderStatus = "SL_FILLED"
	StatusSLModifyPending  OrderStatus = "SL_MODIFY_PENDING"
	StatusEmergencySell    OrderStatus = "EMERGENCY_SELL"
	// SL_DEFERRED_BAND: the intended 20% stop is below the day's DPR band, so a
	// resting SL there is unplaceable and a band-floor stop would exit prematurely.
	// We hold (no broker order) — the stock can't reach the intended level in one
	// session — and the daily protective-replay places the real 20% SL once the
	// band re-centers. NON-terminal: the position is still live and re-evaluated.
	// Deliberately NOT included in GetActiveSLOrders, so the safety monitor never
	// treats a deferred position as "unprotected → re-place/market-sell".
	StatusSLDeferredBand OrderStatus = "SL_DEFERRED_BAND"

	// AMO replayer-specific lifecycle states.
	// AMO_PENDING:        submitted to broker AMO queue at 16:35; awaits 08:50 conversion
	// AMO_REJECTED:       converted but exchange rejected (typically DPR breach)
	// (Successful conversion ⇒ row is promoted to OrderTypeSLSell + StatusSLPlaced
	// with a fresh broker_order_id, indistinguishable from a regular in-session SL.)
	StatusAMOPending  OrderStatus = "AMO_PENDING"
	StatusAMORejected OrderStatus = "AMO_REJECTED"
)

func (s OrderStatus) IsTerminal() bool {
	switch s {
	case StatusFilled, StatusRejected, StatusCancelled, StatusExpired, StatusSLFilled:
		return true
	}
	return false
}

// ManthanOrder represents one order tracked in manthan_orders table.
type ManthanOrder struct {
	ID              int64       `db:"id"`
	SignalID        string      `db:"signal_id"`
	StrategyID      string      `db:"strategy_id"`
	UserID          string      `db:"user_id"`
	Symbol          string      `db:"symbol"`
	ISIN            string      `db:"isin"`
	Exchange        string      `db:"exchange"`
	OrderType       OrderType   `db:"order_type"`
	OrderSide       string      `db:"order_side"`
	ProductType     string      `db:"product_type"`
	Qty             int         `db:"qty"`
	FilledQty       int         `db:"filled_qty"`
	LimitPrice      float64     `db:"limit_price"`
	TriggerPrice    float64     `db:"trigger_price"` // INTENDED stop (high*0.80, un-clamped) — drives trail/ratchet
	AvgFillPrice    float64     `db:"avg_fill_price"`
	// BrokerTriggerPrice/BrokerLimitPrice mirror the broker's actual resting SL
	// (post DPR/tick clamp). Populated from the adapter's place/modify return
	// value and re-synced by the reconciler. trigger_price stays the intended 20%.
	BrokerTriggerPrice float64 `db:"broker_trigger_price"`
	BrokerLimitPrice   float64 `db:"broker_limit_price"`
	BrokerOrderID   string      `db:"broker_order_id"`
	BrokerStatus    string      `db:"broker_status"`
	IndiraSymbol    string      `db:"indira_symbol"`
	ExchangeToken   string      `db:"exchange_token"`
	Status          OrderStatus `db:"status"`
	RetryCount      int         `db:"retry_count"`
	MaxRetries      int         `db:"max_retries"`
	LastError       string      `db:"last_error"`
	ParentOrderID   *int64      `db:"parent_order_id"`
	CreatedAt       time.Time   `db:"created_at"`
	PlacedAt        *time.Time  `db:"placed_at"`
	FilledAt        *time.Time  `db:"filled_at"`
	CancelledAt     *time.Time  `db:"cancelled_at"`
	UpdatedAt       time.Time   `db:"updated_at"`
}

// ManthanSignal is the Kafka message from rules-engine (or rebalancer).
type ManthanSignal struct {
	OrderID       string  `json:"order_id"`
	UserID        string  `json:"user_id"`
	StrategyID    string  `json:"strategy_id"`
	Symbol        string  `json:"symbol"`
	ISIN          string  `json:"isin"`
	Exchange      string  `json:"exchange"`
	OrderType     string  `json:"order_type"`      // MARKET
	OrderSide     string  `json:"order_side"`       // BUY or SELL
	ProductType   string  `json:"product_type"`     // DELIVERY
	Quantity      int32   `json:"quantity"`
	EntryPrice    float64 `json:"entry_price"`
	StopLoss      float64 `json:"stop_loss"`
	StopLossType  string  `json:"stop_loss_type"`   // TRAILING
	StopLossPct   float64 `json:"stop_loss_pct"`    // 20
	TrailingSLPct float64 `json:"trailing_sl_pct"`  // 2
	InvestedAmt   float64 `json:"invested_amt"`
	Industry      string  `json:"industry"`
	MCapBucket    string  `json:"mcap_bucket"`
	IndexName     string  `json:"index_name"`
	EMAAllocPct   float64 `json:"ema_alloc_pct"`
	// BearerToken/AppId/Source fields removed 2026-06-25 — handlers now
	// always fetch creds at-edge via authProvider (user-config gRPC + DB
	// fallback). The token no longer rides the Kafka wire. See
	// internal/repository/grpc_credentials_repository.go.
	TradingMode   string  `json:"trading_mode"`

	// TopUpForSignalID — when set, this signal tops up an existing position
	// rather than opening a fresh one. Set only by the rebalancer (see
	// services/rebalancer/internal/allocator.go TopUpExistingPositions).
	// Effect on entry_handler: skip the "already holding" duplicate check.
	// Effect on rules-engine projector (via the FILL event): add qty +
	// invested onto the parent's manthan_positions row instead of creating
	// a new row. Empty for normal first-time entries.
	TopUpForSignalID string `json:"top_up_for_signal_id,omitempty"`
}

// SLModifySignal is the Kafka message for trailing SL update.
type SLModifySignal struct {
	OrderID    string  `json:"order_id"`
	UserID     string  `json:"user_id"`
	StrategyID string  `json:"strategy_id"`
	Symbol     string  `json:"symbol"`
	ISIN       string  `json:"isin"`
	Exchange   string  `json:"exchange"`
	OrderType  string  `json:"order_type"` // SL_MODIFY
	NewSL      float64 `json:"new_sl"`
	OldSL      float64 `json:"old_sl"`
	NewHigh    float64 `json:"new_high"`
	// Auth fields removed 2026-06-25 — fetched at-edge via authProvider.
	TradingMode string `json:"trading_mode"`
}

// SLExitSignal is the Kafka message for emergency sell.
type SLExitSignal struct {
	OrderID     string  `json:"order_id"`
	UserID      string  `json:"user_id"`
	StrategyID  string  `json:"strategy_id"`
	Symbol      string  `json:"symbol"`
	ISIN        string  `json:"isin"`
	Exchange    string  `json:"exchange"`
	OrderType   string  `json:"order_type"`    // MARKET
	OrderSide   string  `json:"order_side"`     // SELL
	ProductType string  `json:"product_type"`   // DELIVERY
	Quantity    int32   `json:"quantity"`
	ExitPrice   float64 `json:"exit_price"`
	SLPrice     float64 `json:"sl_price"`
	PnL         float64 `json:"pnl"`
	// Auth fields removed 2026-06-25 — fetched at-edge via authProvider.
	TradingMode string  `json:"trading_mode"`
}

// BrokerAuth holds credentials for broker API calls.
type BrokerAuth struct {
	UserID      string
	BearerToken string
	AppID       string
	Source      string
}

// SymbolInfo holds resolved symbol data for order placement.
type SymbolInfo struct {
	Symbol        string  // e.g., "GALLANTT"
	ExchangeToken string  // e.g., "13337"
	IndiraSymbol  string  // e.g., "STK_GALLANTT_EQ_NSE_13337"
	Exchange      string  // e.g., "NSE"
	TickSize      float64 // e.g., 0.05
	DPRLower      float64
	DPRUpper      float64
	// FreezeQty = exchange-imposed single-order size cap. Orders above this
	// are rejected outright by the exchange ("freeze qty exceeded"). 0 = no
	// limit advertised (treat as unlimited). For most NSE equities this is
	// 5-20 lakh shares, so rarely hit for Manthan positional sizing — kept
	// here as defensive sanity cap.
	FreezeQty int
}
