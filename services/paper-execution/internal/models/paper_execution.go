package models

import "time"

// PaperExecutionEvent is emitted by paper-execution to Kafka topic
// paper-executions.52w whenever a simulated fill/exit happens.
//
// This is the primary "truth" for CLOSED PnL in paper trading.
type PaperExecutionEvent struct {
	EventID     string    `json:"event_id"`
	StrategyID  string    `json:"strategy_id"`
	UserID      string    `json:"user_id"`
	Token       int64     `json:"token"`
	Symbol      string    `json:"symbol"`
	Exchange    string    `json:"exchange"`
	OrderSide   string    `json:"order_side"` // BUY or SELL
	Quantity    int32     `json:"quantity"`
	Price       float64   `json:"price"`
	Leg         string    `json:"leg"`          // ENTRY, SL_HALF, TSL_REST
	Reason      string    `json:"reason"`       // e.g. SL_10, TSL_20
	BuyOrderID  string    `json:"buy_order_id"` // original buy
	PnL         float64   `json:"pnl"`          // realized pnl for this leg
	CreatedAt   time.Time `json:"created_at"`
}

// PaperPnLSnapshot is optional aggregated snapshot emitted to paper-pnl.52w.
// This is useful for UI/analytics without needing to replay executions.
type PaperPnLSnapshot struct {
	UserID       string    `json:"user_id"`
	StrategyID   string    `json:"strategy_id"`
	ClosedPnL    float64   `json:"closed_pnl"`
	OpenPositions int      `json:"open_positions"`
	Timestamp    time.Time `json:"timestamp"`
}
