package models

import "time"

// TradeSignal mirrors the rules-engine OrderRequest JSON that is
// published to the Kafka topic "trade-signals".
//
// We intentionally duplicate the minimal fields we need for PAPER
// execution instead of importing rules-engine packages.
type TradeSignal struct {
	OrderID      string    `json:"order_id"`
	UserID       string    `json:"user_id"`
	StrategyID   string    `json:"strategy_id"`
	StrategyName string    `json:"strategy_name"`
	StockCode    int64     `json:"stock_code"`
	Token        int64     `json:"token"`
	Symbol       string    `json:"symbol"`
	Exchange     string    `json:"exchange"`
	OrderType    string    `json:"order_type"`
	Quantity     int32     `json:"quantity"`
	Price        float64   `json:"price"`
	StopLoss     float64   `json:"stop_loss"`
	TakeProfit   float64   `json:"take_profit"`
	Timestamp    time.Time `json:"timestamp"`
	OrderSide    string    `json:"order_side"`   // BUY/SELL
	TradingMode  string    `json:"trading_mode"` // PAPER/LIVE
}
