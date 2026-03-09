package models

// TradeSignal is the real-time signal emitted by Rules Engine.
//
// It is published to:
// - NATS subject: trade.signals.{user_id}
// - Kafka topic: trade-signals (audit/persistence)
//
// Trade Execution / Paper Execution must be able to place the order without
// fetching strategy config on the hot path, so TradeConfig and RiskLimits are
// embedded.
type TradeSignal struct {
	UserID      string      `json:"user_id"`
	StrategyID  string      `json:"strategy_id"`
	NewsID      string      `json:"news_id"`
	TradingMode string      `json:"trading_mode"`
	StockCode   int64       `json:"stock_code"`
	GeneratedAt int64       `json:"generated_at"` // UnixNano
	TradeConfig TradeConfig `json:"trade_config"`
	RiskLimits  RiskLimits  `json:"risk_limits"`
}
