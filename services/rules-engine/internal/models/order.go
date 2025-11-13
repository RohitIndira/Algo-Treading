package models

import (
	"time"

	"github.com/google/uuid"
)

// OrderRequest represents an order request to be published to RabbitMQ
type OrderRequest struct {
	OrderID      string    `json:"order_id"`
	UserID       string    `json:"user_id"`
	StrategyID   string    `json:"strategy_id"`
	StrategyName string    `json:"strategy_name"`
	EventID      string    `json:"event_id"`
	StockCode    int64     `json:"stock_code"`
	Symbol       string    `json:"symbol"`
	Exchange     string    `json:"exchange"`
	OrderType    string    `json:"order_type"` // MARKET, LIMIT
	Quantity     int32     `json:"quantity"`
	Price        float64   `json:"price"`
	StopLoss     float64   `json:"stop_loss"`
	TakeProfit   float64   `json:"take_profit"`
	Timestamp    time.Time `json:"timestamp"`
	MatchScore   float64   `json:"match_score"`
	ImpactScore  int32     `json:"impact_score"`
	Sentiment    string    `json:"sentiment"`
	NewsCategory string    `json:"news_category"`
}

// RuleMatch represents a successful rule match
type RuleMatch struct {
	UserID            string    `json:"user_id"`
	StrategyID        string    `json:"strategy_id"`
	StrategyName      string    `json:"strategy_name"`
	MatchScore        float64   `json:"match_score"`
	MatchedConditions []string  `json:"matched_conditions"`
	FailedConditions  []string  `json:"failed_conditions"`
	ApprovedByRisk    bool      `json:"approved_by_risk"`
	OrderRequestID    string    `json:"order_request_id"`
	Timestamp         time.Time `json:"timestamp"`
	EventID           string    `json:"event_id"`
}

// NewOrderRequest creates a new order request from a match and event
func NewOrderRequest(match *RuleMatch, event *MarketEvent, strategy *Strategy) *OrderRequest {
	orderID := uuid.New().String()

	// Calculate stop loss and take profit
	price := event.MarketData.LastTradedPrice
	stopLoss := price * (1 - strategy.TradeConfig.StopLossPct/100)
	takeProfit := price * (1 + strategy.TradeConfig.TakeProfitPct/100)

	return &OrderRequest{
		OrderID:      orderID,
		UserID:       strategy.UserID,
		StrategyID:   strategy.StrategyID,
		StrategyName: strategy.StrategyName,
		EventID:      event.EventID,
		StockCode:    event.StockData.StockCode,
		Symbol:       event.StockData.Symbol,
		Exchange:     event.StockData.Exchange,
		OrderType:    strategy.TradeConfig.OrderType,
		Quantity:     strategy.TradeConfig.Quantity,
		Price:        price,
		StopLoss:     stopLoss,
		TakeProfit:   takeProfit,
		Timestamp:    time.Now(),
		MatchScore:   match.MatchScore,
		ImpactScore:  event.Analysis.ImpactScore,
		Sentiment:    event.Analysis.Sentiment,
		NewsCategory: event.NewsData.Category,
	}
}

// Validate validates an order request
func (o *OrderRequest) Validate() error {
	if o.OrderID == "" {
		return ErrInvalidOrderID
	}
	if o.UserID == "" {
		return ErrInvalidUserID
	}
	if o.StrategyID == "" {
		return ErrInvalidStrategyID
	}
	if o.StockCode <= 0 {
		return ErrInvalidStockCode
	}
	if o.Exchange == "" {
		return ErrInvalidExchange
	}
	if o.Quantity <= 0 {
		return ErrInvalidQuantity
	}
	if o.Price <= 0 {
		return ErrInvalidPrice
	}
	if o.OrderType != "MARKET" && o.OrderType != "LIMIT" {
		return ErrInvalidOrderType
	}
	return nil
}
