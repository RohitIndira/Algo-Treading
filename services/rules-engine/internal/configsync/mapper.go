package configsync

import (
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// ToModelStrategy converts Kafka payload strategy into the internal model.
// It normalizes fields to match rules-engine expectations (order type, exchange, etc.).
func ToModelStrategy(p *StrategyPayload) (*models.Strategy, error) {
	if p == nil {
		return nil, fmt.Errorf("payload is nil")
	}
	if p.UserID == "" || p.StrategyID == "" {
		return nil, fmt.Errorf("missing user_id/strategy_id")
	}

	s := &models.Strategy{
		StrategyID:   p.StrategyID,
		UserID:       p.UserID,
		StrategyName: p.StrategyName,
		StrategyType: p.StrategyType,
		Version:      p.Version,
		Active:       p.Active,
		TradingMode:  p.TradingMode,
		CreatedAt:    unixNanosToTime(p.CreatedAt),
		UpdatedAt:    unixNanosToTime(p.UpdatedAt),
	}

	// MANTHAN strategies are always active once created — backend controls lifecycle.
	if p.StrategyType == "MANTHAN" {
		s.Active = true
	}

	// Conditions + RiskLimits mapping removed 2026-06-25 — those Strategy
	// fields are gone with the news-event path. Mapper now only fills the
	// TradeConfig block that the Manthan allocator/order generator consume.
	// Incoming p.Conditions / p.RiskLimits fields are silently ignored.

	// Trade config
	s.TradeConfig = models.TradeConfig{
		OrderType:       normalizeOrderType(p.TradeConfig.OrderType),
		Quantity:        p.TradeConfig.Quantity,
		Exchange:        normalizeExchange(p.TradeConfig.Exchange),
		OrderSide:       normalizeOrderSide(p.TradeConfig.OrderSide),
		Validity:        normalizeValidity(p.TradeConfig.Validity),
		LimitPrice:      p.TradeConfig.LimitPrice,
		StopLossPct:     p.TradeConfig.StopLossPct,
		TakeProfitPct:   p.TradeConfig.TakeProfitPct,
		StopLossType:    normalizeStopLossType(p.TradeConfig.StopLossType),
		TrailingSLPct:   p.TradeConfig.TrailingSLPct,
		ProductType:     normalizeProductType(p.TradeConfig.ProductType),
		TotalCapital:    p.TradeConfig.TotalCapital,
		MaxPositions:    p.TradeConfig.MaxPositions,
		PerStockAmount:  p.TradeConfig.PerStockAmount,
		MaxPositionSize: 0,
	}

	return s, nil
}

func unixNanosToTime(n int64) time.Time {
	if n <= 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// nilSafeStringSlice / nilSafeInt64Slice removed 2026-06-25 (Cat B trim) —
// they only protected the Conditions mapping that's gone with the news path.

func normalizeOrderType(v string) string {
	switch v {
	case "ORDER_TYPE_MARKET", "MARKET":
		return "MARKET"
	case "ORDER_TYPE_LIMIT", "LIMIT":
		return "LIMIT"
	case "ORDER_TYPE_BRACKET", "BRACKET":
		return "BRACKET"
	default:
		// safe default
		return "MARKET"
	}
}

func normalizeExchange(v string) string {
	switch v {
	case "EXCHANGE_NSE", "NSE":
		return "NSE"
	case "EXCHANGE_BSE", "BSE":
		return "BSE"
	default:
		return v
	}
}

// normalizeExchanges removed 2026-06-25 (Cat B trim) — served the Conditions
// mapping. normalizeExchange (singular) is still used for TradeConfig.Exchange.

func normalizeOrderSide(v string) string {
	switch v {
	case "ORDER_SIDE_BUY", "BUY":
		return "BUY"
	case "ORDER_SIDE_SELL", "SELL":
		return "SELL"
	default:
		return "BUY"
	}
}

func normalizeValidity(v string) string {
	if v == "" {
		return "DAY"
	}
	return v
}

func normalizeStopLossType(v string) string {
	switch v {
	case "FIXED", "STOP_LOSS_TYPE_FIXED":
		return "FIXED"
	case "TRAILING", "STOP_LOSS_TYPE_TRAILING":
		return "TRAILING"
	default:
		return "FIXED"
	}
}

func normalizeProductType(v string) string {
	if v == "" {
		return "INTRADAY"
	}
	return v
}
