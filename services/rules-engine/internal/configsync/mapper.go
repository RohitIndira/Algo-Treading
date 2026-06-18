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
		StrategyID:             p.StrategyID,
		UserID:                 p.UserID,
		StrategyName:           p.StrategyName,
		Version:                p.Version,
		Active:                 p.Active,
		TradingMode:            p.TradingMode,
		CreatedAt:              unixNanosToTime(p.CreatedAt),
		UpdatedAt:              unixNanosToTime(p.UpdatedAt),
		ProcessAfterMarketNews: p.ProcessAfterMarketNews,
		AMNSelectedStocks:      p.AMNSelectedStocks,
	}

	// Conditions
	s.Conditions = models.Conditions{
		MatchAllNews:    p.Conditions.MatchAllNews,
		ImpactScoreMin:  p.Conditions.ImpactScoreMin,
		ImpactScoreMax:  p.Conditions.ImpactScoreMax,
		Sentiments:   nilSafeStringSlice(p.Conditions.Sentiments),
		Categories:   nilSafeStringSlice(p.Conditions.Categories),
		MinPctChange: p.Conditions.MinPriceChangePct,
		MaxPctChange:    p.Conditions.MaxPriceChangePct,
		Exchanges:      normalizeExchanges(p.Conditions.Exchanges),
		MarketCapTypes: nilSafeStringSlice(p.Conditions.MarketCapTypes),
	}
	s.Conditions.MarketCapRange = models.MarketCapRange{
		MinMcap: p.Conditions.MinMarketCap,
		MaxMcap: p.Conditions.MaxMarketCap,
	}

	// Trade config
	s.TradeConfig = models.TradeConfig{
		OrderType:        normalizeOrderType(p.TradeConfig.OrderType),
		Quantity:         p.TradeConfig.Quantity,
		Exchange:         normalizeExchange(p.TradeConfig.Exchange),
		OrderSide:        normalizeOrderSide(p.TradeConfig.OrderSide),
		Validity:         normalizeValidity(p.TradeConfig.Validity),
		LimitPrice:       p.TradeConfig.LimitPrice,
		StopLossPct:      p.TradeConfig.StopLossPct,
		TakeProfitPct:    p.TradeConfig.TakeProfitPct,
		StopLossType:     normalizeStopLossType(p.TradeConfig.StopLossType),
		TakeProfitType:   normalizeTakeProfitType(p.TradeConfig.TakeProfitType),
		TrailingSLPct:    p.TradeConfig.TrailingSLPct,
		ProductType:      normalizeProductType(p.TradeConfig.ProductType),
		MaxPositionSize:  0,
		MultiLevelSL:     toModelMultiLevel(p.TradeConfig.MultiLevelSL),
		MultiLevelTP:     toModelMultiLevel(p.TradeConfig.MultiLevelTP),
		TradeWindowStart: p.TradeConfig.TradeWindowStart,
		TradeWindowEnd:   p.TradeConfig.TradeWindowEnd,
	}

	// Risk limits
	s.RiskLimits = models.RiskLimits{
		MaxDailyTrades:       p.RiskLimits.MaxDailyTrades,
		MaxPerTradeRisk:      p.RiskLimits.MaxPerTradeRisk,
		MaxLossPerDay:        p.RiskLimits.MaxLossPerDay,
		EnableAutoSquareOff:  p.RiskLimits.EnableAutoSquareOff,
		AutoSquareOffTime:    p.RiskLimits.AutoSquareOffTime,
		MaxPositionSize:      0,
		MaxAmountPerStock:    p.RiskLimits.MaxAmountPerStock,
		MaxTradesPerStrategy: p.RiskLimits.MaxTradesPerStrategy,
	}

	return s, nil
}

func unixNanosToTime(n int64) time.Time {
	if n <= 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

func nilSafeStringSlice(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

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

func normalizeExchanges(in []string) []string {
	if in == nil {
		return []string{}
	}
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, normalizeExchange(v))
	}
	return out
}

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
	case "MULTI_LEVEL", "STOP_LOSS_TYPE_MULTI_LEVEL":
		return "MULTI_LEVEL"
	default:
		return "FIXED"
	}
}

func normalizeTakeProfitType(v string) string {
	switch v {
	case "MULTI_LEVEL", "TAKE_PROFIT_TYPE_MULTI_LEVEL":
		return "MULTI_LEVEL"
	default:
		return "FIXED"
	}
}

func toModelMultiLevel(in []MultiLevelExitLevelPayload) []models.MultiLevelExitLevel {
	if len(in) == 0 {
		return nil
	}
	out := make([]models.MultiLevelExitLevel, len(in))
	for i, l := range in {
		out[i] = models.MultiLevelExitLevel{
			LevelNum: l.LevelNum,
			PricePct: l.PricePct,
			QtyPct:   l.QtyPct,
		}
	}
	return out
}

func normalizeProductType(v string) string {
	if v == "" {
		return "INTRADAY"
	}
	return v
}
