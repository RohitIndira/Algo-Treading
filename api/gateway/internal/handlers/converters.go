package handlers

import (
	"fmt"
	"strings"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/dto"
	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
)

func mapTradingMode(mode string) pb.TradingMode {
	switch strings.ToUpper(mode) {
	case "PAPER":
		return pb.TradingMode_PAPER
	case "LIVE":
		return pb.TradingMode_LIVE
	default:
		return pb.TradingMode_TRADING_MODE_UNSPECIFIED
	}
}

func mapPositionSizingMode(m string) pb.PositionSizingMode {
	switch strings.ToUpper(m) {
	case "EMA_ALLOCATION":
		return pb.PositionSizingMode_EMA_ALLOCATION
	case "FIXED_QTY":
		return pb.PositionSizingMode_FIXED_QTY
	default:
		return pb.PositionSizingMode_FIXED_QTY
	}
}

func mapStrategyType(t string) pb.StrategyType {
	switch strings.ToUpper(t) {
	case "52W_BREAKOUT":
		return pb.StrategyType_WEEK52_BREAKOUT
	case "MANTHAN":
		return pb.StrategyType_MANTHAN
	case "HFT_BIDDING":
		return pb.StrategyType_HFT_BIDDING
	case "NEWS":
		return pb.StrategyType_NEWS
	default:
		return pb.StrategyType_NEWS
	}
}

// dtoHFTConfigToProto converts the JSON HFT config DTO to its proto form.
// Returns nil when the DTO is nil (non-HFT strategies).
func dtoHFTConfigToProto(h *dto.HFTConfig) *pb.HFTConfig {
	if h == nil {
		return nil
	}
	return &pb.HFTConfig{
		Symbol:              h.Symbol,
		Isin:                h.ISIN,
		Exchange:            h.Exchange,
		Side:                h.Side,
		ProductType:         h.ProductType,
		TickSize:            h.TickSize,
		MaxBuyQty:           h.MaxBuyQty,
		MaxSellQty:          h.MaxSellQty,
		SingleBuyQty:        h.SingleBuyQty,
		SingleSellQty:       h.SingleSellQty,
		BuyLimitPrice:       h.BuyLimitPrice,
		SellLimitPrice:      h.SellLimitPrice,
		WindowStart:         h.WindowStart,
		WindowEnd:           h.WindowEnd,
		ModifyOnPriceChange: h.ModifyOnPriceChange,
	}
}

func mapOrderType(orderType string) common.OrderType {
	switch strings.ToUpper(orderType) {
	case "MARKET":
		return common.OrderType_ORDER_TYPE_MARKET
	case "LIMIT":
		return common.OrderType_ORDER_TYPE_LIMIT
	case "STOP_LOSS":
		return common.OrderType_ORDER_TYPE_STOP_LOSS
	case "STOP_LOSS_MARKET":
		return common.OrderType_ORDER_TYPE_STOP_LOSS_MARKET
	default:
		return common.OrderType_ORDER_TYPE_UNSPECIFIED
	}
}

func mapProductType(productType string) common.ProductType {
	switch strings.ToUpper(productType) {
	case "INTRADAY", "MIS":
		return common.ProductType_PRODUCT_TYPE_INTRADAY
	case "DELIVERY", "CNC":
		return common.ProductType_PRODUCT_TYPE_DELIVERY
	case "CASH":
		return common.ProductType_PRODUCT_TYPE_CASH
	case "BRACKET", "BRACKET_ORDER", "BO":
		return common.ProductType_PRODUCT_TYPE_BRACKET
	default:
		return common.ProductType_PRODUCT_TYPE_UNSPECIFIED
	}
}

func mapExchange(exchange string) common.Exchange {
	switch strings.ToUpper(exchange) {
	case "NSE":
		return common.Exchange_EXCHANGE_NSE
	case "BSE":
		return common.Exchange_EXCHANGE_BSE
	default:
		return common.Exchange_EXCHANGE_UNSPECIFIED
	}
}

func mapOrderSide(side string) common.OrderSide {
	switch strings.ToUpper(side) {
	case "BUY":
		return common.OrderSide_ORDER_SIDE_BUY
	case "SELL":
		return common.OrderSide_ORDER_SIDE_SELL
	default:
		return common.OrderSide_ORDER_SIDE_UNSPECIFIED
	}
}

func mapStopLossType(slType string) pb.StopLossType {
	switch strings.ToUpper(slType) {
	case "FIXED":
		return pb.StopLossType_FIXED
	case "TRAILING":
		return pb.StopLossType_TRAILING
	default:
		return pb.StopLossType_STOP_LOSS_TYPE_UNSPECIFIED
	}
}

func mapSentiment(sentiment string) common.Sentiment {
	switch strings.ToUpper(sentiment) {
	case "POSITIVE":
		return common.Sentiment_SENTIMENT_POSITIVE
	case "NEGATIVE":
		return common.Sentiment_SENTIMENT_NEGATIVE
	case "NEUTRAL":
		return common.Sentiment_SENTIMENT_NEUTRAL
	default:
		return common.Sentiment_SENTIMENT_UNSPECIFIED
	}
}

func mapPositionSizing(sizing string) common.PositionSizing {
	switch strings.ToUpper(sizing) {
	case "FIXED":
		return common.PositionSizing_POSITION_SIZING_FIXED
	case "PERCENTAGE":
		return common.PositionSizing_POSITION_SIZING_PERCENTAGE
	case "RISK_BASED":
		return common.PositionSizing_POSITION_SIZING_RISK_BASED
	default:
		return common.PositionSizing_POSITION_SIZING_UNSPECIFIED
	}
}

func dtoConditionsToProto(c *dto.StrategyConditions) *pb.StrategyConditions {
	if c == nil {
		return nil
	}

	sentiments := make([]common.Sentiment, len(c.Sentiments))
	for i, s := range c.Sentiments {
		sentiments[i] = mapSentiment(s)
	}

	exchanges := make([]common.Exchange, len(c.Exchanges))
	for i, e := range c.Exchanges {
		exchanges[i] = mapExchange(e)
	}

	return &pb.StrategyConditions{
		MatchAllNews:   c.MatchAllNews,
		ImpactScoreMin: c.ImpactScoreMin,
		ImpactScoreMax: c.ImpactScoreMax,
		Sentiments:     sentiments,
		Categories:     c.Categories,
		StockCodes:     c.StockCodes,
		MarketCapTypes: c.MarketCapTypes,
		MarketCapRange: &pb.StrategyConditions_MarketCapRange{
			MinMcap: c.MinMarketCap,
			MaxMcap: c.MaxMarketCap,
		},
		PctChangeRange: &pb.StrategyConditions_PctChangeRange{
			MinPctChange: c.MinPriceChangePct,
			MaxPctChange: c.MaxPriceChangePct,
		},
		VolumeThreshold: c.MinVolume,
		Exchanges:       exchanges,
	}
}

func dtoTradeConfigToProto(tc *dto.TradeConfig) *pb.TradeConfig {
	if tc == nil {
		return nil
	}
	return &pb.TradeConfig{
		OrderType:          mapOrderType(tc.OrderType),
		ProductType:        mapProductType(tc.ProductType),
		Validity:           tc.Validity,
		Quantity:           tc.Quantity,
		Exchange:           mapExchange(tc.Exchange),
		OrderSide:          mapOrderSide(tc.OrderSide),
		StopLossPct:        tc.StopLossPct,
		TakeProfitPct:      tc.TakeProfitPct,
		TrailingSlPct:      tc.TrailingSLPct,
		StopLossType:       mapStopLossType(tc.StopLossType),
		PositionSizingMode: mapPositionSizingMode(tc.PositionSizingMode),
		TotalCapital:       tc.TotalCapital,
		MaxPositions:       tc.MaxPositions,
		PerStockAmount:     tc.PerStockAmount,
	}
}

func dtoRiskLimitsToProto(rl *dto.RiskLimits) *pb.RiskLimits {
	if rl == nil {
		return nil
	}
	return &pb.RiskLimits{
		MaxDailyTrades:          rl.MaxDailyTrades,
		MaxPerTradeRisk:         rl.MaxPerTradeRisk,
		MaxPortfolioExposurePct: rl.MaxPortfolioExposurePct,
		MaxLossPerDay:           rl.MaxLossPerDay,
		EnableRiskChecks:        rl.EnableRiskChecks,
		EnableAutoSquareOff:     rl.EnableAutoSquareOff,
		AutoSquareOffTime:       rl.AutoSquareOffTime,
		PositionSizing:          mapPositionSizing(rl.PositionSizing),
	}
}

func dtoUpdateStrategyToProto(reqDTO *dto.UpdateStrategyRequest) *pb.UpdateStrategyRequest {
	req := &pb.UpdateStrategyRequest{
		UserId:      reqDTO.UserID,
		Conditions:  dtoConditionsToProto(reqDTO.Conditions),
		TradeConfig: dtoTradeConfigToProto(reqDTO.TradeConfig),
		RiskLimits:  dtoRiskLimitsToProto(reqDTO.RiskLimits),
		Version:     reqDTO.Version,
	}

	if reqDTO.StrategyName != nil {
		req.StrategyName = reqDTO.StrategyName
	}
	if reqDTO.Description != nil {
		req.Description = reqDTO.Description
	}
	if reqDTO.TradingMode != nil {
		tm := mapTradingMode(*reqDTO.TradingMode)
		req.TradingMode = &tm
	}
	if reqDTO.HFTConfig != nil {
		req.HftConfig = dtoHFTConfigToProto(reqDTO.HFTConfig)
	}

	return req
}

// build52WResponse creates a clean JSON response for 52W strategies.
// Only includes fields relevant to 52W — no news conditions, no bracket order fields.
func build52WResponse(resp *pb.CreateStrategyResponse) map[string]interface{} {
	s := resp.Strategy
	tc := s.TradeConfig

	result := map[string]interface{}{
		"success": true,
		"strategy": map[string]interface{}{
			"strategy_id":   s.StrategyId,
			"user_id":       s.UserId,
			"strategy_name": s.StrategyName,
			"strategy_type": "52W_BREAKOUT",
			"active":        s.Active,
			"trading_mode":  fmt.Sprintf("%v", s.TradingMode),
			"trade_config": map[string]interface{}{
				"total_capital":    tc.TotalCapital,
				"max_positions":    tc.MaxPositions,
				"per_stock_amount": tc.PerStockAmount,
				"stop_loss_pct":    tc.StopLossPct,
				"take_profit_pct":  tc.TakeProfitPct,
			},
		},
	}

	return result
}

func buildManthanResponse(resp *pb.CreateStrategyResponse) map[string]interface{} {
	s := resp.Strategy
	return map[string]interface{}{
		"success":       true,
		"strategy_id":   s.StrategyId,
		"strategy_name": s.StrategyName,
		"strategy_type": "MANTHAN",
		"trading_mode":  fmt.Sprintf("%v", s.TradingMode),
	}
}

// buildHFTResponse creates a clean JSON response for HFT_BIDDING strategies,
// echoing back the resolved HFT config so the frontend can confirm what was
// stored (engine-applied defaults included).
func buildHFTResponse(resp *pb.CreateStrategyResponse) map[string]interface{} {
	s := resp.Strategy
	out := map[string]interface{}{
		"success":       true,
		"strategy_id":   s.StrategyId,
		"user_id":       s.UserId,
		"strategy_name": s.StrategyName,
		"strategy_type": "HFT_BIDDING",
		"active":        s.Active,
		"trading_mode":  fmt.Sprintf("%v", s.TradingMode),
	}
	if h := s.HftConfig; h != nil {
		out["hft_config"] = map[string]interface{}{
			"symbol":                 h.Symbol,
			"isin":                   h.Isin,
			"exchange":               h.Exchange,
			"side":                   h.Side,
			"product_type":           h.ProductType,
			"tick_size":              h.TickSize,
			"max_buy_qty":            h.MaxBuyQty,
			"max_sell_qty":           h.MaxSellQty,
			"single_buy_qty":         h.SingleBuyQty,
			"single_sell_qty":        h.SingleSellQty,
			"buy_limit_price":        h.BuyLimitPrice,
			"sell_limit_price":       h.SellLimitPrice,
			"window_start":           h.WindowStart,
			"window_end":             h.WindowEnd,
			"modify_on_price_change": h.ModifyOnPriceChange,
		}
	}
	return out
}
