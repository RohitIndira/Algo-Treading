package handlers

import (
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
	case "MULTI_LEVEL":
		return pb.StopLossType_MULTI_LEVEL
	default:
		return pb.StopLossType_STOP_LOSS_TYPE_UNSPECIFIED
	}
}

func mapTakeProfitType(tpType string) pb.TakeProfitType {
	switch strings.ToUpper(tpType) {
	case "MULTI_LEVEL":
		return pb.TakeProfitType_TAKE_PROFIT_MULTI_LEVEL
	default:
		return pb.TakeProfitType_TAKE_PROFIT_FIXED
	}
}

func dtoMultiLevelToProto(levels []dto.MultiLevelExitLevel) []*pb.MultiLevelExitLevel {
	out := make([]*pb.MultiLevelExitLevel, len(levels))
	for i, l := range levels {
		out[i] = &pb.MultiLevelExitLevel{
			LevelNum: int32(l.LevelNum),
			PricePct: l.PricePct,
			QtyPct:   l.QtyPct,
		}
	}
	return out
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
		MarketCapTypes: c.MarketCapTypes,
		MarketCapRange: &pb.StrategyConditions_MarketCapRange{
			MinMcap: c.MinMarketCap,
			MaxMcap: c.MaxMarketCap,
		},
		PctChangeRange: &pb.StrategyConditions_PctChangeRange{
			MinPctChange: c.MinPriceChangePct,
			MaxPctChange: c.MaxPriceChangePct,
		},
		Exchanges: exchanges,
	}
}

func dtoTradeConfigToProto(tc *dto.TradeConfig) *pb.TradeConfig {
	if tc == nil {
		return nil
	}
	cfg := &pb.TradeConfig{
		OrderType:      mapOrderType(tc.OrderType),
		ProductType:    mapProductType(tc.ProductType),
		Validity:       tc.Validity,
		Quantity:       tc.Quantity,
		Exchange:       mapExchange(tc.Exchange),
		OrderSide:      mapOrderSide(tc.OrderSide),
		StopLossPct:    tc.StopLossPct,
		TakeProfitPct:  tc.TakeProfitPct,
		TrailingSlPct:  tc.TrailingSLPct,
		StopLossType:   mapStopLossType(tc.StopLossType),
		TakeProfitType: mapTakeProfitType(tc.TakeProfitType),
	}
	if len(tc.MultiLevelSL) > 0 {
		cfg.MultiLevelSl = dtoMultiLevelToProto(tc.MultiLevelSL)
	}
	if len(tc.MultiLevelTP) > 0 {
		cfg.MultiLevelTp = dtoMultiLevelToProto(tc.MultiLevelTP)
	}
	cfg.TradeWindowStart = tc.TradeWindowStart
	cfg.TradeWindowEnd = tc.TradeWindowEnd
	return cfg
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
		MaxAmountPerStock:        rl.MaxAmountPerStock,
		MaxTradesPerStrategy:     rl.MaxTradesPerStrategy,
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

	return req
}
