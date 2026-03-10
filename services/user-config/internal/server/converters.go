package server

import (
	"strings"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
)

func stringToOrderType(s string) common.OrderType {
	switch strings.ToUpper(s) {
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

func stringToExchange(s string) common.Exchange {
	switch strings.ToUpper(s) {
	case "NSE":
		return common.Exchange_EXCHANGE_NSE
	case "BSE":
		return common.Exchange_EXCHANGE_BSE
	default:
		return common.Exchange_EXCHANGE_UNSPECIFIED
	}
}

func stringToOrderSide(s string) common.OrderSide {
	switch strings.ToUpper(s) {
	case "BUY":
		return common.OrderSide_ORDER_SIDE_BUY
	case "SELL":
		return common.OrderSide_ORDER_SIDE_SELL
	default:
		return common.OrderSide_ORDER_SIDE_UNSPECIFIED
	}
}

func stringToProductType(s string) common.ProductType {
	switch strings.ToUpper(s) {
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

func stringToStopLossType(s string) pb.StopLossType {
	switch strings.ToUpper(s) {
	case "FIXED":
		return pb.StopLossType_FIXED
	case "TRAILING":
		return pb.StopLossType_TRAILING
	default:
		return pb.StopLossType_STOP_LOSS_TYPE_UNSPECIFIED
	}
}

func stringToPositionSizing(s string) common.PositionSizing {
	switch strings.ToUpper(s) {
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
