package server

import (
	"context"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/service"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// UserConfigServer implements the gRPC UserConfigService
type UserConfigServer struct {
	pb.UnimplementedUserConfigServiceServer
	service *service.StrategyService
}

// NewUserConfigServer creates a new gRPC server
func NewUserConfigServer(service *service.StrategyService) *UserConfigServer {
	return &UserConfigServer{
		service: service,
	}
}

// CreateStrategy creates a new trading strategy
func (s *UserConfigServer) CreateStrategy(ctx context.Context, req *pb.CreateStrategyRequest) (*pb.CreateStrategyResponse, error) {
	// Convert proto request to domain model
	modelReq := &models.CreateStrategyRequest{
		UserID:              req.UserId,
		StrategyName:        req.StrategyName,
		Description:         req.Description,
		Conditions:          protoConditionsToModel(req.Conditions),
		TradeConfig:         protoTradeConfigToModel(req.TradeConfig),
		RiskLimits:          protoRiskLimitsToModel(req.RiskLimits),
		ActivateImmediately: req.ActivateImmediately,
	}

	// Extract Indira auth context if provided
	if req.IndiraAuth != nil {
		modelReq.BearerToken = req.IndiraAuth.BearerToken
		modelReq.AppId = req.IndiraAuth.AppId
		modelReq.Source = req.IndiraAuth.Source
	}

	// Create strategy
	strategy, err := s.service.CreateStrategy(ctx, modelReq)
	if err != nil {
		return &pb.CreateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "CREATION_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.CreateStrategyResponse{
		Success:  true,
		Strategy: modelStrategyToProto(strategy),
	}, nil
}

// UpdateStrategy updates an existing strategy
func (s *UserConfigServer) UpdateStrategy(ctx context.Context, req *pb.UpdateStrategyRequest) (*pb.UpdateStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.UpdateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_STRATEGY_ID",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	modelReq := &models.UpdateStrategyRequest{
		StrategyID: strategyID,
		UserID:     req.UserId,
		Version:    req.Version,
	}

	if req.StrategyName != nil {
		modelReq.StrategyName = req.StrategyName
	}
	if req.Description != nil {
		modelReq.Description = req.Description
	}
	if req.Conditions != nil {
		modelReq.Conditions = protoConditionsToModel(req.Conditions)
	}
	if req.TradeConfig != nil {
		modelReq.TradeConfig = protoTradeConfigToModel(req.TradeConfig)
	}
	if req.RiskLimits != nil {
		modelReq.RiskLimits = protoRiskLimitsToModel(req.RiskLimits)
	}

	strategy, err := s.service.UpdateStrategy(ctx, modelReq)
	if err != nil {
		return &pb.UpdateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "UPDATE_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.UpdateStrategyResponse{
		Success:  true,
		Strategy: modelStrategyToProto(strategy),
	}, nil
}

// DeleteStrategy deletes a strategy
func (s *UserConfigServer) DeleteStrategy(ctx context.Context, req *pb.DeleteStrategyRequest) (*pb.DeleteStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.DeleteStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_STRATEGY_ID",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	err = s.service.DeleteStrategy(ctx, strategyID, req.UserId)
	if err != nil {
		return &pb.DeleteStrategyResponse{
			Success: false,
			Message: "",
			Error: &common.Error{
				Code:    "DELETION_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.DeleteStrategyResponse{
		Success: true,
		Message: "Strategy deleted successfully",
	}, nil
}

// GetStrategy retrieves a specific strategy
func (s *UserConfigServer) GetStrategy(ctx context.Context, req *pb.GetStrategyRequest) (*pb.GetStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.GetStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_STRATEGY_ID",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	strategy, err := s.service.GetStrategy(ctx, strategyID, req.UserId)
	if err != nil {
		return &pb.GetStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "NOT_FOUND",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.GetStrategyResponse{
		Success:  true,
		Strategy: modelStrategyToProto(strategy),
	}, nil
}

// ListUserStrategies lists all strategies for a user
func (s *UserConfigServer) ListUserStrategies(ctx context.Context, req *pb.ListUserStrategiesRequest) (*pb.ListUserStrategiesResponse, error) {
	page := int(req.Pagination.GetPage())
	pageSize := int(req.Pagination.GetPageSize())

	// Convert page/pageSize to limit/offset
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	strategies, total, err := s.service.ListUserStrategies(ctx, req.UserId, req.ActiveOnly, pageSize, offset)
	if err != nil {
		return &pb.ListUserStrategiesResponse{
			Success: false,
			Error: &common.Error{
				Code:    "LIST_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	// Initialize empty slice to ensure JSON returns [] instead of null
	protoStrategies := make([]*pb.Strategy, 0)
	if len(strategies) > 0 {
		protoStrategies = make([]*pb.Strategy, len(strategies))
		for i, strategy := range strategies {
			protoStrategies[i] = modelStrategyToProto(strategy)
		}
	}

	totalPages := int32((int64(total) + int64(pageSize) - 1) / int64(pageSize))

	return &pb.ListUserStrategiesResponse{
		Success:    true,
		Strategies: protoStrategies,
		Pagination: &common.PaginationResponse{
			Page:        int32(page),
			PageSize:    int32(pageSize),
			TotalItems:  int64(total),
			TotalPages:  totalPages,
			HasNext:     page < int(totalPages),
			HasPrevious: page > 1,
		},
	}, nil
}

// ActivateStrategy activates a strategy
func (s *UserConfigServer) ActivateStrategy(ctx context.Context, req *pb.ActivateStrategyRequest) (*pb.ActivateStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.ActivateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_STRATEGY_ID",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	strategy, err := s.service.ActivateStrategy(ctx, strategyID, req.UserId)
	if err != nil {
		return &pb.ActivateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "ACTIVATION_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.ActivateStrategyResponse{
		Success:  true,
		Strategy: modelStrategyToProto(strategy),
	}, nil
}

// DeactivateStrategy deactivates a strategy
func (s *UserConfigServer) DeactivateStrategy(ctx context.Context, req *pb.DeactivateStrategyRequest) (*pb.DeactivateStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.DeactivateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_STRATEGY_ID",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	strategy, err := s.service.DeactivateStrategy(ctx, strategyID, req.UserId)
	if err != nil {
		return &pb.DeactivateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "DEACTIVATION_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.DeactivateStrategyResponse{
		Success:  true,
		Strategy: modelStrategyToProto(strategy),
	}, nil
}

// GetStrategiesByIDs retrieves multiple strategies by their IDs
func (s *UserConfigServer) GetStrategiesByIDs(ctx context.Context, req *pb.GetStrategiesByIDsRequest) (*pb.GetStrategiesByIDsResponse, error) {
	strategyIDs := make([]uuid.UUID, len(req.StrategyIds))
	for i, idStr := range req.StrategyIds {
		id, err := uuid.Parse(idStr)
		if err != nil {
			return &pb.GetStrategiesByIDsResponse{
				Success: false,
				Error: &common.Error{
					Code:    "INVALID_STRATEGY_ID",
					Message: fmt.Sprintf("Invalid strategy ID at index %d", i),
				},
			}, nil
		}
		strategyIDs[i] = id
	}

	strategies, err := s.service.GetStrategiesByIDs(ctx, strategyIDs)
	if err != nil {
		return &pb.GetStrategiesByIDsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "FETCH_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	protoStrategies := make([]*pb.Strategy, len(strategies))
	for i, strategy := range strategies {
		protoStrategies[i] = modelStrategyToProto(strategy)
	}

	return &pb.GetStrategiesByIDsResponse{
		Success:    true,
		Strategies: protoStrategies,
	}, nil
}

// HealthCheck performs a health check
func (s *UserConfigServer) HealthCheck(ctx context.Context, req *common.HealthCheckRequest) (*common.HealthCheckResponse, error) {
	return &common.HealthCheckResponse{
		Healthy: true,
		Service: "user-config-service",
		Version: "1.0.0",
	}, nil
}

// Helper functions to convert between proto and model types

func protoConditionsToModel(proto *pb.StrategyConditions) *models.StrategyCondition {
	if proto == nil {
		return nil
	}

	sentiments := make(pq.StringArray, len(proto.Sentiments))
	for i, s := range proto.Sentiments {
		sentiments[i] = sentimentToString(s)
	}

	exchanges := make(pq.StringArray, len(proto.Exchanges))
	for i, e := range proto.Exchanges {
		exchanges[i] = exchangeToString(e)
	}

	stockCodes := make(pq.Int64Array, len(proto.StockCodes))
	for i, code := range proto.StockCodes {
		stockCodes[i] = code
	}

	cond := &models.StrategyCondition{
		ImpactScoreThreshold: proto.ImpactScoreThreshold,
		Sentiments:           sentiments,
		Categories:           pq.StringArray(proto.Categories),
		StockCodes:           stockCodes,
		VolumeThreshold:      &proto.VolumeThreshold,
		PctChangeThreshold:   &proto.PctChangeThreshold,
		Exchanges:            exchanges,
	}

	if proto.PriceRange != nil {
		cond.PriceRangeMin = &proto.PriceRange.MinPrice
		cond.PriceRangeMax = &proto.PriceRange.MaxPrice
	}

	if proto.MarketCapRange != nil {
		cond.MinMarketCap = &proto.MarketCapRange.MinMcap
		cond.MaxMarketCap = &proto.MarketCapRange.MaxMcap
	}

	return cond
}

// Helper functions to convert protobuf enums to short string codes
func orderTypeToString(ot common.OrderType) string {
	switch ot {
	case common.OrderType_ORDER_TYPE_MARKET:
		return "MARKET"
	case common.OrderType_ORDER_TYPE_LIMIT:
		return "LIMIT"
	case common.OrderType_ORDER_TYPE_STOP_LOSS:
		return "STOP_LOSS"
	case common.OrderType_ORDER_TYPE_STOP_LOSS_MARKET:
		return "STOP_LOSS_MKT"
	default:
		return "MARKET"
	}
}

func exchangeToString(ex common.Exchange) string {
	switch ex {
	case common.Exchange_EXCHANGE_NSE:
		return "NSE"
	case common.Exchange_EXCHANGE_BSE:
		return "BSE"
	default:
		return "NSE"
	}
}

func orderSideToString(os common.OrderSide) string {
	switch os {
	case common.OrderSide_ORDER_SIDE_BUY:
		return "BUY"
	case common.OrderSide_ORDER_SIDE_SELL:
		return "SELL"
	default:
		return "BUY"
	}
}

func productTypeToString(pt common.ProductType) string {
	switch pt {
	case common.ProductType_PRODUCT_TYPE_INTRADAY:
		return "INTRADAY"
	case common.ProductType_PRODUCT_TYPE_DELIVERY:
		return "DELIVERY"
	case common.ProductType_PRODUCT_TYPE_CASH:
		return "CASH"
	default:
		return "INTRADAY"
	}
}

func stopLossTypeToString(slt pb.StopLossType) string {
	switch slt {
	case pb.StopLossType_FIXED:
		return "FIXED"
	case pb.StopLossType_TRAILING:
		return "TRAILING"
	default:
		return "FIXED"
	}
}

func positionSizingToString(ps common.PositionSizing) string {
	switch ps {
	case common.PositionSizing_POSITION_SIZING_FIXED:
		return "FIXED"
	case common.PositionSizing_POSITION_SIZING_PERCENTAGE:
		return "PERCENTAGE"
	case common.PositionSizing_POSITION_SIZING_RISK_BASED:
		return "RISK_BASED"
	default:
		return "FIXED"
	}
}

func protoTradeConfigToModel(proto *pb.TradeConfig) *models.TradeConfig {
	if proto == nil {
		return nil
	}

	config := &models.TradeConfig{
		OrderType:       orderTypeToString(proto.OrderType),
		Quantity:        proto.Quantity,
		MaxPositionSize: &proto.MaxPositionSize,
		StopLossPct:     &proto.StopLossPct,
		TakeProfitPct:   &proto.TakeProfitPct,
		Exchange:        exchangeToString(proto.Exchange),
		OrderSide:       orderSideToString(proto.OrderSide),
		LimitPrice:      &proto.LimitPrice,
		Validity:        proto.Validity,
		StopLossType:    stopLossTypeToString(proto.StopLossType),
		TrailingSLPct:   &proto.TrailingSlPct,
		ProductType:     productTypeToString(proto.ProductType),
	}

	return config
}

func protoRiskLimitsToModel(proto *pb.RiskLimits) *models.RiskLimits {
	if proto == nil {
		return nil
	}

	limits := &models.RiskLimits{
		MaxDailyTrades:          &proto.MaxDailyTrades,
		MaxLossPerDay:           &proto.MaxLossPerDay,
		PositionSizing:          positionSizingToString(proto.PositionSizing),
		MaxPortfolioExposurePct: &proto.MaxPortfolioExposurePct,
		MaxPerTradeRisk:         &proto.MaxPerTradeRisk,
		EnableRiskChecks:        proto.EnableRiskChecks,
		EnableAutoSquareOff:     proto.EnableAutoSquareOff,
		AutoSquareOffTime:       proto.AutoSquareOffTime,
	}

	return limits
}

func modelStrategyToProto(model *models.Strategy) *pb.Strategy {
	if model == nil {
		return nil
	}

	strategy := &pb.Strategy{
		StrategyId:   model.StrategyID.String(),
		UserId:       model.UserID,
		StrategyName: model.StrategyName,
		Description:  model.Description,
		Active:       model.Active,
		Version:      model.Version,
		CreatedAt:    &common.Timestamp{Seconds: model.CreatedAt.Unix()},
		UpdatedAt:    &common.Timestamp{Seconds: model.UpdatedAt.Unix()},
	}

	if model.Conditions != nil {
		strategy.Conditions = modelConditionsToProto(model.Conditions)
	}
	if model.TradeConfig != nil {
		strategy.TradeConfig = modelTradeConfigToProto(model.TradeConfig)
	}
	if model.RiskLimits != nil {
		strategy.RiskLimits = modelRiskLimitsToProto(model.RiskLimits)
	}

	return strategy
}

func modelConditionsToProto(model *models.StrategyCondition) *pb.StrategyConditions {
	if model == nil {
		return nil
	}

	sentiments := make([]common.Sentiment, len(model.Sentiments))
	for i, s := range model.Sentiments {
		sentiments[i] = stringToSentiment(s)
	}

	exchanges := make([]common.Exchange, len(model.Exchanges))
	for i, e := range model.Exchanges {
		exchanges[i] = stringToExchange(e)
	}

	stockCodes := make([]int64, len(model.StockCodes))
	for i, code := range model.StockCodes {
		stockCodes[i] = code
	}

	cond := &pb.StrategyConditions{
		ImpactScoreThreshold: model.ImpactScoreThreshold,
		Sentiments:           sentiments,
		Categories:           []string(model.Categories),
		StockCodes:           stockCodes,
		Exchanges:            exchanges,
	}

	if model.PriceRangeMin != nil && model.PriceRangeMax != nil {
		cond.PriceRange = &common.PriceRange{
			MinPrice: *model.PriceRangeMin,
			MaxPrice: *model.PriceRangeMax,
		}
	}

	if model.VolumeThreshold != nil {
		cond.VolumeThreshold = *model.VolumeThreshold
	}
	if model.PctChangeThreshold != nil {
		cond.PctChangeThreshold = *model.PctChangeThreshold
	}

	return cond
}

// Helper functions to convert short string codes back to protobuf enums
func stringToOrderType(s string) common.OrderType {
	switch s {
	case "MARKET":
		return common.OrderType_ORDER_TYPE_MARKET
	case "LIMIT":
		return common.OrderType_ORDER_TYPE_LIMIT
	case "STOP_LOSS":
		return common.OrderType_ORDER_TYPE_STOP_LOSS
	case "STOP_LOSS_MKT":
		return common.OrderType_ORDER_TYPE_STOP_LOSS_MARKET
	default:
		return common.OrderType_ORDER_TYPE_MARKET
	}
}

func stringToExchange(s string) common.Exchange {
	switch s {
	case "NSE":
		return common.Exchange_EXCHANGE_NSE
	case "BSE":
		return common.Exchange_EXCHANGE_BSE
	default:
		return common.Exchange_EXCHANGE_NSE
	}
}

func stringToOrderSide(s string) common.OrderSide {
	switch s {
	case "BUY":
		return common.OrderSide_ORDER_SIDE_BUY
	case "SELL":
		return common.OrderSide_ORDER_SIDE_SELL
	default:
		return common.OrderSide_ORDER_SIDE_BUY
	}
}

func stringToProductType(s string) common.ProductType {
	switch s {
	case "INTRADAY":
		return common.ProductType_PRODUCT_TYPE_INTRADAY
	case "DELIVERY":
		return common.ProductType_PRODUCT_TYPE_DELIVERY
	case "CASH":
		return common.ProductType_PRODUCT_TYPE_CASH
	default:
		return common.ProductType_PRODUCT_TYPE_INTRADAY
	}
}

func stringToStopLossType(s string) pb.StopLossType {
	switch s {
	case "FIXED":
		return pb.StopLossType_FIXED
	case "TRAILING":
		return pb.StopLossType_TRAILING
	default:
		return pb.StopLossType_FIXED
	}
}

func stringToPositionSizing(s string) common.PositionSizing {
	switch s {
	case "FIXED":
		return common.PositionSizing_POSITION_SIZING_FIXED
	case "PERCENTAGE":
		return common.PositionSizing_POSITION_SIZING_PERCENTAGE
	case "RISK_BASED":
		return common.PositionSizing_POSITION_SIZING_RISK_BASED
	default:
		return common.PositionSizing_POSITION_SIZING_FIXED
	}
}

// Helper functions for sentiments and exchanges (short codes)
func sentimentToString(s common.Sentiment) string {
	switch s {
	case common.Sentiment_SENTIMENT_POSITIVE:
		return "POSITIVE"
	case common.Sentiment_SENTIMENT_NEUTRAL:
		return "NEUTRAL"
	case common.Sentiment_SENTIMENT_NEGATIVE:
		return "NEGATIVE"
	default:
		return "POSITIVE"
	}
}

func stringToSentiment(s string) common.Sentiment {
	switch s {
	case "POSITIVE":
		return common.Sentiment_SENTIMENT_POSITIVE
	case "NEUTRAL":
		return common.Sentiment_SENTIMENT_NEUTRAL
	case "NEGATIVE":
		return common.Sentiment_SENTIMENT_NEGATIVE
	default:
		return common.Sentiment_SENTIMENT_POSITIVE
	}
}

func modelTradeConfigToProto(model *models.TradeConfig) *pb.TradeConfig {
	if model == nil {
		return nil
	}

	config := &pb.TradeConfig{
		OrderType:   stringToOrderType(model.OrderType),
		Quantity:    model.Quantity,
		Exchange:    stringToExchange(model.Exchange),
		OrderSide:   stringToOrderSide(model.OrderSide),
		Validity:    model.Validity,
		ProductType: stringToProductType(model.ProductType),
	}

	if model.MaxPositionSize != nil {
		config.MaxPositionSize = *model.MaxPositionSize
	}
	if model.StopLossPct != nil {
		config.StopLossPct = *model.StopLossPct
	}
	if model.TakeProfitPct != nil {
		config.TakeProfitPct = *model.TakeProfitPct
	}
	if model.LimitPrice != nil {
		config.LimitPrice = *model.LimitPrice
	}
	if model.StopLossType != "" {
		config.StopLossType = stringToStopLossType(model.StopLossType)
	}
	if model.TrailingSLPct != nil {
		config.TrailingSlPct = *model.TrailingSLPct
	}

	return config
}

func modelRiskLimitsToProto(model *models.RiskLimits) *pb.RiskLimits {
	if model == nil {
		return nil
	}

	limits := &pb.RiskLimits{
		PositionSizing:      stringToPositionSizing(model.PositionSizing),
		EnableRiskChecks:    model.EnableRiskChecks,
		EnableAutoSquareOff: model.EnableAutoSquareOff,
		AutoSquareOffTime:   model.AutoSquareOffTime,
	}

	if model.MaxDailyTrades != nil {
		limits.MaxDailyTrades = *model.MaxDailyTrades
	}
	if model.MaxLossPerDay != nil {
		limits.MaxLossPerDay = *model.MaxLossPerDay
	}
	if model.MaxPortfolioExposurePct != nil {
		limits.MaxPortfolioExposurePct = *model.MaxPortfolioExposurePct
	}
	if model.MaxPerTradeRisk != nil {
		limits.MaxPerTradeRisk = *model.MaxPerTradeRisk
	}

	return limits
}
