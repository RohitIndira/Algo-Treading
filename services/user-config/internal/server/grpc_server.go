package server

import (
	"context"
	"fmt"
	"strconv"

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
		UserID:                 req.UserId,
		StrategyName:           req.StrategyName,
		Description:            req.Description,
		Conditions:             protoConditionsToModel(req.Conditions),
		TradeConfig:            protoTradeConfigToModel(req.TradeConfig),
		RiskLimits:             protoRiskLimitsToModel(req.RiskLimits),
		ActivateImmediately:    req.ActivateImmediately,
		TradingMode:            protoTradingModeToModel(req.TradingMode),
		ProcessAfterMarketNews: req.ProcessAfterMarketNews,
		AMNSelectedStocks:      req.AmnSelectedStocks,
	}

	// Extract Indira auth context if provided
	if req.IndiraAuth != nil {
		modelReq.IndiraAuth = &models.IndiraAuthContext{
			UserID:      req.IndiraAuth.UserId,
			AppID:       req.IndiraAuth.AppId,
			Source:      req.IndiraAuth.Source,
			BearerToken: req.IndiraAuth.BearerToken,
		}
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
	if req.TradingMode != nil {
		mode := protoTradingModeToModel(*req.TradingMode)
		modelReq.TradingMode = &mode
	}
	if req.IndiraAuth != nil {
		modelReq.IndiraAuth = &models.IndiraAuthContext{
			UserID:      req.IndiraAuth.UserId,
			AppID:       req.IndiraAuth.AppId,
			Source:      req.IndiraAuth.Source,
			BearerToken: req.IndiraAuth.BearerToken,
		}
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

	strategies, total, err := s.service.ListUserStrategies(ctx, req.UserId, req.ActiveOnly, req.IncludeDeleted, pageSize, offset)
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

	// ActivateStrategyRequest has no IndiraAuth field in the proto.
	// Credential refresh is handled by the gateway calling UpdateUserCredentials separately.
	strategy, err := s.service.ActivateStrategy(ctx, strategyID, req.UserId, nil)
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

// UpdateUserCredentials stores fresh Indira credentials for a user.
// Called by the gateway on strategy activate to refresh the bearer token in DB.
func (s *UserConfigServer) UpdateUserCredentials(ctx context.Context, req *pb.UpdateUserCredentialsRequest) (*pb.UpdateUserCredentialsResponse, error) {
	if req.UserId == "" || req.IndiraAuth == nil || req.IndiraAuth.BearerToken == "" {
		return &pb.UpdateUserCredentialsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_REQUEST",
				Message: "user_id and indira_auth.bearer_token are required",
			},
		}, nil
	}

	auth := &models.IndiraAuthContext{
		UserID:      req.IndiraAuth.UserId,
		AppID:       req.IndiraAuth.AppId,
		Source:      req.IndiraAuth.Source,
		BearerToken: req.IndiraAuth.BearerToken,
	}

	if err := s.service.UpdateCredentials(ctx, req.UserId, auth); err != nil {
		return &pb.UpdateUserCredentialsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "UPDATE_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.UpdateUserCredentialsResponse{Success: true}, nil
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

// GetAllActiveStrategies returns ALL active strategies in a paginated manner.
// This is used by Rule Engine at startup for BulkLoad.
func (s *UserConfigServer) GetAllActiveStrategies(ctx context.Context, req *pb.GetAllActiveStrategiesRequest) (*pb.GetAllActiveStrategiesResponse, error) {
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = 500
	}
	if pageSize > 2000 {
		pageSize = 2000
	}

	offset := 0
	if req.GetPageToken() != "" {
		o, err := strconv.Atoi(req.GetPageToken())
		if err != nil || o < 0 {
			return &pb.GetAllActiveStrategiesResponse{
				Success: false,
				Error:   &common.Error{Code: "INVALID_PAGE_TOKEN", Message: "page_token must be an integer offset"},
			}, nil
		}
		offset = o
	}

	strategies, err := s.service.ListAllActiveStrategies(ctx, pageSize, offset)
	if err != nil {
		return &pb.GetAllActiveStrategiesResponse{
			Success: false,
			Error:   &common.Error{Code: "LIST_FAILED", Message: err.Error()},
		}, nil
	}

	protoStrategies := make([]*pb.Strategy, 0, len(strategies))
	for _, st := range strategies {
		protoStrategies = append(protoStrategies, modelStrategyToProto(st))
	}

	nextToken := ""
	if len(strategies) == pageSize {
		nextToken = strconv.Itoa(offset + pageSize)
	}

	return &pb.GetAllActiveStrategiesResponse{
		Success:       true,
		Strategies:    protoStrategies,
		NextPageToken: nextToken,
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

	marketCapTypes := make(pq.StringArray, len(proto.MarketCapTypes))
	copy(marketCapTypes, proto.MarketCapTypes)

	cond := &models.StrategyCondition{
		MatchAllNews:   proto.MatchAllNews,
		ImpactScoreMin: proto.ImpactScoreMin,
		ImpactScoreMax: proto.ImpactScoreMax,
		Sentiments:     sentiments,
		Categories:     pq.StringArray(proto.Categories),
		Exchanges:      exchanges,
		MarketCapTypes: marketCapTypes,
	}

	// PriceRange fields not present in DB/Model yet.
	/*
		if proto.PriceRange != nil {
			cond.PriceRangeMin = &proto.PriceRange.MinPrice
			cond.PriceRangeMax = &proto.PriceRange.MaxPrice
		}
	*/

	if proto.MarketCapRange != nil {
		cond.MinMarketCap = &proto.MarketCapRange.MinMcap
		cond.MaxMarketCap = &proto.MarketCapRange.MaxMcap
	}

	if proto.PctChangeRange != nil {
		cond.MinPriceChangePct = &proto.PctChangeRange.MinPctChange
		cond.MaxPriceChangePct = &proto.PctChangeRange.MaxPctChange
	}

	return cond
}

func protoTradingModeToModel(mode pb.TradingMode) models.TradingMode {
	switch mode {
	case pb.TradingMode_PAPER:
		return models.TradingModePaper
	case pb.TradingMode_LIVE:
		return models.TradingModeLive
	default:
		return models.TradingModePaper
	}
}

func modelTradingModeToProto(mode models.TradingMode) pb.TradingMode {
	switch mode {
	case models.TradingModePaper:
		return pb.TradingMode_PAPER
	case models.TradingModeLive:
		return pb.TradingMode_LIVE
	default:
		return pb.TradingMode_PAPER
	}
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
	case common.ProductType_PRODUCT_TYPE_BRACKET:
		return "BRACKET"
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
	case pb.StopLossType_MULTI_LEVEL:
		return "MULTI_LEVEL"
	default:
		return "FIXED"
	}
}

func takeProfitTypeToString(tpt pb.TakeProfitType) string {
	switch tpt {
	case pb.TakeProfitType_TAKE_PROFIT_MULTI_LEVEL:
		return "MULTI_LEVEL"
	default:
		return "FIXED"
	}
}

func protoMultiLevelToModel(levels []*pb.MultiLevelExitLevel) []models.MultiLevelExitLevel {
	out := make([]models.MultiLevelExitLevel, len(levels))
	for i, l := range levels {
		out[i] = models.MultiLevelExitLevel{
			LevelNum: int(l.LevelNum),
			PricePct: l.PricePct,
			QtyPct:   l.QtyPct,
		}
	}
	return out
}

func modelMultiLevelToProto(levels []models.MultiLevelExitLevel) []*pb.MultiLevelExitLevel {
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
		OrderType:      orderTypeToString(proto.OrderType),
		Quantity:       proto.Quantity,
		StopLossPct:    &proto.StopLossPct,
		TakeProfitPct:  &proto.TakeProfitPct,
		Exchange:       exchangeToString(proto.Exchange),
		OrderSide:      orderSideToString(proto.OrderSide),
		LimitPrice:     &proto.LimitPrice,
		Validity:       proto.Validity,
		StopLossType:   stopLossTypeToString(proto.StopLossType),
		TakeProfitType: takeProfitTypeToString(proto.TakeProfitType),
		TrailingSLPct:  &proto.TrailingSlPct,
		ProductType:    productTypeToString(proto.ProductType),
	}

	if len(proto.MultiLevelSl) > 0 {
		config.MultiLevelSL = protoMultiLevelToModel(proto.MultiLevelSl)
	}
	if len(proto.MultiLevelTp) > 0 {
		config.MultiLevelTP = protoMultiLevelToModel(proto.MultiLevelTp)
	}

	config.TradeWindowStart = proto.TradeWindowStart
	config.TradeWindowEnd = proto.TradeWindowEnd

	return config
}

func protoRiskLimitsToModel(proto *pb.RiskLimits) *models.RiskLimits {
	if proto == nil {
		return nil
	}

	limits := &models.RiskLimits{
		MaxDailyTrades:          &proto.MaxDailyTrades,
		MaxLossPerDay:           &proto.MaxLossPerDay,
		MaxPortfolioExposurePct: &proto.MaxPortfolioExposurePct,
		MaxPerTradeRisk:         &proto.MaxPerTradeRisk,
		EnableRiskChecks:        proto.EnableRiskChecks,
		EnableAutoSquareOff:     proto.EnableAutoSquareOff,
		AutoSquareOffTime:       proto.AutoSquareOffTime,
	}
	if proto.MaxAmountPerStock > 0 {
		v := proto.MaxAmountPerStock
		limits.MaxAmountPerStock = &v
	}
	if proto.MaxTradesPerStrategy > 0 {
		v := proto.MaxTradesPerStrategy
		limits.MaxTradesPerStrategy = &v
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
		TradingMode:  modelTradingModeToProto(model.TradingMode),
		Version:      model.Version,
		CreatedAt:    &common.Timestamp{Seconds: model.CreatedAt.Unix()},
		UpdatedAt:    &common.Timestamp{Seconds: model.UpdatedAt.Unix()},
	}
	if model.DeletedAt != nil {
		strategy.DeletedAt = &common.Timestamp{Seconds: model.DeletedAt.Unix()}
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

	marketCapTypes := make([]string, len(model.MarketCapTypes))
	copy(marketCapTypes, model.MarketCapTypes)

	cond := &pb.StrategyConditions{
		MatchAllNews:   model.MatchAllNews,
		ImpactScoreMin: model.ImpactScoreMin,
		ImpactScoreMax: model.ImpactScoreMax,
		Sentiments:     sentiments,
		Categories:     []string(model.Categories),
		Exchanges:      exchanges,
		MarketCapTypes: marketCapTypes,
	}

	if model.MinMarketCap != nil && model.MaxMarketCap != nil {
		cond.MarketCapRange = &pb.StrategyConditions_MarketCapRange{
			MinMcap: *model.MinMarketCap,
			MaxMcap: *model.MaxMarketCap,
		}
	}

	if model.MinPriceChangePct != nil && model.MaxPriceChangePct != nil {
		cond.PctChangeRange = &pb.StrategyConditions_PctChangeRange{
			MinPctChange: *model.MinPriceChangePct,
			MaxPctChange: *model.MaxPriceChangePct,
		}
	}

	return cond
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
	if model.TakeProfitType != "" {
		config.TakeProfitType = stringToTakeProfitType(model.TakeProfitType)
	}
	if model.TrailingSLPct != nil {
		config.TrailingSlPct = *model.TrailingSLPct
	}
	if len(model.MultiLevelSL) > 0 {
		config.MultiLevelSl = modelMultiLevelToProto(model.MultiLevelSL)
	}
	if len(model.MultiLevelTP) > 0 {
		config.MultiLevelTp = modelMultiLevelToProto(model.MultiLevelTP)
	}

	config.TradeWindowStart = model.TradeWindowStart
	config.TradeWindowEnd = model.TradeWindowEnd

	return config
}

func modelRiskLimitsToProto(model *models.RiskLimits) *pb.RiskLimits {
	if model == nil {
		return nil
	}

	limits := &pb.RiskLimits{
		// PositionSizing:      stringToPositionSizing(model.PositionSizing), // Removed from model
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
	if model.MaxAmountPerStock != nil {
		limits.MaxAmountPerStock = *model.MaxAmountPerStock
	}
	if model.MaxTradesPerStrategy != nil {
		limits.MaxTradesPerStrategy = *model.MaxTradesPerStrategy
	}

	return limits
}
