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
		BearerToken:         req.BearerToken,
		AppId:               req.AppId,
		Source:              req.Source,
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

	// proto.Exchanges is a slice of enum values; convert each to its string name
	exchanges := make(pq.StringArray, len(proto.Exchanges))
	for i, e := range proto.Exchanges {
		exchanges[i] = e.String()
	}

	stockCodes := make(pq.Int64Array, len(proto.StockCodes))
	for i, code := range proto.StockCodes {
		stockCodes[i] = code
	}

	cond := &models.StrategyCondition{
		StockCodes:           stockCodes,
		Exchanges:            exchanges,
		ImpactScoreThreshold: 5, // Default value for depth-based trading
	}

	if proto.PriceRange != nil {
		cond.PriceRangeMin = &proto.PriceRange.MinPrice
		cond.PriceRangeMax = &proto.PriceRange.MaxPrice
	}

	if proto.MarketCapRange != nil {
		cond.MinMarketCap = &proto.MarketCapRange.MinMcap
		cond.MaxMarketCap = &proto.MarketCapRange.MaxMcap
	}

	// Depth-related fields (market depth based trading)
	if proto.MinBidQuantity != 0 {
		cond.MinBidQuantity = &proto.MinBidQuantity
	}
	if proto.MinAskQuantity != 0 {
		cond.MinAskQuantity = &proto.MinAskQuantity
	}
	if proto.MaxSpreadPct != 0 {
		cond.MaxSpreadPct = &proto.MaxSpreadPct
	}
	if proto.VolumeThreshold != 0 {
		cond.VolumeThreshold = &proto.VolumeThreshold
	}
	if proto.PctChangeThreshold != 0 {
		cond.PctChangeThreshold = &proto.PctChangeThreshold
	}
	if proto.RequireLtpBetweenSpread {
		v := true
		cond.RequireLTPBetweenSpread = &v
	}

	return cond
}

func protoTradeConfigToModel(proto *pb.TradeConfig) *models.TradeConfig {
	if proto == nil {
		return nil
	}

	config := &models.TradeConfig{
		OrderType:       proto.OrderType.String(),
		Quantity:        proto.Quantity,
		MaxPositionSize: &proto.MaxPositionSize,
		StopLossPct:     &proto.StopLossPct,
		TakeProfitPct:   &proto.TakeProfitPct,
		Exchange:        proto.Exchange.String(),
		OrderSide:       proto.OrderSide.String(),
		LimitPrice:      &proto.LimitPrice,
		Validity:        proto.Validity,
		StopLossType:    proto.StopLossType.String(),
		TrailingSLPct:   &proto.TrailingSlPct,
		ProductType:     proto.ProductType,
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
		PositionSizing:          proto.PositionSizing.String(),
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

	// model.Exchanges holds enum names as strings; convert to enum values
	exchanges := make([]common.Exchange, len(model.Exchanges))
	for i, e := range model.Exchanges {
		if val, ok := common.Exchange_value[e]; ok {
			exchanges[i] = common.Exchange(val)
		} else {
			// fallback to ZERO value (0) if unknown
			exchanges[i] = common.Exchange(0)
		}
	}

	stockCodes := make([]int64, len(model.StockCodes))
	for i, code := range model.StockCodes {
		stockCodes[i] = code
	}

	cond := &pb.StrategyConditions{
		StockCodes: stockCodes,
		Exchanges:  exchanges,
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

	// Depth-related fields (market depth based trading)
	if model.MinBidQuantity != nil {
		cond.MinBidQuantity = *model.MinBidQuantity
	}
	if model.MinAskQuantity != nil {
		cond.MinAskQuantity = *model.MinAskQuantity
	}
	if model.MaxSpreadPct != nil {
		cond.MaxSpreadPct = *model.MaxSpreadPct
	}
	if model.RequireLTPBetweenSpread != nil {
		cond.RequireLtpBetweenSpread = *model.RequireLTPBetweenSpread
	}

	return cond
}

func modelTradeConfigToProto(model *models.TradeConfig) *pb.TradeConfig {
	if model == nil {
		return nil
	}

	config := &pb.TradeConfig{
		OrderType:   common.OrderType(common.OrderType_value[model.OrderType]),
		Quantity:    model.Quantity,
		Exchange:    common.Exchange(common.Exchange_value[model.Exchange]),
		OrderSide:   common.OrderSide(common.OrderSide_value[model.OrderSide]),
		Validity:    model.Validity,
		ProductType: model.ProductType,
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
		config.StopLossType = pb.StopLossType(pb.StopLossType_value[model.StopLossType])
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
		PositionSizing:      common.PositionSizing(common.PositionSizing_value[model.PositionSizing]),
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
