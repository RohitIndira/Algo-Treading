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

// ConfigureCash52WeekStrategy configures the managed Cash 52-week High strategy
// for a user. It exposes only high-level fields (enabled, capital_per_stock)
// to callers; the service fills in all detailed trade/risk settings.
func (s *UserConfigServer) ConfigureCash52WeekStrategy(ctx context.Context, req *pb.ConfigureCash52WeekStrategyRequest) (*pb.ConfigureCash52WeekStrategyResponse, error) {
	modelReq := &models.ConfigureCash52WeekStrategyRequest{
		UserID:          req.UserId,
		Enabled:         req.Enabled,
		CapitalPerStock: req.CapitalPerStock,
		// max_positions, stop_loss_pct, take_profit_pct and risk_profile are
		// optional; backend will default them if zero/empty.
		MaxPositions:  int(req.MaxPositions),
		StopLossPct:   req.StopLossPct,
		TakeProfitPct: req.TakeProfitPct,
		RiskProfile:   req.RiskProfile,
		// Forward per-user trading_mode ("LIVE"/"PAPER") from the proto
		// request into the domain model so the service can normalise and
		// persist it. If this is empty or anything other than PAPER, the
		// service layer will default it to LIVE.
		TradingMode: req.TradingMode,
	}

	strategy, err := s.service.ConfigureCash52WeekStrategy(ctx, modelReq)
	if err != nil {
		return &pb.ConfigureCash52WeekStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "CONFIGURE_52W_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	var protoStrategy *pb.Strategy
	if strategy != nil {
		protoStrategy = modelStrategyToProto(strategy)
	}

	return &pb.ConfigureCash52WeekStrategyResponse{
		Success:  true,
		Strategy: protoStrategy,
	}, nil
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

// ============================================================================
// PHASE 1: Enhanced Cash52W Configuration gRPC Handlers
// ============================================================================

// ConfigureCash52WStrategyEnhanced configures the FULL Phase 1 enhanced 52W strategy
// with multi-level profit/SL, portfolio config, and manual controls
func (s *UserConfigServer) ConfigureCash52WStrategyEnhanced(ctx context.Context, req *pb.ConfigureCash52WStrategyEnhancedRequest) (*pb.ConfigureCash52WStrategyEnhancedResponse, error) {
	// Convert proto to model
	cfg := &models.Cash52WConfig{
		UserID:          req.UserId,
		Enabled:         req.Enabled,
		TotalCapital:    req.TotalCapital,
		CapitalPerStock: req.CapitalPerStock,
		MaxStocks:       int(req.MaxStocks),
		AutoRebalance:   req.AutoRebalance,
		TradingMode:     req.TradingMode,
		ForceExitAll:    req.ForceExitAll,
		ForceExitStocks: req.ForceExitStocks,
		PauseNewEntries: req.PauseNewEntries,
	}

	// Convert stop-loss levels
	if req.StopLossLevels != nil {
		cfg.StopLossLevels = models.StopLossLevels{
			Level1: models.StopLossLevel{
				TriggerPercent:      req.StopLossLevels.Level_1.TriggerPercent,
				ExitQuantityPercent: int(req.StopLossLevels.Level_1.ExitQuantityPercent),
				Type:                req.StopLossLevels.Level_1.Type,
				Enabled:             req.StopLossLevels.Level_1.Enabled,
			},
			Level2: models.StopLossLevel{
				TriggerPercent:      req.StopLossLevels.Level_2.TriggerPercent,
				ExitQuantityPercent: int(req.StopLossLevels.Level_2.ExitQuantityPercent),
				Type:                req.StopLossLevels.Level_2.Type,
				Enabled:             req.StopLossLevels.Level_2.Enabled,
			},
		}
	}

	// Convert profit levels
	if req.ProfitLevels != nil {
		cfg.ProfitLevels = models.ProfitLevels{
			Level1: models.ProfitLevel{
				TriggerPercent:      req.ProfitLevels.Level_1.TriggerPercent,
				ExitQuantityPercent: int(req.ProfitLevels.Level_1.ExitQuantityPercent),
				Type:                req.ProfitLevels.Level_1.Type,
				Enabled:             req.ProfitLevels.Level_1.Enabled,
			},
			Level2: models.ProfitLevel{
				TriggerPercent:      req.ProfitLevels.Level_2.TriggerPercent,
				ExitQuantityPercent: int(req.ProfitLevels.Level_2.ExitQuantityPercent),
				Type:                req.ProfitLevels.Level_2.Type,
				Enabled:             req.ProfitLevels.Level_2.Enabled,
			},
			Level3: models.ProfitLevel{
				TriggerPercent:      req.ProfitLevels.Level_3.TriggerPercent,
				ExitQuantityPercent: int(req.ProfitLevels.Level_3.ExitQuantityPercent),
				Type:                req.ProfitLevels.Level_3.Type,
				TrailPercent:        req.ProfitLevels.Level_3.TrailPercent,
				Enabled:             req.ProfitLevels.Level_3.Enabled,
			},
		}
	}

	// Call service
	result, err := s.service.ConfigureCash52WStrategyEnhanced(ctx, cfg)
	if err != nil {
		return &pb.ConfigureCash52WStrategyEnhancedResponse{
			Success: false,
			Error: &common.Error{
				Code:    "CONFIGURE_ENHANCED_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	// Convert back to proto
	return &pb.ConfigureCash52WStrategyEnhancedResponse{
		Success: true,
		Config:  modelCash52WConfigToProto(result),
	}, nil
}

// GetCash52WConfig retrieves the Phase 1 configuration for a user
func (s *UserConfigServer) GetCash52WConfig(ctx context.Context, req *pb.GetCash52WConfigRequest) (*pb.GetCash52WConfigResponse, error) {
	cfg, err := s.service.GetCash52WConfig(ctx, req.UserId)
	if err != nil {
		return &pb.GetCash52WConfigResponse{
			Success: false,
			Error: &common.Error{
				Code:    "GET_CONFIG_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.GetCash52WConfigResponse{
		Success: true,
		Config:  modelCash52WConfigToProto(cfg),
	}, nil
}

// ForceExitAll triggers emergency exit for all positions
func (s *UserConfigServer) ForceExitAll(ctx context.Context, req *pb.ForceExitAllRequest) (*pb.ForceExitAllResponse, error) {
	err := s.service.ForceExitAll(ctx, req.UserId)
	if err != nil {
		return &pb.ForceExitAllResponse{
			Success: false,
			Message: "",
			Error: &common.Error{
				Code:    "FORCE_EXIT_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.ForceExitAllResponse{
		Success: true,
		Message: "Force exit all triggered successfully",
	}, nil
}

// ForceExitStocks triggers exit for specific stocks
func (s *UserConfigServer) ForceExitStocks(ctx context.Context, req *pb.ForceExitStocksRequest) (*pb.ForceExitStocksResponse, error) {
	err := s.service.ForceExitStocks(ctx, req.UserId, req.Stocks)
	if err != nil {
		return &pb.ForceExitStocksResponse{
			Success: false,
			Message: "",
			Error: &common.Error{
				Code:    "FORCE_EXIT_STOCKS_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.ForceExitStocksResponse{
		Success: true,
		Message: fmt.Sprintf("Force exit triggered for %d stocks", len(req.Stocks)),
	}, nil
}

// UpdateManualControls updates manual control flags
func (s *UserConfigServer) UpdateManualControls(ctx context.Context, req *pb.UpdateManualControlsRequest) (*pb.UpdateManualControlsResponse, error) {
	err := s.service.UpdateManualControls(ctx, req.UserId, req.PauseNewEntries, req.ResetForceExit)
	if err != nil {
		return &pb.UpdateManualControlsResponse{
			Success: false,
			Message: "",
			Error: &common.Error{
				Code:    "UPDATE_CONTROLS_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.UpdateManualControlsResponse{
		Success: true,
		Message: "Manual controls updated successfully",
	}, nil
}

// DisableCash52W disables the 52W strategy for a user
func (s *UserConfigServer) DisableCash52W(ctx context.Context, req *pb.DisableCash52WRequest) (*pb.DisableCash52WResponse, error) {
	err := s.service.DisableCash52W(ctx, req.UserId)
	if err != nil {
		return &pb.DisableCash52WResponse{
			Success: false,
			Message: "",
			Error: &common.Error{
				Code:    "DISABLE_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.DisableCash52WResponse{
		Success: true,
		Message: "Cash 52W strategy disabled successfully",
	}, nil
}

// GetAllEnabledConfigs retrieves all enabled 52W configurations (admin/monitoring)
func (s *UserConfigServer) GetAllEnabledConfigs(ctx context.Context, req *pb.GetAllEnabledConfigsRequest) (*pb.GetAllEnabledConfigsResponse, error) {
	configs, err := s.service.GetAllEnabledConfigs(ctx)
	if err != nil {
		return &pb.GetAllEnabledConfigsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "GET_ALL_FAILED",
				Message: err.Error(),
			},
		}, nil
	}

	protoConfigs := make([]*pb.Cash52WConfig, len(configs))
	for i, cfg := range configs {
		protoConfigs[i] = modelCash52WConfigToProto(cfg)
	}

	return &pb.GetAllEnabledConfigsResponse{
		Success: true,
		Configs: protoConfigs,
	}, nil
}

// modelCash52WConfigToProto converts model to proto
func modelCash52WConfigToProto(model *models.Cash52WConfig) *pb.Cash52WConfig {
	if model == nil {
		return nil
	}

	return &pb.Cash52WConfig{
		UserId:          model.UserID,
		Enabled:         model.Enabled,
		TotalCapital:    model.TotalCapital,
		CapitalPerStock: model.CapitalPerStock,
		MaxStocks:       int32(model.MaxStocks),
		AutoRebalance:   model.AutoRebalance,
		StopLossLevels: &pb.StopLossLevels{
			Level_1: &pb.StopLossLevel{
				TriggerPercent:      model.StopLossLevels.Level1.TriggerPercent,
				ExitQuantityPercent: int32(model.StopLossLevels.Level1.ExitQuantityPercent),
				Type:                model.StopLossLevels.Level1.Type,
				Enabled:             model.StopLossLevels.Level1.Enabled,
			},
			Level_2: &pb.StopLossLevel{
				TriggerPercent:      model.StopLossLevels.Level2.TriggerPercent,
				ExitQuantityPercent: int32(model.StopLossLevels.Level2.ExitQuantityPercent),
				Type:                model.StopLossLevels.Level2.Type,
				Enabled:             model.StopLossLevels.Level2.Enabled,
			},
		},
		ProfitLevels: &pb.ProfitLevels{
			Level_1: &pb.ProfitLevel{
				TriggerPercent:      model.ProfitLevels.Level1.TriggerPercent,
				ExitQuantityPercent: int32(model.ProfitLevels.Level1.ExitQuantityPercent),
				Type:                model.ProfitLevels.Level1.Type,
				Enabled:             model.ProfitLevels.Level1.Enabled,
			},
			Level_2: &pb.ProfitLevel{
				TriggerPercent:      model.ProfitLevels.Level2.TriggerPercent,
				ExitQuantityPercent: int32(model.ProfitLevels.Level2.ExitQuantityPercent),
				Type:                model.ProfitLevels.Level2.Type,
				Enabled:             model.ProfitLevels.Level2.Enabled,
			},
			Level_3: &pb.ProfitLevel{
				TriggerPercent:      model.ProfitLevels.Level3.TriggerPercent,
				ExitQuantityPercent: int32(model.ProfitLevels.Level3.ExitQuantityPercent),
				Type:                model.ProfitLevels.Level3.Type,
				TrailPercent:        model.ProfitLevels.Level3.TrailPercent,
				Enabled:             model.ProfitLevels.Level3.Enabled,
			},
		},
		TradingMode:     model.TradingMode,
		ForceExitAll:    model.ForceExitAll,
		ForceExitStocks: model.ForceExitStocks,
		PauseNewEntries: model.PauseNewEntries,
		UpdatedAt:       &common.Timestamp{Seconds: model.UpdatedAt.Unix()},
		Version:         int32(model.Version),
	}
}

// Helper functions to convert between proto and model types

func protoConditionsToModel(proto *pb.StrategyConditions) *models.StrategyCondition {
	if proto == nil {
		return nil
	}

	sentiments := make(pq.StringArray, len(proto.Sentiments))
	for i, s := range proto.Sentiments {
		sentiments[i] = s.String()
	}

	exchanges := make(pq.StringArray, len(proto.Exchanges))
	for i, e := range proto.Exchanges {
		exchanges[i] = e.String()
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
		// Propagate per-strategy trading_mode ("LIVE"/"PAPER") from the
		// domain model to the protobuf response so callers can see the
		// effective mode in JSON responses.
		TradingMode:  model.TradingMode,
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
		sentiments[i] = common.Sentiment(common.Sentiment_value[s])
	}

	exchanges := make([]common.Exchange, len(model.Exchanges))
	for i, e := range model.Exchanges {
		exchanges[i] = common.Exchange(common.Exchange_value[e])
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

func modelTradeConfigToProto(model *models.TradeConfig) *pb.TradeConfig {
	if model == nil {
		return nil
	}

	config := &pb.TradeConfig{
		OrderType: common.OrderType(common.OrderType_value[model.OrderType]),
		Quantity:  model.Quantity,
		Exchange:  common.Exchange(common.Exchange_value[model.Exchange]),
		OrderSide: common.OrderSide(common.OrderSide_value[model.OrderSide]),
		Validity:  model.Validity,
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

	return config
}

func modelRiskLimitsToProto(model *models.RiskLimits) *pb.RiskLimits {
	if model == nil {
		return nil
	}

	limits := &pb.RiskLimits{
		PositionSizing:   common.PositionSizing(common.PositionSizing_value[model.PositionSizing]),
		EnableRiskChecks: model.EnableRiskChecks,
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
