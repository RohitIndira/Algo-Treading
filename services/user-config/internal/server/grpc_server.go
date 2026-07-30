package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/apperr"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/service"
	"github.com/google/uuid"
)

// errorCodeFor classifies a service/repository error into the semantic code
// string that api-gateway maps to an HTTP status (see the api-gateway
// handlers' mapErrorCodeToHTTPStatus). It is the single place that turns the
// apperr sentinels into the cross-service error contract:
//
//	ErrValidation                         → INVALID_ARGUMENT    (400)
//	ErrNotFound                           → NOT_FOUND           (404)
//	ErrVersionConflict / ErrTerminalState → FAILED_PRECONDITION (412)
//	ErrDuplicate                          → ALREADY_EXISTS      (409)
//	ErrUnauthorized                       → PERMISSION_DENIED   (403)
//	anything else                         → INTERNAL            (500)
func errorCodeFor(err error) string {
	switch {
	case errors.Is(err, apperr.ErrValidation):
		return "INVALID_ARGUMENT"
	case errors.Is(err, apperr.ErrNotFound):
		return "NOT_FOUND"
	case errors.Is(err, apperr.ErrVersionConflict), errors.Is(err, apperr.ErrTerminalState):
		return "FAILED_PRECONDITION"
	case errors.Is(err, apperr.ErrDuplicate):
		return "ALREADY_EXISTS"
	case errors.Is(err, apperr.ErrUnauthorized):
		return "PERMISSION_DENIED"
	default:
		return "INTERNAL"
	}
}

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
		StrategyType:        protoStrategyTypeToModel(req.StrategyType),
		Conditions:          protoConditionsToModel(req.Conditions),
		TradeConfig:         protoTradeConfigToModel(req.TradeConfig),
		RiskLimits:          protoRiskLimitsToModel(req.RiskLimits),
		ActivateImmediately: req.ActivateImmediately,
		TradingMode:         protoTradingModeToModel(req.TradingMode),
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
				Code:    errorCodeFor(err),
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
				Code:    "INVALID_ARGUMENT",
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

	strategy, err := s.service.UpdateStrategy(ctx, modelReq)
	if err != nil {
		return &pb.UpdateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    errorCodeFor(err),
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.UpdateStrategyResponse{
		Success:  true,
		Strategy: modelStrategyToProto(strategy),
	}, nil
}

// DeleteStrategy deletes a strategy. When
// req.position_handling = SQUARE_OFF_AT_MARKET, user-config first calls
// trade-execution to place reverse market/aggressive-limit exit orders
// for every ACTIVE position of the strategy; the delete is gated on
// that succeeding so a failing exit surfaces to the UI atomically.
func (s *UserConfigServer) DeleteStrategy(ctx context.Context, req *pb.DeleteStrategyRequest) (*pb.DeleteStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.DeleteStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_ARGUMENT",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	positionsExited, err := s.service.DeleteStrategy(ctx, strategyID, req.UserId,
		forceExitParamsFromProto(req.PositionHandling, req.IndiraAuth))
	if err != nil {
		return &pb.DeleteStrategyResponse{
			Success: false,
			Message: "",
			Error: &common.Error{
				Code:    errorCodeFor(err),
				Message: err.Error(),
			},
			PositionsExited: int32(positionsExited),
		}, nil
	}

	return &pb.DeleteStrategyResponse{
		Success:         true,
		Message:         "Strategy deleted successfully",
		PositionsExited: int32(positionsExited),
	}, nil
}

// GetStrategy retrieves a specific strategy
func (s *UserConfigServer) GetStrategy(ctx context.Context, req *pb.GetStrategyRequest) (*pb.GetStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.GetStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_ARGUMENT",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	strategy, err := s.service.GetStrategy(ctx, strategyID, req.UserId)
	if err != nil {
		return &pb.GetStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    errorCodeFor(err),
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
				Code:    errorCodeFor(err),
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
				Code:    "INVALID_ARGUMENT",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	strategy, err := s.service.ActivateStrategy(ctx, strategyID, req.UserId)
	if err != nil {
		return &pb.ActivateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    errorCodeFor(err),
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.ActivateStrategyResponse{
		Success:  true,
		Strategy: modelStrategyToProto(strategy),
	}, nil
}

// DeactivateStrategy pauses a strategy. When
// req.position_handling = SQUARE_OFF_AT_MARKET, user-config first calls
// trade-execution to place reverse exit orders for every ACTIVE position
// of the strategy; the deactivate is gated on that succeeding so a
// failing exit surfaces to the UI atomically (matches the mockup's
// "Algo Pause Failed" flow).
func (s *UserConfigServer) DeactivateStrategy(ctx context.Context, req *pb.DeactivateStrategyRequest) (*pb.DeactivateStrategyResponse, error) {
	strategyID, err := uuid.Parse(req.StrategyId)
	if err != nil {
		return &pb.DeactivateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_ARGUMENT",
				Message: "Invalid strategy ID format",
			},
		}, nil
	}

	strategy, positionsExited, err := s.service.DeactivateStrategy(ctx, strategyID, req.UserId,
		forceExitParamsFromProto(req.PositionHandling, req.IndiraAuth))
	if err != nil {
		return &pb.DeactivateStrategyResponse{
			Success: false,
			Error: &common.Error{
				Code:    errorCodeFor(err),
				Message: err.Error(),
			},
			PositionsExited: int32(positionsExited),
		}, nil
	}

	return &pb.DeactivateStrategyResponse{
		Success:         true,
		Strategy:        modelStrategyToProto(strategy),
		PositionsExited: int32(positionsExited),
	}, nil
}

// forceExitParamsFromProto folds the position_handling enum + optional
// Indira auth block from any lifecycle request into the internal
// ForceExitParams shape. UNSPECIFIED + KEEP_POSITIONS_OPEN both map to
// SquareOff=false so no exit call is made.
func forceExitParamsFromProto(ph pb.PositionHandling, auth *common.IndiraAuthContext) service.ForceExitParams {
	p := service.ForceExitParams{
		SquareOff: ph == pb.PositionHandling_SQUARE_OFF_AT_MARKET,
	}
	if p.SquareOff && auth != nil {
		p.BearerToken = auth.BearerToken
		p.AppID = auth.AppId
		p.Source = auth.Source
	}
	return p
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
					Code:    "INVALID_ARGUMENT",
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
				Code:    errorCodeFor(err),
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

// UpdateUserCredentials persists the user's broker auth (called from the SSO
// login flow + JWT-refresh flow). Encrypts the bearer token, upserts
// execution_db.user_credentials, and publishes USER_CREDENTIALS_UPDATED
// to user-config-events so trade-execution can invalidate its CredentialsCache.
//
// Idempotent: ON CONFLICT (user_id) DO UPDATE on the underlying table — calling
// this multiple times with the same JWT is safe (latest call wins).
func (s *UserConfigServer) UpdateUserCredentials(ctx context.Context, req *pb.UpdateUserCredentialsRequest) (*pb.UpdateUserCredentialsResponse, error) {
	if req == nil || req.IndiraAuth == nil {
		return &pb.UpdateUserCredentialsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_REQUEST",
				Message: "user_id and indira_auth are required",
			},
		}, nil
	}
	if req.UserId == "" || req.IndiraAuth.BearerToken == "" {
		return &pb.UpdateUserCredentialsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_REQUEST",
				Message: "user_id and bearer_token are required",
			},
		}, nil
	}

	if err := s.service.UpdateUserCredentials(ctx, service.UpdateUserCredentialsRequest{
		UserID:       req.UserId,
		IndiraUserID: req.IndiraAuth.UserId,
		AppID:        req.IndiraAuth.AppId,
		Source:       req.IndiraAuth.Source,
		BearerToken:  req.IndiraAuth.BearerToken,
	}); err != nil {
		return &pb.UpdateUserCredentialsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "PERSIST_FAILED",
				Message: err.Error(),
			},
		}, nil
	}
	return &pb.UpdateUserCredentialsResponse{Success: true}, nil
}

// GetUserCredentials returns the user's decrypted broker credentials wrapped
// in an IndiraAuthContext. user-config is the SINGLE READER for this data —
// other services must call this RPC instead of querying the DB directly.
//
// Audit: every call is logged with caller hint + user id so SEBI auditors
// can answer "who accessed user X's credentials and when".
//
// Errors:
//   - INVALID_REQUEST  if req.UserId is empty
//   - NOT_FOUND        if no credentials exist for the user
//   - INTERNAL         on DB or decryption failure
func (s *UserConfigServer) GetUserCredentials(ctx context.Context, req *pb.GetUserCredentialsRequest) (*pb.GetUserCredentialsResponse, error) {
	if req == nil || req.UserId == "" {
		return &pb.GetUserCredentialsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INVALID_REQUEST",
				Message: "user_id is required",
			},
		}, nil
	}

	creds, err := s.service.GetUserCredentials(ctx, req.UserId)
	if err != nil {
		if errors.Is(err, repository.ErrCredentialsNotFound) {
			return &pb.GetUserCredentialsResponse{
				Success: false,
				Error: &common.Error{
					Code:    "NOT_FOUND",
					Message: fmt.Sprintf("no credentials for user %s", req.UserId),
				},
			}, nil
		}
		log.Printf("[user-config] GetUserCredentials error user=%s: %v", req.UserId, err)
		return &pb.GetUserCredentialsResponse{
			Success: false,
			Error: &common.Error{
				Code:    "INTERNAL",
				Message: "failed to read credentials",
			},
		}, nil
	}

	// Compliance audit — who is asking for whose credentials? Format mirrors
	// other audit lines so a single grep finds them. Do NOT log the token.
	log.Printf("[user-config] AUDIT credentials_read user=%s source=%s app_id=%s",
		req.UserId, creds.Source, creds.AppID)

	return &pb.GetUserCredentialsResponse{
		Success: true,
		IndiraAuth: &common.IndiraAuthContext{
			UserId:      creds.IndiraUserID,
			AppId:       creds.AppID,
			Source:      creds.Source,
			BearerToken: creds.BearerToken, // decrypted; transport is gRPC over mTLS in prod
		},
	}, nil
}

// Helper functions to convert between proto and model types

// protoConditionsToModel constructs an empty StrategyCondition placeholder.
// The 13 news-specific fields were removed 2026-07-20 because MANTHAN doesn't
// use them; incoming proto fields are silently ignored.
func protoConditionsToModel(proto *pb.StrategyConditions) *models.StrategyCondition {
	if proto == nil {
		return nil
	}
	return &models.StrategyCondition{}
}

func modelStrategyTypeToProto(t models.StrategyType) pb.StrategyType {
	// Only MANTHAN survives after 2026-07-20 cleanup; everything else maps to it.
	return pb.StrategyType_MANTHAN
}

func positionSizingModeToString(m pb.PositionSizingMode) string {
	switch m {
	case pb.PositionSizingMode_EMA_ALLOCATION:
		return "EMA_ALLOCATION"
	case pb.PositionSizingMode_FIXED_QTY:
		return "FIXED_QTY"
	default:
		return "FIXED_QTY"
	}
}

func protoStrategyTypeToModel(t pb.StrategyType) models.StrategyType {
	// Only MANTHAN survives after 2026-07-20 cleanup; everything else maps to it.
	return models.StrategyTypeManthan
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

	totalCap := proto.TotalCapital
	maxPos := proto.MaxPositions
	perStock := proto.PerStockAmount

	config := &models.TradeConfig{
		OrderType:          orderTypeToString(proto.OrderType),
		Quantity:           proto.Quantity,
		StopLossPct:        &proto.StopLossPct,
		TakeProfitPct:      &proto.TakeProfitPct,
		Exchange:           exchangeToString(proto.Exchange),
		OrderSide:          orderSideToString(proto.OrderSide),
		LimitPrice:         &proto.LimitPrice,
		Validity:           proto.Validity,
		StopLossType:       stopLossTypeToString(proto.StopLossType),
		TrailingSLPct:      &proto.TrailingSlPct,
		ProductType:        productTypeToString(proto.ProductType),
		PositionSizingMode: positionSizingModeToString(proto.PositionSizingMode),
		TotalCapital:       &totalCap,
		MaxPositions:       &maxPos,
		PerStockAmount:     &perStock,
	}

	return config
}

// protoRiskLimitsToModel returns an empty RiskLimits placeholder. The 7 risk
// fields were dropped 2026-07-30 (migration 017); incoming proto fields are
// silently ignored (proto keeps them for wire compat). Same pattern as
// protoConditionsToModel.
func protoRiskLimitsToModel(proto *pb.RiskLimits) *models.RiskLimits {
	if proto == nil {
		return nil
	}
	return &models.RiskLimits{}
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
		StrategyType: modelStrategyTypeToProto(model.StrategyType),
		TradingMode:  modelTradingModeToProto(model.TradingMode),
		Version:      model.Version,
		CreatedAt:    &common.Timestamp{Seconds: model.CreatedAt.Unix()},
		UpdatedAt:    &common.Timestamp{Seconds: model.UpdatedAt.Unix()},
	}

	// stopped_at is nullable — only set on the proto if the row has
	// been STOPPED, so the client reads absent-vs-present as the
	// stop-vs-not-stopped signal.
	if model.StoppedAt != nil {
		strategy.StoppedAt = &common.Timestamp{Seconds: model.StoppedAt.Unix()}
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

// modelConditionsToProto returns an empty StrategyConditions envelope.
// News-specific fields dropped 2026-07-20 (MANTHAN doesn't use them).
func modelConditionsToProto(model *models.StrategyCondition) *pb.StrategyConditions {
	if model == nil {
		return nil
	}
	return &pb.StrategyConditions{}
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
	if model.TrailingSLPct != nil {
		config.TrailingSlPct = *model.TrailingSLPct
	}

	// 52W fields
	config.PositionSizingMode = stringToPositionSizingMode(model.PositionSizingMode)
	if model.TotalCapital != nil {
		config.TotalCapital = *model.TotalCapital
	}
	if model.MaxPositions != nil {
		config.MaxPositions = *model.MaxPositions
	}
	if model.PerStockAmount != nil {
		config.PerStockAmount = *model.PerStockAmount
	}

	return config
}

func stringToPositionSizingMode(s string) pb.PositionSizingMode {
	switch s {
	case "EMA_ALLOCATION":
		return pb.PositionSizingMode_EMA_ALLOCATION
	case "FIXED_QTY":
		return pb.PositionSizingMode_FIXED_QTY
	default:
		return pb.PositionSizingMode_FIXED_QTY
	}
}

// modelRiskLimitsToProto returns an empty RiskLimits envelope. The 7 risk
// fields were dropped 2026-07-30 (migration 017); the proto message is kept for
// wire compat but user-config produces empty values. Same pattern as
// modelConditionsToProto.
func modelRiskLimitsToProto(model *models.RiskLimits) *pb.RiskLimits {
	if model == nil {
		return nil
	}
	return &pb.RiskLimits{}
}
