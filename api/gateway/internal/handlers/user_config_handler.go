package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/gorilla/mux"
)

type UserConfigHandler struct {
	client *grpc_clients.UserConfigClient
}

func NewUserConfigHandler(client *grpc_clients.UserConfigClient) *UserConfigHandler {
	return &UserConfigHandler{
		client: client,
	}
}

// CreateStrategy handles POST /api/v1/strategies
func (h *UserConfigHandler) CreateStrategy(w http.ResponseWriter, r *http.Request) {
	var req pb.CreateStrategyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	// Extract authentication headers from frontend
	bearerToken := r.Header.Get("Authorization")
	if bearerToken != "" {
		// Remove "Bearer " prefix if present
		bearerToken = strings.TrimPrefix(bearerToken, "Bearer ")
		req.BearerToken = bearerToken
	}

	appId := r.Header.Get("appId")
	if appId != "" {
		req.AppId = appId
	}

	source := r.Header.Get("source")
	if source != "" {
		req.Source = source
	}

	// Validate authentication data
	if req.BearerToken == "" || req.AppId == "" || req.Source == "" {
		respondWithError(w, http.StatusBadRequest, "Missing authentication headers: Authorization, appId, and source are required")
		return
	}

	resp, err := h.client.CreateStrategy(r.Context(), &req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to create strategy: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusCreated, resp)
}

// CreateDepthMarketStrategy handles POST /api/v1/strategies/depth-market/create
// Specialized endpoint for creating depth market trading strategies
func (h *UserConfigHandler) CreateDepthMarketStrategy(w http.ResponseWriter, r *http.Request) {
	type DepthMarketRequest struct {
		UserID                  string   `json:"user_id"`
		StrategyName            string   `json:"strategy_name"`
		Description             string   `json:"description"`
		StockCodes              []int64  `json:"stock_codes"`
		Exchanges               []string `json:"exchanges"`
		ImpactScoreThreshold    int32    `json:"impact_score_threshold"`
		MinBidQuantity          int64    `json:"min_bid_quantity,omitempty"`
		MinAskQuantity          int64    `json:"min_ask_quantity,omitempty"`
		MaxSpreadPct            float64  `json:"max_spread_pct,omitempty"`
		RequireLTPBetweenSpread bool     `json:"require_ltp_between_spread,omitempty"`
		PriceRangeMin           float64  `json:"price_range_min,omitempty"`
		PriceRangeMax           float64  `json:"price_range_max,omitempty"`
		VolumeThreshold         int64    `json:"volume_threshold,omitempty"`
		MinMarketCap            float64  `json:"min_market_cap,omitempty"`
		MaxMarketCap            float64  `json:"max_market_cap,omitempty"`
		OrderType               string   `json:"order_type"`
		OrderSide               string   `json:"order_side"`
		Quantity                int32    `json:"quantity"`
		Exchange                string   `json:"exchange"`
		LimitPrice              float64  `json:"limit_price,omitempty"`
		MaxPositionSize         float64  `json:"max_position_size,omitempty"`
		StopLossPct             float64  `json:"stop_loss_pct,omitempty"`
		TakeProfitPct           float64  `json:"take_profit_pct,omitempty"`
		Validity                string   `json:"validity"`
		StopLossType            string   `json:"stop_loss_type"`
		TrailingSlPct           float64  `json:"trailing_sl_pct,omitempty"`
		ProductType             string   `json:"product_type"`
		MaxDailyTrades          int32    `json:"max_daily_trades,omitempty"`
		MaxLossPerDay           float64  `json:"max_loss_per_day,omitempty"`
		PositionSizing          string   `json:"position_sizing"`
		MaxPortfolioExposurePct float64  `json:"max_portfolio_exposure_pct,omitempty"`
		MaxPerTradeRisk         float64  `json:"max_per_trade_risk,omitempty"`
		EnableRiskChecks        bool     `json:"enable_risk_checks"`
		EnableAutoSquareOff     bool     `json:"enable_auto_square_off"`
		AutoSquareOffTime       string   `json:"auto_square_off_time,omitempty"`
		ActivateImmediately     bool     `json:"activate_immediately"`
		BearerToken             string   `json:"bearer_token"`
		AppId                   string   `json:"app_id"`
		Source                  string   `json:"source"`
	}

	var depthReq DepthMarketRequest
	if err := json.NewDecoder(r.Body).Decode(&depthReq); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	// Validate required fields
	if depthReq.UserID == "" || depthReq.StrategyName == "" {
		respondWithError(w, http.StatusBadRequest, "user_id and strategy_name are required")
		return
	}

	if len(depthReq.StockCodes) == 0 || len(depthReq.Exchanges) == 0 {
		respondWithError(w, http.StatusBadRequest, "stock_codes and exchanges are required")
		return
	}

	if depthReq.OrderType == "" || depthReq.OrderSide == "" || depthReq.Exchange == "" {
		respondWithError(w, http.StatusBadRequest, "order_type, order_side, and exchange are required")
		return
	}

	if depthReq.Quantity <= 0 {
		respondWithError(w, http.StatusBadRequest, "quantity must be greater than 0")
		return
	}

	// Extract from headers if not in body
	bearerToken := depthReq.BearerToken
	if bearerToken == "" {
		bearerToken = r.Header.Get("Authorization")
		if bearerToken != "" {
			bearerToken = strings.TrimPrefix(bearerToken, "Bearer ")
		}
	}

	appId := depthReq.AppId
	if appId == "" {
		appId = r.Header.Get("appId")
	}

	source := depthReq.Source
	if source == "" {
		source = r.Header.Get("source")
	}

	// Validate authentication
	if bearerToken == "" || appId == "" || source == "" {
		respondWithError(w, http.StatusBadRequest, "bearer_token, app_id, and source are required")
		return
	}

	// Convert string enums to proto enums
	orderType := parseOrderType(depthReq.OrderType)
	orderSide := parseOrderSide(depthReq.OrderSide)
	exchange := parseExchange(depthReq.Exchange)
	positionSizing := parsePositionSizing(depthReq.PositionSizing)
	stopLossType := parseStopLossType(depthReq.StopLossType)

	// Build price range if provided
	var priceRange *common.PriceRange
	if depthReq.PriceRangeMin > 0 || depthReq.PriceRangeMax > 0 {
		priceRange = &common.PriceRange{
			MinPrice: depthReq.PriceRangeMin,
			MaxPrice: depthReq.PriceRangeMax,
		}
	}

	// Convert exchanges []string to []common.Exchange
	var exchangesProto []common.Exchange
	if len(depthReq.Exchanges) > 0 {
		exchangesProto = make([]common.Exchange, len(depthReq.Exchanges))
		for i, s := range depthReq.Exchanges {
			exchangesProto[i] = parseExchange(s)
		}
	}

	// Build the proto request
	req := &pb.CreateStrategyRequest{
		UserId:              depthReq.UserID,
		StrategyName:        depthReq.StrategyName,
		Description:         depthReq.Description,
		ActivateImmediately: depthReq.ActivateImmediately,
		BearerToken:         bearerToken,
		AppId:               appId,
		Source:              source,
		Conditions: &pb.StrategyConditions{
			StockCodes:              depthReq.StockCodes,
			Exchanges:               exchangesProto,
			PriceRange:              priceRange,
			VolumeThreshold:         depthReq.VolumeThreshold,
			PctChangeThreshold:      0,
			MinBidQuantity:          depthReq.MinBidQuantity,
			MinAskQuantity:          depthReq.MinAskQuantity,
			MaxSpreadPct:            depthReq.MaxSpreadPct,
			RequireLtpBetweenSpread: depthReq.RequireLTPBetweenSpread,
		},
		TradeConfig: &pb.TradeConfig{
			OrderType:       orderType,
			OrderSide:       orderSide,
			Quantity:        depthReq.Quantity,
			Exchange:        exchange,
			LimitPrice:      depthReq.LimitPrice,
			MaxPositionSize: depthReq.MaxPositionSize,
			StopLossPct:     depthReq.StopLossPct,
			TakeProfitPct:   depthReq.TakeProfitPct,
			Validity:        depthReq.Validity,
			StopLossType:    stopLossType,
			TrailingSlPct:   depthReq.TrailingSlPct,
			ProductType:     depthReq.ProductType,
		},
		RiskLimits: &pb.RiskLimits{
			MaxDailyTrades:          depthReq.MaxDailyTrades,
			MaxLossPerDay:           depthReq.MaxLossPerDay,
			PositionSizing:          positionSizing,
			MaxPortfolioExposurePct: depthReq.MaxPortfolioExposurePct,
			MaxPerTradeRisk:         depthReq.MaxPerTradeRisk,
			EnableRiskChecks:        depthReq.EnableRiskChecks,
			EnableAutoSquareOff:     depthReq.EnableAutoSquareOff,
			AutoSquareOffTime:       depthReq.AutoSquareOffTime,
		},
	}

	resp, err := h.client.CreateStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to create depth market strategy: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusCreated, resp)
}

// Helper functions to parse string enums to proto enums
func parseOrderType(s string) common.OrderType {
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

func parseOrderSide(s string) common.OrderSide {
	switch strings.ToUpper(s) {
	case "BUY":
		return common.OrderSide_ORDER_SIDE_BUY
	case "SELL":
		return common.OrderSide_ORDER_SIDE_SELL
	default:
		return common.OrderSide_ORDER_SIDE_UNSPECIFIED
	}
}

func parseExchange(s string) common.Exchange {
	switch strings.ToUpper(s) {
	case "NSE":
		return common.Exchange_EXCHANGE_NSE
	case "BSE":
		return common.Exchange_EXCHANGE_BSE
	default:
		return common.Exchange_EXCHANGE_UNSPECIFIED
	}
}

func parsePositionSizing(s string) common.PositionSizing {
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

func parseStopLossType(s string) pb.StopLossType {
	switch strings.ToUpper(s) {
	case "FIXED":
		return pb.StopLossType_FIXED
	case "TRAILING":
		return pb.StopLossType_TRAILING
	default:
		return pb.StopLossType_STOP_LOSS_TYPE_UNSPECIFIED
	}
}

// UpdateStrategy handles PUT /api/v1/strategies/{strategy_id}
func (h *UserConfigHandler) UpdateStrategy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	strategyID := vars["strategy_id"]

	var req pb.UpdateStrategyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	// Override strategy_id from URL
	req.StrategyId = strategyID

	resp, err := h.client.UpdateStrategy(r.Context(), &req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to update strategy: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// DeleteStrategy handles DELETE /api/v1/strategies/{strategy_id}
func (h *UserConfigHandler) DeleteStrategy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	strategyID := vars["strategy_id"]
	userID := r.URL.Query().Get("user_id")

	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id query parameter is required")
		return
	}

	req := &pb.DeleteStrategyRequest{
		StrategyId: strategyID,
		UserId:     userID,
	}

	resp, err := h.client.DeleteStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to delete strategy: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// GetStrategy handles GET /api/v1/strategies/{strategy_id}
func (h *UserConfigHandler) GetStrategy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	strategyID := vars["strategy_id"]
	userID := r.URL.Query().Get("user_id")

	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id query parameter is required")
		return
	}

	req := &pb.GetStrategyRequest{
		StrategyId: strategyID,
		UserId:     userID,
	}

	resp, err := h.client.GetStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to get strategy: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusNotFound, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// ListUserStrategies handles GET /api/v1/users/{user_id}/strategies
func (h *UserConfigHandler) ListUserStrategies(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	activeOnly := r.URL.Query().Get("active_only") == "true"

	// Parse pagination parameters
	pageStr := r.URL.Query().Get("page")
	pageSizeStr := r.URL.Query().Get("page_size")

	page := int32(1)
	pageSize := int32(10)

	if pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil {
			page = int32(p)
		}
	}

	if pageSizeStr != "" {
		if ps, err := strconv.Atoi(pageSizeStr); err == nil {
			pageSize = int32(ps)
		}
	}

	req := &pb.ListUserStrategiesRequest{
		UserId:     userID,
		ActiveOnly: activeOnly,
		Pagination: &common.PaginationRequest{
			Page:     page,
			PageSize: pageSize,
		},
	}

	resp, err := h.client.ListUserStrategies(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to list strategies: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// ActivateStrategy handles POST /api/v1/strategies/{strategy_id}/activate
func (h *UserConfigHandler) ActivateStrategy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	strategyID := vars["strategy_id"]

	var reqBody struct {
		UserID string `json:"user_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if reqBody.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	req := &pb.ActivateStrategyRequest{
		StrategyId: strategyID,
		UserId:     reqBody.UserID,
	}

	resp, err := h.client.ActivateStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to activate strategy: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// DeactivateStrategy handles POST /api/v1/strategies/{strategy_id}/deactivate
func (h *UserConfigHandler) DeactivateStrategy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	strategyID := vars["strategy_id"]

	var reqBody struct {
		UserID string `json:"user_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if reqBody.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	req := &pb.DeactivateStrategyRequest{
		StrategyId: strategyID,
		UserId:     reqBody.UserID,
	}

	resp, err := h.client.DeactivateStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to deactivate strategy: "+err.Error())
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// HealthCheck handles GET /api/v1/health
func (h *UserConfigHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	req := &common.HealthCheckRequest{}

	resp, err := h.client.HealthCheck(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusServiceUnavailable, "Service unavailable: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// Helper functions
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"Failed to marshal response"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}
