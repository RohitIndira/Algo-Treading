package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func cash52wStableStrategyID(userID string) string {
	// Must match user-config service stable id logic:
	// uuid.NewSHA1(uuid.NameSpaceOID, []byte("CASH_52W_HIGH:"+user_id))
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("CASH_52W_HIGH:"+userID)).String()
}

type UserConfigHandler struct {
	client *grpc_clients.UserConfigClient
}

func NewUserConfigHandler(client *grpc_clients.UserConfigClient) *UserConfigHandler {
	return &UserConfigHandler{
		client: client,
	}
}

// ConfigureCash52WeekStrategy handles POST /api/v1/strategies/cash52w/configure
// This is a high-level endpoint for the managed Cash 52-week High strategy.
// Frontend sends a simple JSON payload with user_id, enabled and a few
// numeric fields; the backend fills in detailed trade_config/risk_limits.
func (h *UserConfigHandler) ConfigureCash52WeekStrategy(w http.ResponseWriter, r *http.Request) {
	// For the managed Cash 52W strategy we keep the public JSON payload
	// intentionally minimal so that callers don't have to understand all
	// low-level fields. The backend will apply sensible defaults for
	// everything else.
	//
	// Accepted JSON fields:
	//   - user_id          (string, required)
	//   - enabled          (bool, required)
	//   - capital_per_stock (float, optional; default ~20000 if <= 0)
	//   - trading_mode     (string, optional; "LIVE" or "PAPER", default LIVE)
	//
	// We also accept camelCase "tradingMode" for convenience.
	var body struct {
		UserID          string  `json:"user_id"`
		Enabled         bool    `json:"enabled"`
		CapitalPerStock float64 `json:"capital_per_stock"`
		TradingModeSnake string `json:"trading_mode"`
		TradingModeCamel string `json:"tradingMode"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if body.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	// Prefer explicit snake_case field, but fall back to camelCase if needed.
	tradingMode := body.TradingModeSnake
	if tradingMode == "" {
		tradingMode = body.TradingModeCamel
	}

	req := &pb.ConfigureCash52WeekStrategyRequest{
		UserId:          body.UserID,
		Enabled:         body.Enabled,
		CapitalPerStock: body.CapitalPerStock,
		TradingMode:     tradingMode,
	}

	resp, err := h.client.ConfigureCash52WeekStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to configure 52w strategy: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to configure 52w strategy")
		}
		return
	}

	// Return a minimal JSON response for the managed 52W strategy instead
	// of the full generic Strategy payload. Frontend callers only care
	// about the high-level configuration they just set/applied.
	strategy := resp.GetStrategy()
	if strategy == nil {
		respondWithError(w, http.StatusInternalServerError, "52w strategy missing in response")
		return
	}

	// Derive capital_per_stock from trade_config.max_position_size, which
	// is where the backend stores this value for the managed 52W strategy.
	capital := 0.0
	if strategy.TradeConfig != nil {
		capital = strategy.TradeConfig.MaxPositionSize
	}

	out := struct {
		Success         bool    `json:"success"`
		UserID          string  `json:"user_id"`
		Enabled         bool    `json:"enabled"`
		CapitalPerStock float64 `json:"capital_per_stock"`
		TradingMode     string  `json:"trading_mode"`
	}{
		Success:         resp.Success,
		UserID:          strategy.UserId,
		Enabled:         strategy.Active,
		CapitalPerStock: capital,
		TradingMode:     strategy.TradingMode,
	}

	respondWithJSON(w, http.StatusOK, out)
}

// CreateStrategy handles POST /api/v1/strategies
func (h *UserConfigHandler) CreateStrategy(w http.ResponseWriter, r *http.Request) {
	var req pb.CreateStrategyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
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

	respondWithProtoJSON(w, http.StatusCreated, resp)
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

	respondWithProtoJSON(w, http.StatusOK, resp)
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

	// Special case: managed CASH_52W_HIGH is stored in cash52w_configs and does
	// not exist as a generic strategies row. Frontend still calls
	// DELETE /strategies/{strategy_id}?user_id=... using the stable UUID.
	// We translate that into a disable call.
	if strategyID == cash52wStableStrategyID(userID) {
		// Disable (delete config row) via the managed endpoint.
		req := &pb.ConfigureCash52WeekStrategyRequest{
			UserId:  userID,
			Enabled: false,
		}
		resp, err := h.client.ConfigureCash52WeekStrategy(r.Context(), req)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to disable cash52w strategy: "+err.Error())
			return
		}
		if !resp.Success {
			if resp.Error != nil {
				respondWithError(w, http.StatusBadRequest, resp.Error.Message)
				return
			}
			respondWithError(w, http.StatusBadRequest, "Failed to disable cash52w strategy")
			return
		}

		// Mirror the minimal JSON response used by ConfigureCash52WeekStrategy.
		strategy := resp.GetStrategy()
		capital := 0.0
		if strategy != nil && strategy.TradeConfig != nil {
			capital = strategy.TradeConfig.MaxPositionSize
		}
		respondWithJSON(w, http.StatusOK, map[string]any{
			"success":           true,
			"strategy_id":       strategyID,
			"user_id":           userID,
			"enabled":           false,
			"capital_per_stock": capital,
			"trading_mode":      func() string { if strategy != nil { return strategy.TradingMode }; return "" }(),
		})
		return
	}

	// Default: delete generic strategy via user-config service.
	req := &pb.DeleteStrategyRequest{StrategyId: strategyID, UserId: userID}
	resp, err := h.client.DeleteStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to delete strategy: "+err.Error())
		return
	}
	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}
	respondWithProtoJSON(w, http.StatusOK, resp)
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

	respondWithProtoJSON(w, http.StatusOK, resp)
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

	// Use protobuf-aware JSON marshalling with EmitUnpopulated so that
	// boolean fields like `active` are always present in the response,
	// even when false. This fixes the issue where `active` disappeared
	// after deactivation.
	respondWithProtoJSON(w, http.StatusOK, resp)
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

	respondWithProtoJSON(w, http.StatusOK, resp)
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

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// HealthCheck handles GET /api/v1/health
func (h *UserConfigHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	req := &common.HealthCheckRequest{}

	resp, err := h.client.HealthCheck(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusServiceUnavailable, "Service unavailable: "+err.Error())
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
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

// respondWithProtoJSON marshals a protobuf message to JSON using the
// official protojson package, with EmitUnpopulated enabled so that
// zero-value fields (like bool=false) are still included in the output.
func respondWithProtoJSON(w http.ResponseWriter, code int, msg proto.Message) {
	mo := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseEnumNumbers:  false,
	}

	response, err := mo.Marshal(msg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"Failed to marshal protobuf response"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

// ============================================================================
// PHASE 1: Enhanced Cash52W Configuration REST Endpoints
// ============================================================================

// ConfigureCash52WStrategyEnhanced handles POST /api/v1/strategies/cash52w/configure-enhanced
// This is the FULL Phase 1 endpoint with all multi-level profit/SL, portfolio config, and manual controls
func (h *UserConfigHandler) ConfigureCash52WStrategyEnhanced(w http.ResponseWriter, r *http.Request) {
	var req pb.ConfigureCash52WStrategyEnhancedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.UserId == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	resp, err := h.client.ConfigureCash52WStrategyEnhanced(r.Context(), &req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to configure enhanced strategy: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to configure enhanced strategy")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// GetCash52WConfig handles GET /api/v1/strategies/cash52w/config/{user_id}
func (h *UserConfigHandler) GetCash52WConfig(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	req := &pb.GetCash52WConfigRequest{
		UserId: userID,
	}

	resp, err := h.client.GetCash52WConfig(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to get config: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusNotFound, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusNotFound, "Config not found")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// ForceExitAll handles PUT /api/v1/strategies/cash52w/force-exit-all
func (h *UserConfigHandler) ForceExitAll(w http.ResponseWriter, r *http.Request) {
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

	req := &pb.ForceExitAllRequest{
		UserId: reqBody.UserID,
	}

	resp, err := h.client.ForceExitAll(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to force exit: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to force exit")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// ForceExitStocks handles PUT /api/v1/strategies/cash52w/force-exit-stocks
func (h *UserConfigHandler) ForceExitStocks(w http.ResponseWriter, r *http.Request) {
	var reqBody struct {
		UserID string   `json:"user_id"`
		Stocks []string `json:"stocks"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if reqBody.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	if len(reqBody.Stocks) == 0 {
		respondWithError(w, http.StatusBadRequest, "stocks list cannot be empty")
		return
	}

	req := &pb.ForceExitStocksRequest{
		UserId: reqBody.UserID,
		Stocks: reqBody.Stocks,
	}

	resp, err := h.client.ForceExitStocks(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to force exit stocks: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to force exit stocks")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// UpdateManualControls handles PUT /api/v1/strategies/cash52w/manual-controls
func (h *UserConfigHandler) UpdateManualControls(w http.ResponseWriter, r *http.Request) {
	var reqBody struct {
		UserID          string `json:"user_id"`
		PauseNewEntries bool   `json:"pause_new_entries"`
		ResetForceExit  bool   `json:"reset_force_exit"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if reqBody.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	req := &pb.UpdateManualControlsRequest{
		UserId:          reqBody.UserID,
		PauseNewEntries: reqBody.PauseNewEntries,
		ResetForceExit:  reqBody.ResetForceExit,
	}

	resp, err := h.client.UpdateManualControls(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to update manual controls: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to update manual controls")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// DisableCash52W handles PUT /api/v1/strategies/cash52w/disable
func (h *UserConfigHandler) DisableCash52W(w http.ResponseWriter, r *http.Request) {
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

	req := &pb.DisableCash52WRequest{
		UserId: reqBody.UserID,
	}

	resp, err := h.client.DisableCash52W(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to disable strategy: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to disable strategy")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// GetAllEnabledConfigs handles GET /api/v1/strategies/cash52w/enabled-configs
// This is for admin/monitoring purposes only
func (h *UserConfigHandler) GetAllEnabledConfigs(w http.ResponseWriter, r *http.Request) {
	req := &pb.GetAllEnabledConfigsRequest{}

	resp, err := h.client.GetAllEnabledConfigs(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to get enabled configs: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusInternalServerError, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusInternalServerError, "Failed to get enabled configs")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}
