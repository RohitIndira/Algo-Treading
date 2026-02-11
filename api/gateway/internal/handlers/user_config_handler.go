package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"github.com/gorilla/mux"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

	// Delete generic strategy via user-config service.
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

// ========================================================================
// Jobbing Strategy REST API Handlers
// ========================================================================

// ConfigureJobbingStrategy handles POST /api/v1/strategies/jobbing/configure
func (h *UserConfigHandler) ConfigureJobbingStrategy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID  string `json:"user_id"`
		Configs []struct {
			Token            string   `json:"token"`
			Symbol           string   `json:"symbol"`
			Exchange         string   `json:"exchange"`
			LowerRange       float64  `json:"lower_range"`
			HigherRange      float64  `json:"higher_range"`
			InitialBuyOffset *float64 `json:"initial_buy_offset,omitempty"`
			DistanceContinue *float64 `json:"distance_continue,omitempty"`
			QuantityPerOrder *int32   `json:"quantity_per_order,omitempty"`
			MaxQuantity      *int32   `json:"max_quantity,omitempty"`
			TradingMode      *string  `json:"trading_mode,omitempty"`
			Enabled          *bool    `json:"enabled,omitempty"`
		} `json:"configs"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if body.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	if len(body.Configs) == 0 {
		respondWithError(w, http.StatusBadRequest, "at least one token configuration is required")
		return
	}

	// Convert to proto
	protoConfigs := make([]*pb.JobbingTokenConfig, len(body.Configs))
	for i, cfg := range body.Configs {
		protoConfig := &pb.JobbingTokenConfig{
			Token:       cfg.Token,
			Symbol:      cfg.Symbol,
			Exchange:    cfg.Exchange,
			LowerRange:  cfg.LowerRange,
			HigherRange: cfg.HigherRange,
		}
		if cfg.InitialBuyOffset != nil {
			protoConfig.InitialBuyOffset = *cfg.InitialBuyOffset
		}
		if cfg.DistanceContinue != nil {
			protoConfig.DistanceContinue = *cfg.DistanceContinue
		}
		if cfg.QuantityPerOrder != nil {
			protoConfig.QuantityPerOrder = *cfg.QuantityPerOrder
		}
		if cfg.MaxQuantity != nil {
			protoConfig.MaxQuantity = *cfg.MaxQuantity
		}
		if cfg.TradingMode != nil {
			protoConfig.TradingMode = *cfg.TradingMode
		}
		if cfg.Enabled != nil {
			protoConfig.Enabled = *cfg.Enabled
		}
		protoConfigs[i] = protoConfig
	}

	req := &pb.ConfigureJobbingStrategyRequest{
		UserId:  body.UserID,
		Configs: protoConfigs,
	}

	resp, err := h.client.ConfigureJobbingStrategy(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to configure jobbing strategy: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to configure jobbing strategy")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// GetJobbingConfigs handles GET /api/v1/strategies/jobbing
func (h *UserConfigHandler) GetJobbingConfigs(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id query parameter is required")
		return
	}

	enabledOnly := r.URL.Query().Get("enabled_only") == "true"

	req := &pb.GetJobbingConfigsRequest{
		UserId:      userID,
		EnabledOnly: enabledOnly,
	}

	resp, err := h.client.GetJobbingConfigs(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve jobbing configs: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to retrieve jobbing configs")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// GetJobbingConfig handles GET /api/v1/strategies/jobbing/{token}
func (h *UserConfigHandler) GetJobbingConfig(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	token := vars["token"]

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id query parameter is required")
		return
	}

	req := &pb.GetJobbingConfigRequest{
		UserId: userID,
		Token:  token,
	}

	resp, err := h.client.GetJobbingConfig(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve jobbing config: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			code := http.StatusBadRequest
			if resp.Error.Code == "NOT_FOUND" {
				code = http.StatusNotFound
			}
			respondWithError(w, code, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to retrieve jobbing config")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// UpdateJobbingConfig handles PUT /api/v1/strategies/jobbing/{token}
func (h *UserConfigHandler) UpdateJobbingConfig(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	token := vars["token"]

	var body struct {
		UserID           string   `json:"user_id"`
		LowerRange       *float64 `json:"lower_range,omitempty"`
		HigherRange      *float64 `json:"higher_range,omitempty"`
		InitialBuyOffset *float64 `json:"initial_buy_offset,omitempty"`
		DistanceContinue *float64 `json:"distance_continue,omitempty"`
		QuantityPerOrder *int32   `json:"quantity_per_order,omitempty"`
		MaxQuantity      *int32   `json:"max_quantity,omitempty"`
		TradingMode      *string  `json:"trading_mode,omitempty"`
		Enabled          *bool    `json:"enabled,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if body.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	req := &pb.UpdateJobbingConfigRequest{
		UserId:           body.UserID,
		Token:            token,
		LowerRange:       body.LowerRange,
		HigherRange:      body.HigherRange,
		InitialBuyOffset: body.InitialBuyOffset,
		DistanceContinue: body.DistanceContinue,
		QuantityPerOrder: body.QuantityPerOrder,
		MaxQuantity:      body.MaxQuantity,
		TradingMode:      body.TradingMode,
		Enabled:          body.Enabled,
	}

	resp, err := h.client.UpdateJobbingConfig(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to update jobbing config: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to update jobbing config")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// DeleteJobbingConfig handles DELETE /api/v1/strategies/jobbing/{token}
func (h *UserConfigHandler) DeleteJobbingConfig(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	token := vars["token"]

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id query parameter is required")
		return
	}

	req := &pb.DeleteJobbingConfigRequest{
		UserId: userID,
		Token:  token,
	}

	resp, err := h.client.DeleteJobbingConfig(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to delete jobbing config: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to delete jobbing config")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// EnableJobbingConfig handles POST /api/v1/strategies/jobbing/{token}/enable
func (h *UserConfigHandler) EnableJobbingConfig(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	token := vars["token"]

	var body struct {
		UserID string `json:"user_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if body.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	req := &pb.EnableJobbingConfigRequest{
		UserId: body.UserID,
		Token:  token,
	}

	resp, err := h.client.EnableJobbingConfig(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to enable jobbing config: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to enable jobbing config")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}

// DisableJobbingConfig handles POST /api/v1/strategies/jobbing/{token}/disable
func (h *UserConfigHandler) DisableJobbingConfig(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	token := vars["token"]

	var body struct {
		UserID string `json:"user_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if body.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	req := &pb.DisableJobbingConfigRequest{
		UserId: body.UserID,
		Token:  token,
	}

	resp, err := h.client.DisableJobbingConfig(r.Context(), req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to disable jobbing config: "+err.Error())
		return
	}

	if !resp.Success {
		if resp.Error != nil {
			respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		} else {
			respondWithError(w, http.StatusBadRequest, "Failed to disable jobbing config")
		}
		return
	}

	respondWithProtoJSON(w, http.StatusOK, resp)
}
