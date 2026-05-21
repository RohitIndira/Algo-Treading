package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/dto"
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
	var reqDTO dto.CreateStrategyRequest
	if err := json.NewDecoder(r.Body).Decode(&reqDTO); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Extract authentication headers from frontend
	bearerToken := r.Header.Get("Authorization")
	if bearerToken != "" {
		bearerToken = strings.TrimPrefix(bearerToken, "Bearer ")
	}

	appId := r.Header.Get("appId")
	source := r.Header.Get("source")
	userIdHeader := r.Header.Get("userId")

	// --- Security & Validation Checks ---

	// 1. Header Validation
	if bearerToken == "" || appId == "" || source == "" || userIdHeader == "" {
		respondWithError(w, http.StatusUnauthorized, "Missing authentication headers: Authorization, appId, source, and userId are required")
		return
	}

	// 2. IDOR Protection: Body UserID must match Header UserID
	if reqDTO.UserID != "" && reqDTO.UserID != userIdHeader {
		respondWithError(w, http.StatusForbidden, "User ID mismatch between header and body")
		return
	}
	// Force body UserID to match header if empty
	reqDTO.UserID = userIdHeader

	// 3. Input Sanitization (Basic)
	// Implement stricter sanitization if needed. For now, we rely on Type safety and SQL parameters in backend.
	// But we can check for obviously bad characters in strings if we wanted to.

	// 4. Logic Validation
	if reqDTO.StrategyName == "" {
		respondWithError(w, http.StatusBadRequest, "Strategy name is required")
		return
	}
	if reqDTO.Conditions != nil {
		if reqDTO.Conditions.MinMarketCap > reqDTO.Conditions.MaxMarketCap && reqDTO.Conditions.MaxMarketCap > 0 {
			respondWithError(w, http.StatusBadRequest, "Min Market Cap cannot be greater than Max Market Cap")
			return
		}
		// Add more range checks here
	}

	// Map DTO to Proto
	pbReq := &pb.CreateStrategyRequest{
		UserId:                 reqDTO.UserID,
		StrategyName:           reqDTO.StrategyName,
		Description:            reqDTO.Description,
		ActivateImmediately:    reqDTO.ActivateImmediately,
		TradingMode:            mapTradingMode(reqDTO.TradingMode),
		Conditions:             dtoConditionsToProto(reqDTO.Conditions),
		TradeConfig:            dtoTradeConfigToProto(reqDTO.TradeConfig),
		RiskLimits:             dtoRiskLimitsToProto(reqDTO.RiskLimits),
		ProcessAfterMarketNews: reqDTO.ProcessAfterMarketNews,
		IndiraAuth: &common.IndiraAuthContext{
			UserId:      userIdHeader,
			AppId:       appId,
			Source:      source,
			BearerToken: bearerToken,
		},
	}

	// Call Service
	resp, err := h.client.CreateStrategy(r.Context(), pbReq)
	if err != nil {
		log.Printf("ERROR CreateStrategy user_id=%s: %v", userIdHeader, err)
		respondWithError(w, http.StatusInternalServerError, "Failed to create strategy")
		return
	}

	if !resp.Success {
		respondWithError(w, http.StatusBadRequest, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusCreated, resp)
}

// UpdateStrategy handles PUT /api/v1/strategies/{strategy_id}
func (h *UserConfigHandler) UpdateStrategy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	strategyID := vars["strategy_id"]

	var reqDTO dto.UpdateStrategyRequest
	if err := json.NewDecoder(r.Body).Decode(&reqDTO); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	userIdHeader := r.Header.Get("userId")
	if userIdHeader == "" {
		respondWithError(w, http.StatusUnauthorized, "userId header is required")
		return
	}

	// IDOR Protection
	if reqDTO.UserID != "" && reqDTO.UserID != userIdHeader {
		respondWithError(w, http.StatusForbidden, "User ID mismatch between header and body")
		return
	}
	reqDTO.UserID = userIdHeader

	// Map DTO to Proto
	pbReq := dtoUpdateStrategyToProto(&reqDTO)
	pbReq.StrategyId = strategyID

	resp, err := h.client.UpdateStrategy(r.Context(), pbReq)
	if err != nil {
		log.Printf("ERROR UpdateStrategy strategy_id=%s user_id=%s: %v", strategyID, userIdHeader, err)
		respondWithError(w, http.StatusInternalServerError, "Failed to update strategy")
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
		log.Printf("ERROR DeleteStrategy strategy_id=%s user_id=%s: %v", strategyID, userID, err)
		respondWithError(w, http.StatusInternalServerError, "Failed to delete strategy")
		return
	}

	if !resp.Success {
		statusCode := mapErrorCodeToHTTPStatus(resp.Error.Code)
		respondWithError(w, statusCode, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Strategy deleted successfully",
	})
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
		log.Printf("ERROR GetStrategy strategy_id=%s user_id=%s: %v", strategyID, userID, err)
		respondWithError(w, http.StatusInternalServerError, "Failed to get strategy")
		return
	}

	if !resp.Success {
		statusCode := mapErrorCodeToHTTPStatus(resp.Error.Code)
		respondWithError(w, statusCode, resp.Error.Message)
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// ListUserStrategies handles GET /api/v1/users/{user_id}/strategies
func (h *UserConfigHandler) ListUserStrategies(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	activeOnly := r.URL.Query().Get("active_only") == "true"
	includeDeleted := r.URL.Query().Get("include_deleted") == "true"

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
		UserId:         userID,
		ActiveOnly:     activeOnly,
		IncludeDeleted: includeDeleted,
		Pagination: &common.PaginationRequest{
			Page:     page,
			PageSize: pageSize,
		},
	}

	resp, err := h.client.ListUserStrategies(r.Context(), req)
	if err != nil {
		log.Printf("ERROR ListUserStrategies user_id=%s: %v", userID, err)
		respondWithError(w, http.StatusInternalServerError, "Failed to list strategies")
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
		respondWithError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Fall back to userId header if body is missing user_id
	if reqBody.UserID == "" {
		reqBody.UserID = r.Header.Get("userId")
	}

	if reqBody.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required (in body or userId header)")
		return
	}

	// Write fresh credentials to broker_accounts BEFORE activating the strategy.
	// Order matters: ActivateStrategy publishes CONFIG_UPDATED via Kafka, which
	// causes trade-execution to invalidate its credentials cache and immediately
	// re-read from DB (via subscribe-broker-ws). If we wrote credentials AFTER
	// activation, trade-execution would re-cache the stale token from DB before
	// our write lands, causing 401 on the next order.
	bearerToken := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	appId := r.Header.Get("appId")
	source := r.Header.Get("source")
	userIdHeader := r.Header.Get("userId")
	if bearerToken != "" && appId != "" && source != "" && userIdHeader != "" {
		credsReq := &pb.UpdateUserCredentialsRequest{
			UserId: reqBody.UserID,
			IndiraAuth: &common.IndiraAuthContext{
				UserId:      userIdHeader,
				AppId:       appId,
				Source:      source,
				BearerToken: bearerToken,
			},
		}
		if _, credsErr := h.client.UpdateUserCredentials(r.Context(), credsReq); credsErr != nil {
			log.Printf("WARN ActivateStrategy: credential refresh failed for user %s: %v", reqBody.UserID, credsErr)
		}
	} else {
		log.Printf("WARN ActivateStrategy: missing auth headers for user %s — credentials NOT refreshed (bearer=%v appId=%v source=%v userId=%v)",
			reqBody.UserID, bearerToken != "", appId != "", source != "", userIdHeader != "")
	}

	req := &pb.ActivateStrategyRequest{
		StrategyId: strategyID,
		UserId:     reqBody.UserID,
	}

	resp, err := h.client.ActivateStrategy(r.Context(), req)
	if err != nil {
		log.Printf("ERROR ActivateStrategy strategy_id=%s user_id=%s: %v", strategyID, reqBody.UserID, err)
		respondWithError(w, http.StatusInternalServerError, "Failed to activate strategy")
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
		respondWithError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Fall back to userId header if body is missing user_id
	if reqBody.UserID == "" {
		reqBody.UserID = r.Header.Get("userId")
	}

	if reqBody.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required (in body or userId header)")
		return
	}

	req := &pb.DeactivateStrategyRequest{
		StrategyId: strategyID,
		UserId:     reqBody.UserID,
	}

	resp, err := h.client.DeactivateStrategy(r.Context(), req)
	if err != nil {
		log.Printf("ERROR DeactivateStrategy strategy_id=%s user_id=%s: %v", strategyID, reqBody.UserID, err)
		respondWithError(w, http.StatusInternalServerError, "Failed to deactivate strategy")
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
		log.Printf("ERROR HealthCheck: %v", err)
		respondWithError(w, http.StatusServiceUnavailable, "Service unavailable")
		return
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// Helper functions
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	var response []byte
	var err error

	if msg, ok := payload.(proto.Message); ok {
		marshaler := protojson.MarshalOptions{
			UseProtoNames:   true,
			EmitUnpopulated: true,
		}
		response, err = marshaler.Marshal(msg)
	} else {
		response, err = json.Marshal(payload)
	}

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
