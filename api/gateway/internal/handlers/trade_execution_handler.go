package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	"github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
	"github.com/gorilla/mux"
)

// TradeExecutionHandler exposes HTTP endpoints related to trade executions.
// For now, the /orders/success endpoint reads directly from the Kafka
// trade-executions topic instead of the trade-execution gRPC/DB layer.
type TradeExecutionHandler struct {
	// We keep the gRPC client in case we want to switch back to DB-based
	// queries later, but it is not used in the Kafka-backed endpoint.
	client *grpc_clients.TradeExecutionClient
}

func NewTradeExecutionHandler(client *grpc_clients.TradeExecutionClient) *TradeExecutionHandler {
	return &TradeExecutionHandler{client: client}
}

// ListSuccessfulUserOrders handles:
//
//	GET /api/v1/users/{user_id}/orders/success?page=1&page_size=20&trading_mode=PAPER
//
// It retrieves all orders for a user from the database via the trade-execution gRPC service.
// Supports pagination and optional trading_mode filter (LIVE/PAPER).
func (h *TradeExecutionHandler) ListSuccessfulUserOrders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	// Parse pagination parameters
	page := int32(1)
	pageSize := int32(20)
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := parseInt(pageStr); err == nil && p > 0 {
			page = int32(p)
		}
	}
	if pageSizeStr := r.URL.Query().Get("page_size"); pageSizeStr != "" {
		if ps, err := parseInt(pageSizeStr); err == nil && ps > 0 {
			pageSize = int32(ps)
		}
	}

	// Optional trading_mode filter (LIVE/PAPER)
	tradingMode := strings.ToUpper(r.URL.Query().Get("trading_mode"))

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Create gRPC request
	req := &pb.GetUserOrdersRequest{
		UserId: userID,
		Pagination: &common.PaginationRequest{
			Page:     page,
			PageSize: pageSize,
		},
		TradingMode: tradingMode,
	}

	// Call gRPC service to get orders from database
	resp, err := h.client.GetUserOrders(ctx, req)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve orders: "+err.Error())
		return
	}

	if !resp.Success {
		errMsg := "Failed to retrieve orders"
		if resp.Error != nil {
			errMsg = resp.Error.Message
		}
		respondWithError(w, http.StatusInternalServerError, errMsg)
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"orders":     resp.Orders,
		"pagination": resp.Pagination,
	})
}

func parseInt(s string) (int, error) {
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	return i, err
}
