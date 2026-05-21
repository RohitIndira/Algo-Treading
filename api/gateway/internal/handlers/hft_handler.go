package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	hftpb "github.com/RohitIndira/Algo-Treading/api/proto/hft_engine"
	"github.com/gorilla/mux"
)

// HFTHandler exposes the hft-engine strategy lifecycle (start / stop /
// state) over HTTP. Strategy *creation* goes through the normal
// POST /api/v1/strategies path (user-config); these endpoints only drive
// an already-created HFT_BIDDING strategy.
type HFTHandler struct {
	client *grpc_clients.HFTClient
}

func NewHFTHandler(client *grpc_clients.HFTClient) *HFTHandler {
	return &HFTHandler{client: client}
}

// hftEntryBody is the optional JSON body for POST .../start. Both fields
// are runtime overrides — when omitted the engine uses the stored config.
type hftEntryBody struct {
	Side string `json:"side"` // "BUY" | "SELL" | "BOTH"
	Lots int32  `json:"lots"` // per-side qty cap override
}

// StartHFT handles POST /api/v1/hft/{strategy_id}/start
func (h *HFTHandler) StartHFT(w http.ResponseWriter, r *http.Request) {
	strategyID := mux.Vars(r)["strategy_id"]
	if strategyID == "" {
		respondWithError(w, http.StatusBadRequest, "strategy_id is required")
		return
	}
	if r.Header.Get("userId") == "" {
		respondWithError(w, http.StatusUnauthorized, "userId header is required")
		return
	}

	// Body is optional — start with the stored config when absent.
	var body hftEntryBody
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	resp, err := h.client.Entry(r.Context(), &hftpb.EntryRequest{
		StrategyId: strategyID,
		Side:       strings.ToUpper(strings.TrimSpace(body.Side)),
		Lots:       body.Lots,
	})
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "hft-engine Entry failed: "+err.Error())
		return
	}
	if !resp.Success {
		respondWithJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success":     false,
			"status":      resp.Status,
			"error":       resp.Error,
			"strategy_id": strategyID,
		})
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"success":     true,
		"status":      resp.Status,
		"strategy_id": strategyID,
	})
}

// StopHFT handles POST /api/v1/hft/{strategy_id}/stop
func (h *HFTHandler) StopHFT(w http.ResponseWriter, r *http.Request) {
	strategyID := mux.Vars(r)["strategy_id"]
	if strategyID == "" {
		respondWithError(w, http.StatusBadRequest, "strategy_id is required")
		return
	}
	if r.Header.Get("userId") == "" {
		respondWithError(w, http.StatusUnauthorized, "userId header is required")
		return
	}

	resp, err := h.client.Exit(r.Context(), &hftpb.ExitRequest{StrategyId: strategyID})
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "hft-engine Exit failed: "+err.Error())
		return
	}
	if !resp.Success {
		respondWithJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success":     false,
			"status":      resp.Status,
			"error":       resp.Error,
			"strategy_id": strategyID,
		})
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"success":     true,
		"status":      resp.Status,
		"strategy_id": strategyID,
	})
}

// GetHFTState handles GET /api/v1/hft/{strategy_id}/state
func (h *HFTHandler) GetHFTState(w http.ResponseWriter, r *http.Request) {
	strategyID := mux.Vars(r)["strategy_id"]
	if strategyID == "" {
		respondWithError(w, http.StatusBadRequest, "strategy_id is required")
		return
	}
	if r.Header.Get("userId") == "" {
		respondWithError(w, http.StatusUnauthorized, "userId header is required")
		return
	}

	resp, err := h.client.GetState(r.Context(), &hftpb.GetStateRequest{StrategyId: strategyID})
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "hft-engine GetState failed: "+err.Error())
		return
	}
	if !resp.Success {
		// Not running / unknown strategy — the engine returns success=false
		// with a human-readable error string.
		respondWithJSON(w, http.StatusNotFound, map[string]interface{}{
			"success":     false,
			"error":       resp.Error,
			"strategy_id": strategyID,
		})
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"snapshot": hftSnapshotToMap(resp.Snapshot),
	})
}

// hftSnapshotToMap flattens the proto StateSnapshot into a plain JSON map
// so the response is clean (no protobuf internal fields, snake_case keys).
func hftSnapshotToMap(s *hftpb.StateSnapshot) map[string]interface{} {
	if s == nil {
		return nil
	}
	return map[string]interface{}{
		"strategy_id":       s.StrategyId,
		"user_id":           s.UserId,
		"symbol":            s.Symbol,
		"active":            s.Active,
		"status":            s.Status, // RUNNING | COMPLETED | HALTED | STOPPED
		"mode":              s.Mode,
		"started_at_unix":   s.StartedAtUnix,
		"last_tick_at_unix": s.LastTickAtUnix,
		"last_bid":          s.LastBid,
		"last_ask":          s.LastAsk,
		"buy":               hftSideToMap(s.Buy),
		"sell":              hftSideToMap(s.Sell),
	}
}

func hftSideToMap(side *hftpb.SideSnapshot) map[string]interface{} {
	if side == nil {
		return nil
	}
	history := make([]map[string]interface{}, 0, len(side.History))
	for _, c := range side.History {
		history = append(history, hftChunkToMap(c))
	}
	return map[string]interface{}{
		"position":    side.Position,
		"max_qty":     side.MaxQty,
		"done":        side.Done,
		"halt_reason": side.HaltReason,
		"armed":       side.Armed,
		"current":     hftChunkToMap(side.Current),
		"history":     history,
	}
}

func hftChunkToMap(c *hftpb.ChunkSnapshot) map[string]interface{} {
	if c == nil {
		return nil
	}
	return map[string]interface{}{
		"seq":             c.Seq,
		"qty":             c.Qty,
		"filled":          c.Filled,
		"limit_price":     c.LimitPrice,
		"broker_order_id": c.BrokerOrderId,
		"status":          c.Status,
		"modify_count":    c.ModifyCount,
		"placed_at_unix":  c.PlacedAtUnix,
	}
}
