package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// PaperPnLHandler handles HTTP API requests for paper trading PnL data
type PaperPnLHandler struct {
	redisClient *redis.Client
	logger      *zap.Logger
}

// PaperPosition represents an open paper trading position
type PaperPosition struct {
	PositionID    string    `json:"position_id"`
	UserID        string    `json:"user_id"`
	Symbol        string    `json:"symbol"`
	Token         string    `json:"token"`
	Exchange      string    `json:"exchange"`
	Side          string    `json:"side"` // BUY/SELL
	Quantity      int32     `json:"quantity"`
	EntryPrice    float64   `json:"entry_price"`
	CurrentPrice  float64   `json:"current_price"`
	OpenPnL       float64   `json:"open_pnl"`
	OpenPnLPct    float64   `json:"open_pnl_pct"`
	StopLoss      float64   `json:"stop_loss"`
	TakeProfit    float64   `json:"take_profit"`
	TSLActivated  bool      `json:"tsl_activated"`
	TSLPrice      float64   `json:"tsl_price,omitempty"`
	EntryTime     time.Time `json:"entry_time"`
	LastUpdated   time.Time `json:"last_updated"`
}

// ClosedPosition represents a closed paper trading position
type ClosedPosition struct {
	PositionID    string    `json:"position_id"`
	UserID        string    `json:"user_id"`
	Symbol        string    `json:"symbol"`
	Token         string    `json:"token"`
	Exchange      string    `json:"exchange"`
	Side          string    `json:"side"`
	Quantity      int32     `json:"quantity"`
	EntryPrice    float64   `json:"entry_price"`
	ExitPrice     float64   `json:"exit_price"`
	ClosedPnL     float64   `json:"closed_pnl"`
	ClosedPnLPct  float64   `json:"closed_pnl_pct"`
	ExitReason    string    `json:"exit_reason"` // SL/TSL/TP/MANUAL
	EntryTime     time.Time `json:"entry_time"`
	ExitTime      time.Time `json:"exit_time"`
	HoldingPeriod int64     `json:"holding_period_seconds"`
}

// PaperPortfolioSummary represents overall paper trading statistics
type PaperPortfolioSummary struct {
	UserID              string    `json:"user_id"`
	TotalOpenPositions  int       `json:"total_open_positions"`
	TotalClosedTrades   int       `json:"total_closed_trades"`
	TotalInvested       float64   `json:"total_invested"`
	CurrentMarketValue  float64   `json:"current_market_value"`
	TotalOpenPnL        float64   `json:"total_open_pnl"`
	TotalClosedPnL      float64   `json:"total_closed_pnl"`
	OverallPnL          float64   `json:"overall_pnl"`
	OverallPnLPct       float64   `json:"overall_pnl_pct"`
	AvailableCapital    float64   `json:"available_capital"`
	WinRate             float64   `json:"win_rate"`
	AvgWin              float64   `json:"avg_win"`
	AvgLoss             float64   `json:"avg_loss"`
	ProfitFactor        float64   `json:"profit_factor"`
	LargestWin          float64   `json:"largest_win"`
	LargestLoss         float64   `json:"largest_loss"`
	LastUpdated         time.Time `json:"last_updated"`
}

func NewPaperPnLHandler(redisClient *redis.Client, logger *zap.Logger) *PaperPnLHandler {
	return &PaperPnLHandler{
		redisClient: redisClient,
		logger:      logger,
	}
}

// GetOpenPositions returns all open paper trading positions for a user
// GET /api/paper-pnl/:user_id/positions
func (h *PaperPnLHandler) GetOpenPositions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()

	// Fetch open positions from Redis: paper:positions:{user_id}
	key := "paper:positions:" + userID
	positionsJSON, err := h.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		// No open positions
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"user_id":   userID,
			"positions": []PaperPosition{},
			"count":     0,
		})
		return
	} else if err != nil {
		h.logger.Error("Failed to fetch open positions from Redis",
			zap.Error(err),
			zap.String("user_id", userID))
		http.Error(w, "Failed to fetch positions", http.StatusInternalServerError)
		return
	}

	var positions []PaperPosition
	if err := json.Unmarshal([]byte(positionsJSON), &positions); err != nil {
		h.logger.Error("Failed to unmarshal positions",
			zap.Error(err),
			zap.String("user_id", userID))
		http.Error(w, "Failed to parse positions", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user_id":   userID,
		"positions": positions,
		"count":     len(positions),
	})
}

// GetClosedPositions returns closed paper trading positions for a user
// GET /api/paper-pnl/:user_id/history?limit=50&offset=0
func (h *PaperPnLHandler) GetClosedPositions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()

	// Fetch closed positions from Redis list: paper:history:{user_id}
	// This is a list of JSON objects, most recent first
	key := "paper:history:" + userID
	
	// Get latest 50 closed positions (can be parameterized)
	historyJSON, err := h.redisClient.LRange(ctx, key, 0, 49).Result()
	if err != nil && err != redis.Nil {
		h.logger.Error("Failed to fetch closed positions from Redis",
			zap.Error(err),
			zap.String("user_id", userID))
		http.Error(w, "Failed to fetch history", http.StatusInternalServerError)
		return
	}

	var closedPositions []ClosedPosition
	for _, jsonStr := range historyJSON {
		var pos ClosedPosition
		if err := json.Unmarshal([]byte(jsonStr), &pos); err != nil {
			h.logger.Warn("Failed to unmarshal closed position", zap.Error(err))
			continue
		}
		closedPositions = append(closedPositions, pos)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user_id":  userID,
		"history":  closedPositions,
		"count":    len(closedPositions),
	})
}

// GetPortfolioSummary returns overall paper trading statistics for a user
// GET /api/paper-pnl/:user_id/summary
func (h *PaperPnLHandler) GetPortfolioSummary(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()

	// Fetch summary from Redis: paper:summary:{user_id}
	key := "paper:summary:" + userID
	summaryJSON, err := h.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		// No summary yet - return default
		summary := PaperPortfolioSummary{
			UserID:              userID,
			TotalOpenPositions:  0,
			TotalClosedTrades:   0,
			AvailableCapital:    500000, // Default ₹5L
			LastUpdated:         time.Now(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(summary)
		return
	} else if err != nil {
		h.logger.Error("Failed to fetch summary from Redis",
			zap.Error(err),
			zap.String("user_id", userID))
		http.Error(w, "Failed to fetch summary", http.StatusInternalServerError)
		return
	}

	var summary PaperPortfolioSummary
	if err := json.Unmarshal([]byte(summaryJSON), &summary); err != nil {
		h.logger.Error("Failed to unmarshal summary",
			zap.Error(err),
			zap.String("user_id", userID))
		http.Error(w, "Failed to parse summary", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}

// GetLivePnL returns live (real) trading PnL for comparison
// GET /api/live-pnl/:user_id/summary
func (h *PaperPnLHandler) GetLivePnL(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	// TODO: Fetch from trade-execution service or PostgreSQL
	// For now, return placeholder
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user_id": userID,
		"message": "Live PnL - integrate with trade-execution service",
		"todo":    "Query PostgreSQL or call trade-execution gRPC",
	})
}
