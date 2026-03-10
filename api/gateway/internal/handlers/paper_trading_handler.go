package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

// PaperTradingHandler proxies paper trading requests to the trade-execution service.
// The trade-execution service runs the paper WS server on its own port (PAPER_WS_PORT).
type PaperTradingHandler struct {
	tradeExecBaseURL string // e.g. http://localhost:8081
}

func NewPaperTradingHandler(tradeExecPaperURL string) *PaperTradingHandler {
	return &PaperTradingHandler{tradeExecBaseURL: tradeExecPaperURL}
}

// GetPaperPositions handles GET /api/v1/paper-trades/positions?user_id=xxx
func (h *PaperTradingHandler) GetPaperPositions(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	url := fmt.Sprintf("%s/ws/paper-trades/positions?user_id=%s", h.tradeExecBaseURL, userID)
	resp, err := http.Get(url)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// ForceExitAll handles POST /api/v1/paper-trades/force-exit-all
func (h *PaperTradingHandler) ForceExitAll(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("userId")
	if userID == "" {
		respondWithError(w, http.StatusUnauthorized, "userId header is required")
		return
	}

	payload, _ := json.Marshal(map[string]string{"user_id": userID})

	resp, err := http.Post(
		h.tradeExecBaseURL+"/ws/paper-trades/force-exit-all",
		"application/json",
		io.NopCloser(newReaderFrom(payload)),
	)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// ForceExitAllLive handles POST /api/v1/live-orders/force-exit-all
func (h *PaperTradingHandler) ForceExitAllLive(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("userId")
	if userID == "" {
		respondWithError(w, http.StatusUnauthorized, "userId header is required")
		return
	}

	payload, _ := json.Marshal(map[string]string{"user_id": userID})

	resp, err := http.Post(
		h.tradeExecBaseURL+"/ws/live-orders/force-exit-all",
		"application/json",
		io.NopCloser(newReaderFrom(payload)),
	)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// GetLiveOrders handles GET /api/v1/live-orders?user_id=xxx
func (h *PaperTradingHandler) GetLiveOrders(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	url := fmt.Sprintf("%s/ws/live-orders?user_id=%s", h.tradeExecBaseURL, userID)
	resp, err := http.Get(url)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// GetClosedPaperOrders handles GET /api/v1/paper-trades/closed-orders?user_id=xxx
func (h *PaperTradingHandler) GetClosedPaperOrders(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	url := fmt.Sprintf("%s/ws/paper-trades/closed-orders?user_id=%s", h.tradeExecBaseURL, userID)
	resp, err := http.Get(url)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// GetClosedLiveOrders handles GET /api/v1/live-orders/closed-orders?user_id=xxx
func (h *PaperTradingHandler) GetClosedLiveOrders(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	url := fmt.Sprintf("%s/ws/live-orders/closed-orders?user_id=%s", h.tradeExecBaseURL, userID)
	resp, err := http.Get(url)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// GetDashboardStats handles GET /api/v1/dashboard-stats?user_id=xxx&mode=paper
func (h *PaperTradingHandler) GetDashboardStats(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "paper"
	}

	url := fmt.Sprintf("%s/ws/dashboard-stats?user_id=%s&mode=%s", h.tradeExecBaseURL, userID, mode)
	resp, err := http.Get(url)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// GetIndiraPositions handles GET /api/v1/live-orders/indira-positions?user_id=xxx
// Fetches ALL Indira positions and returns only those placed by our algo system.
// Forwards auth headers (Authorization, appId, userId, source) to the trade-execution service
// so it can call the Indira API on behalf of the user.
func (h *PaperTradingHandler) GetIndiraPositions(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	targetURL := fmt.Sprintf("%s/ws/live-orders/indira-positions?user_id=%s", h.tradeExecBaseURL, userID)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL, nil)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Forward auth headers set by the Next.js proxy from httpOnly cookies.
	for _, hdr := range []string{"Authorization", "appId", "appid", "userId", "source"} {
		if val := r.Header.Get(hdr); val != "" {
			req.Header.Set(hdr, val)
		}
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// SubscribeBrokerWS handles POST /api/v1/live-orders/subscribe-broker-ws
// Tells trade-execution to open the per-user Indira WS immediately on strategy activate.
func (h *PaperTradingHandler) SubscribeBrokerWS(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	targetURL := fmt.Sprintf("%s/ws/live-orders/subscribe-broker-ws?user_id=%s", h.tradeExecBaseURL, userID)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, targetURL, nil)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	for _, hdr := range []string{"Authorization", "appId", "appid", "userId", "source"} {
		if val := r.Header.Get(hdr); val != "" {
			req.Header.Set(hdr, val)
		}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		respondWithError(w, http.StatusBadGateway, "Failed to reach trade-execution service: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

// GetPaperWSInfo returns the WebSocket URL the frontend should connect to.
// GET /api/v1/paper-trades/ws-info
func (h *PaperTradingHandler) GetPaperWSInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars

	wsURL := h.tradeExecBaseURL
	// Convert http -> ws
	if len(wsURL) > 5 && wsURL[:5] == "https" {
		wsURL = "wss" + wsURL[5:]
	} else if len(wsURL) > 4 && wsURL[:4] == "http" {
		wsURL = "ws" + wsURL[4:]
	}

	respondWithJSON(w, http.StatusOK, map[string]string{
		"ws_url": wsURL + "/ws/paper-trades",
	})
}

// --- helpers ---

type byteReader struct{ data []byte; pos int }

func newReaderFrom(b []byte) *byteReader { return &byteReader{data: b} }
func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
func (r *byteReader) Close() error { return nil }
