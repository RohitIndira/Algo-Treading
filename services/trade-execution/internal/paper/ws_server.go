package paper

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/gorilla/websocket"
)

const (
	// writeWait is the maximum time allowed to write a message to a client.
	writeWait = 10 * time.Second
	// pongWait is the maximum time to wait for a pong response from the client.
	pongWait = 60 * time.Second
	// pingPeriod is how often the server pings the client (must be < pongWait).
	pingPeriod = (pongWait * 9) / 10
)

// PaperUpdate is the message sent to frontend clients over the paper trading WebSocket.
type PaperUpdate struct {
	Type       string          `json:"type"`                 // connected | pnl_update | position_exit | force_exit_done | initial_orders | new_order
	OrderID    string          `json:"order_id,omitempty"`
	UserID     string          `json:"user_id,omitempty"`
	Symbol     string          `json:"symbol,omitempty"`
	LTP        float64         `json:"ltp,omitempty"`
	LivePnL    float64         `json:"live_pnl,omitempty"`  // Ongoing PnL
	FinalPnL   float64         `json:"final_pnl,omitempty"` // On exit
	Reason     string          `json:"reason,omitempty"`    // STOP_LOSS | TAKE_PROFIT | FORCE_EXIT
	ExitPrice  float64         `json:"exit_price,omitempty"`
	EntryPrice float64         `json:"entry_price,omitempty"`
	Positions  []*models.Order `json:"positions,omitempty"` // Used for initial_orders
	Order      *models.Order   `json:"order,omitempty"`     // Used for new_order
}

// LiveOrderUpdate is the message sent to frontend clients over the live orders WebSocket.
type LiveOrderUpdate struct {
	Type   string          `json:"type"`             // connected | initial_orders | order_update | force_exit_done
	UserID string          `json:"user_id,omitempty"`
	Orders []*models.Order `json:"orders,omitempty"` // For initial_orders
	Order  *models.Order   `json:"order,omitempty"`  // For order_update
}

// BrokerPosition is a position returned by the Indira broker API.
// Defined here to avoid importing pkg/indira into the paper package.
type BrokerPosition struct {
	Symbol        string  `json:"symbol"`
	Exchange      string  `json:"exchange"`
	ProductType   string  `json:"product_type"`
	NetQty        int     `json:"net_qty"`
	BuyQty        int     `json:"buy_qty"`
	SellQty       int     `json:"sell_qty"`
	BuyAvgPrice   float64 `json:"buy_avg_price"`
	SellAvgPrice  float64 `json:"sell_avg_price"`
	CurrentPrice  float64 `json:"current_price"`
	PnL           float64 `json:"pnl"`
	PnLPercentage float64 `json:"pnl_percentage"`
}

// EnrichedAlgoPosition is a broker position filtered and enriched with our algo order metadata.
type EnrichedAlgoPosition struct {
	// Broker data
	Symbol        string  `json:"symbol"`
	Exchange      string  `json:"exchange"`
	ProductType   string  `json:"product_type"`
	NetQty        int     `json:"net_qty"`
	BuyQty        int     `json:"buy_qty"`
	SellQty       int     `json:"sell_qty"`
	BuyAvgPrice   float64 `json:"buy_avg_price"`
	SellAvgPrice  float64 `json:"sell_avg_price"`
	CurrentPrice  float64 `json:"current_price"`
	BrokerPnL     float64 `json:"broker_pnl"`
	BrokerPnLPct  float64 `json:"broker_pnl_pct"`
	// Our algo order metadata
	OrderID       string  `json:"order_id,omitempty"`
	StrategyID    string  `json:"strategy_id,omitempty"`
	IndiraOrderID string  `json:"indira_order_id,omitempty"`
	OrderSide     string  `json:"order_side,omitempty"`
	FilledPrice   float64 `json:"filled_price,omitempty"`
	FilledQty     int32   `json:"filled_qty,omitempty"`
	TradingMode   string  `json:"trading_mode,omitempty"`
	Status        string  `json:"status,omitempty"`
}

// PositionsFetcher fetches live positions from the broker using the provided auth credentials.
// bearerToken, appId, userId, source come from the frontend request headers.
type PositionsFetcher func(ctx context.Context, bearerToken, appId, userId, source string) ([]BrokerPosition, error)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
}

// PaperWSServer is the WebSocket server that pushes live PnL updates (paper) and
// order status updates (live) to the frontend — both run on the same port (PAPER_WS_PORT).
type PaperWSServer struct {
	repo             repository.OrderRepository
	monitor          *PaperTradeMonitor // set after monitor is created
	positionsFetcher PositionsFetcher   // set via SetPositionsFetcher
	// startBrokerWS is wired in main.go to statusService.StartSubscription so the paper
	// package doesn't import statusservice and create an import cycle.
	startBrokerWS func(ctx context.Context, userID, bearerToken, appID, source string) error

	// Paper trade clients: userID -> conn -> per-conn write mutex
	clients map[string]map[*websocket.Conn]*sync.Mutex
	mu      sync.RWMutex

	// Live order clients: userID -> conn -> per-conn write mutex
	liveClients map[string]map[*websocket.Conn]*sync.Mutex
	liveMu      sync.RWMutex
}

// NewPaperWSServer creates the WebSocket server.
func NewPaperWSServer(repo repository.OrderRepository) *PaperWSServer {
	return &PaperWSServer{
		repo:        repo,
		clients:     make(map[string]map[*websocket.Conn]*sync.Mutex),
		liveClients: make(map[string]map[*websocket.Conn]*sync.Mutex),
	}
}

// SetMonitor wires the monitor (must be called after both are created to break circular dep).
func (s *PaperWSServer) SetMonitor(m *PaperTradeMonitor) {
	s.monitor = m
}

// SetPositionsFetcher wires the Indira positions fetcher used by the indira-positions endpoint.
func (s *PaperWSServer) SetPositionsFetcher(fn PositionsFetcher) {
	s.positionsFetcher = fn
}

// SetStatusService wires the broker WS subscription starter.
// fn must call statusService.StartSubscription under the hood.
// Called from main.go to avoid import cycles between paper ↔ statusservice.
func (s *PaperWSServer) SetStatusService(fn func(ctx context.Context, userID, bearerToken, appID, source string) error) {
	s.startBrokerWS = fn
}

// RegisterRoutes registers all paper trading and live order HTTP/WebSocket endpoints onto a mux.
func (s *PaperWSServer) RegisterRoutes(mux *http.ServeMux) {
	// Paper trading
	mux.HandleFunc("/ws/paper-trades", s.handleWSConnection)
	mux.HandleFunc("/ws/paper-trades/positions", s.handleGetPositions)
	mux.HandleFunc("/ws/paper-trades/closed-orders", s.handleGetClosedPaperOrders)
	mux.HandleFunc("/ws/paper-trades/force-exit-all", s.handleForceExitAll)
	// Live orders
	mux.HandleFunc("/ws/live-orders", s.handleLiveOrdersWS)
	mux.HandleFunc("/ws/live-orders/closed-orders", s.handleGetClosedLiveOrders)
	mux.HandleFunc("/ws/live-orders/force-exit-all", s.handleForceExitAllLive)
	mux.HandleFunc("/ws/live-orders/indira-positions", s.handleIndiraPositions)
	mux.HandleFunc("/ws/live-orders/subscribe-broker-ws", s.handleSubscribeBrokerWS)
	// Dashboard
	mux.HandleFunc("/ws/dashboard-stats", s.handleGetDashboardStats)
}

// ─────────────────────────────────────────────────────────────────────────────
// PAPER TRADING handlers
// ─────────────────────────────────────────────────────────────────────────────

// handleWSConnection upgrades an HTTP request to a WebSocket and streams PnL updates.
// GET /ws/paper-trades?user_id=xxx
func (s *PaperWSServer) handleWSConnection(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[paper-ws] Upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("[paper-ws] Client connected: user=%s remote=%s", userID, conn.RemoteAddr())

	// Keepalive: detect dead connections within pongWait (60s).
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Send welcome and initial orders before registering for broadcasts,
	// so concurrent Broadcast calls cannot race with these writes.
	conn.WriteJSON(PaperUpdate{Type: "connected", UserID: userID})

	orders, err := s.repo.GetFilledPaperOrdersByUser(r.Context(), userID)
	if err != nil {
		log.Printf("[paper-ws] Failed to fetch initial orders for user %s: %v", userID, err)
	} else {
		conn.WriteJSON(PaperUpdate{
			Type:      "initial_orders",
			UserID:    userID,
			Positions: orders,
		})
	}

	// Immediately push current PnL snapshot so the client doesn't wait for the next tick.
	if s.monitor != nil {
		for _, snap := range s.monitor.GetCurrentPnLSnapshot(r.Context(), userID) {
			conn.WriteJSON(snap)
		}
	}

	connMu := s.registerClient(userID, conn)
	defer s.unregisterClient(userID, conn)

	// Server-side ping loop: keeps the connection alive through proxies/load balancers.
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for range ticker.C {
			connMu.Lock()
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := conn.WriteMessage(websocket.PingMessage, nil)
			conn.SetWriteDeadline(time.Time{})
			connMu.Unlock()
			if err != nil {
				return
			}
		}
	}()

	// Keep connection alive — server pushes, client just reads (pings)
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break // client disconnected or read deadline exceeded
		}
	}

	log.Printf("[paper-ws] Client disconnected: user=%s", userID)
}

// handleGetPositions returns all open paper positions as JSON.
// GET /ws/paper-trades/positions?user_id=xxx
func (s *PaperWSServer) handleGetPositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	orders, err := s.repo.GetFilledPaperOrdersByUser(r.Context(), userID)
	if err != nil {
		http.Error(w, "failed to fetch positions: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"positions": orders,
	})
}

// handleForceExitAll force-exits all open paper positions for a user.
// POST /ws/paper-trades/force-exit-all  body: {"user_id":"xxx"}
func (s *PaperWSServer) handleForceExitAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	if s.monitor == nil {
		http.Error(w, "monitor not initialized", http.StatusServiceUnavailable)
		return
	}

	if err := s.monitor.ForceExitAll(r.Context(), body.UserID); err != nil {
		http.Error(w, "force exit failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

// Broadcast sends a PaperUpdate to all WebSocket connections for a given user.
func (s *PaperWSServer) Broadcast(userID string, update PaperUpdate) {
	s.mu.RLock()
	conns := make(map[*websocket.Conn]*sync.Mutex, len(s.clients[userID]))
	for c, mu := range s.clients[userID] {
		conns[c] = mu
	}
	s.mu.RUnlock()

	if len(conns) == 0 {
		return
	}

	for conn, mu := range conns {
		mu.Lock()
		conn.SetWriteDeadline(time.Now().Add(writeWait))
		err := conn.WriteJSON(update)
		conn.SetWriteDeadline(time.Time{})
		mu.Unlock()
		if err != nil {
			log.Printf("[paper-ws] Failed to send to user %s: %v — removing conn", userID, err)
			s.unregisterClient(userID, conn)
		}
	}
}

func (s *PaperWSServer) registerClient(userID string, conn *websocket.Conn) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients[userID] == nil {
		s.clients[userID] = make(map[*websocket.Conn]*sync.Mutex)
	}
	mu := &sync.Mutex{}
	s.clients[userID][conn] = mu
	return mu
}

func (s *PaperWSServer) unregisterClient(userID string, conn *websocket.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients[userID] != nil {
		delete(s.clients[userID], conn)
		if len(s.clients[userID]) == 0 {
			delete(s.clients, userID)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE ORDERS handlers
// ─────────────────────────────────────────────────────────────────────────────

// handleLiveOrdersWS upgrades to a WebSocket and streams live order updates.
// GET /ws/live-orders?user_id=xxx
func (s *PaperWSServer) handleLiveOrdersWS(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[live-ws] Upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("[live-ws] Client connected: user=%s remote=%s", userID, conn.RemoteAddr())

	// Keepalive: detect dead connections within pongWait (60s).
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Send welcome and initial orders before registering for broadcasts,
	// so concurrent BroadcastLiveOrder calls cannot race with these writes.
	conn.WriteJSON(LiveOrderUpdate{Type: "connected", UserID: userID})

	orders, err := s.repo.GetLiveOrdersByUser(r.Context(), userID)
	if err != nil {
		log.Printf("[live-ws] Failed to fetch initial orders for user %s: %v", userID, err)
	} else {
		conn.WriteJSON(LiveOrderUpdate{
			Type:   "initial_orders",
			UserID: userID,
			Orders: orders,
		})
	}

	connMu := s.registerLiveClient(userID, conn)
	defer s.unregisterLiveClient(userID, conn)

	// Server-side ping loop: keeps the connection alive through proxies/load balancers.
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for range ticker.C {
			connMu.Lock()
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := conn.WriteMessage(websocket.PingMessage, nil)
			conn.SetWriteDeadline(time.Time{})
			connMu.Unlock()
			if err != nil {
				return
			}
		}
	}()

	// Keep connection alive — server pushes updates, client sends pings
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break // client disconnected or read deadline exceeded
		}
	}

	log.Printf("[live-ws] Client disconnected: user=%s", userID)
}

// handleForceExitAllLive cancels all active live (non-paper) orders for a user, recording exit price + PnL.
// POST /ws/live-orders/force-exit-all  body: {"user_id":"xxx"}
func (s *PaperWSServer) handleForceExitAllLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	orders, err := s.repo.GetLiveOrdersByUser(r.Context(), body.UserID)
	if err != nil {
		http.Error(w, "failed to fetch live orders: "+err.Error(), http.StatusInternalServerError)
		return
	}

	for _, order := range orders {
		exitPrice := safeF(order.FilledPrice)
		if s.monitor != nil {
			if p := s.monitor.ResolveExitPrice(r.Context(), order); p > 0 {
				exitPrice = p
			}
		}

		qty := float64(order.FilledQuantity)
		var pnl float64
		if order.OrderSide == "BUY" {
			pnl = (exitPrice - safeF(order.FilledPrice)) * qty
		} else {
			pnl = (safeF(order.FilledPrice) - exitPrice) * qty
		}

		if err := s.repo.UpdateLiveTradeExit(r.Context(), order.OrderID, exitPrice, pnl); err != nil {
			log.Printf("[live-ws] Failed to record exit for order %s: %v", order.OrderID, err)
		}
	}

	// Broadcast completion to all connected live order WS clients for this user
	s.BroadcastLiveOrder(body.UserID, LiveOrderUpdate{
		Type:   "force_exit_done",
		UserID: body.UserID,
	})

	log.Printf("[live-ws] Force-exited all live orders for user %s", body.UserID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

// handleGetClosedPaperOrders returns all closed (exited) paper positions for a user.
// GET /ws/paper-trades/closed-orders?user_id=xxx
func (s *PaperWSServer) handleGetClosedPaperOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}
	orders, err := s.repo.GetClosedPaperOrdersByUser(r.Context(), userID)
	if err != nil {
		http.Error(w, "failed to fetch closed paper orders: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"orders":  orders,
	})
}

// handleGetClosedLiveOrders returns all closed (force-exited) live orders for a user.
// GET /ws/live-orders/closed-orders?user_id=xxx
func (s *PaperWSServer) handleGetClosedLiveOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}
	orders, err := s.repo.GetClosedLiveOrdersByUser(r.Context(), userID)
	if err != nil {
		http.Error(w, "failed to fetch closed live orders: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"orders":  orders,
	})
}

// handleGetDashboardStats returns aggregated stats (total invested, realized PnL, counts).
// GET /ws/dashboard-stats?user_id=xxx&mode=paper  (mode: paper or live)
func (s *PaperWSServer) handleGetDashboardStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}
	mode := r.URL.Query().Get("mode")
	isPaper := mode != "live"

	stats, err := s.repo.GetDashboardStats(r.Context(), userID, isPaper)
	if err != nil {
		http.Error(w, "failed to fetch dashboard stats: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"stats":   stats,
	})
}

// handleIndiraPositions fetches ALL positions from Indira and returns only those
// that correspond to orders placed by our algo system.
// GET /ws/live-orders/indira-positions?user_id=xxx
// Requires Authorization, appId, userId, source headers (set by the Next.js proxy).
func (s *PaperWSServer) handleIndiraPositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.positionsFetcher == nil {
		http.Error(w, "positions fetcher not configured", http.StatusServiceUnavailable)
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	// Extract Indira auth from headers set by the Next.js proxy from httpOnly cookies.
	bearerToken := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	appId := r.Header.Get("appId")
	if appId == "" {
		appId = r.Header.Get("appid")
	}
	source := r.Header.Get("source")
	if source == "" {
		source = "WEB"
	}
	if bearerToken == "" {
		http.Error(w, "Authorization header required", http.StatusUnauthorized)
		return
	}

	// 1. Fetch ALL positions from Indira.
	brokerPositions, err := s.positionsFetcher(r.Context(), bearerToken, appId, userID, source)
	if err != nil {
		log.Printf("[live-ws] Failed to fetch Indira positions for user %s: %v", userID, err)
		http.Error(w, "failed to fetch broker positions: "+err.Error(), http.StatusBadGateway)
		return
	}

	// 2. Get our algo orders so we can filter to only algo-placed positions.
	algoOrders, err := s.repo.GetLiveOrdersByUser(r.Context(), userID)
	if err != nil {
		log.Printf("[live-ws] Failed to fetch algo orders for user %s: %v", userID, err)
		http.Error(w, "failed to fetch algo orders", http.StatusInternalServerError)
		return
	}

	// Build symbol → latest order map (prefer FILLED over other statuses).
	symbolToOrder := make(map[string]*models.Order, len(algoOrders))
	for _, order := range algoOrders {
		sym := strings.ToUpper(order.Symbol)
		existing, exists := symbolToOrder[sym]
		if !exists || (order.Status == models.StatusFilled && existing.Status != models.StatusFilled) {
			cp := *order
			symbolToOrder[sym] = &cp
		}
	}

	// 3. Filter broker positions to only algo-placed ones, enriching with order metadata.
	result := make([]EnrichedAlgoPosition, 0, len(brokerPositions))
	for _, bp := range brokerPositions {
		if bp.NetQty == 0 {
			continue // skip flat positions
		}
		// Match broker symbol (e.g. "STK_TCS_EQ_NSE_11536") against our symbol ("TCS").
		bpSymUpper := strings.ToUpper(bp.Symbol)
		var matchedOrder *models.Order
		for sym, o := range symbolToOrder {
			if bpSymUpper == sym || strings.Contains(bpSymUpper, sym) || strings.Contains(sym, bpSymUpper) {
				matchedOrder = o
				break
			}
		}
		if matchedOrder == nil {
			continue // not placed by our algo — skip
		}

		ep := EnrichedAlgoPosition{
			Symbol:       bp.Symbol,
			Exchange:     bp.Exchange,
			ProductType:  bp.ProductType,
			NetQty:       bp.NetQty,
			BuyQty:       bp.BuyQty,
			SellQty:      bp.SellQty,
			BuyAvgPrice:  bp.BuyAvgPrice,
			SellAvgPrice: bp.SellAvgPrice,
			CurrentPrice: bp.CurrentPrice,
			BrokerPnL:    bp.PnL,
			BrokerPnLPct: bp.PnLPercentage,
			// Algo order metadata
			OrderID:     matchedOrder.OrderID.String(),
			StrategyID:  matchedOrder.StrategyID,
			OrderSide:   string(matchedOrder.OrderSide),
			FilledQty:   matchedOrder.FilledQuantity,
			TradingMode: matchedOrder.TradingMode,
			Status:      string(matchedOrder.Status),
		}
		if matchedOrder.IndiraOrderID != nil {
			ep.IndiraOrderID = *matchedOrder.IndiraOrderID
		}
		if matchedOrder.FilledPrice != nil {
			ep.FilledPrice = *matchedOrder.FilledPrice
		}
		result = append(result, ep)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":        true,
		"positions":      result,
		"total_broker":   len(brokerPositions),
		"total_filtered": len(result),
	})
}

// BroadcastLiveOrder sends a LiveOrderUpdate to all connected live order WS clients for a user.
func (s *PaperWSServer) BroadcastLiveOrder(userID string, update LiveOrderUpdate) {
	s.liveMu.RLock()
	conns := make(map[*websocket.Conn]*sync.Mutex, len(s.liveClients[userID]))
	for c, mu := range s.liveClients[userID] {
		conns[c] = mu
	}
	s.liveMu.RUnlock()

	if len(conns) == 0 {
		return
	}

	for conn, mu := range conns {
		mu.Lock()
		conn.SetWriteDeadline(time.Now().Add(writeWait))
		err := conn.WriteJSON(update)
		conn.SetWriteDeadline(time.Time{})
		mu.Unlock()
		if err != nil {
			log.Printf("[live-ws] Failed to send to user %s: %v — removing conn", userID, err)
			s.unregisterLiveClient(userID, conn)
		}
	}
}

func (s *PaperWSServer) registerLiveClient(userID string, conn *websocket.Conn) *sync.Mutex {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveClients[userID] == nil {
		s.liveClients[userID] = make(map[*websocket.Conn]*sync.Mutex)
	}
	mu := &sync.Mutex{}
	s.liveClients[userID][conn] = mu
	return mu
}

func (s *PaperWSServer) unregisterLiveClient(userID string, conn *websocket.Conn) {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveClients[userID] != nil {
		delete(s.liveClients[userID], conn)
		if len(s.liveClients[userID]) == 0 {
			delete(s.liveClients, userID)
		}
	}
}

// handleSubscribeBrokerWS starts the per-user Indira broker WebSocket subscription immediately
// when a strategy is created/activated, so real-time order status updates begin before the
// first order is placed.
//
// POST /ws/live-orders/subscribe-broker-ws
// Headers: Authorization (Bearer token), appId, source, userId
func (s *PaperWSServer) handleSubscribeBrokerWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.startBrokerWS == nil {
		// Not fatal — live trading WS subscription not configured (e.g. paper-only mode)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true,"note":"broker ws not configured"}`))
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		http.Error(w, `{"error":"user_id required"}`, http.StatusBadRequest)
		return
	}

	bearerToken := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	appID := r.Header.Get("appId")
	source := r.Header.Get("source")

	if bearerToken == "" || appID == "" || source == "" {
		http.Error(w, `{"error":"Authorization, appId and source headers required"}`, http.StatusUnauthorized)
		return
	}

	if err := s.startBrokerWS(r.Context(), userID, bearerToken, appID, source); err != nil {
		log.Printf("[subscribe-broker-ws] failed for user %s: %v", userID, err)
		http.Error(w, `{"error":"failed to start broker ws subscription"}`, http.StatusInternalServerError)
		return
	}

	log.Printf("[subscribe-broker-ws] started Indira WS subscription for user %s (strategy activated)", userID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"ok":true}`))
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP server
// ─────────────────────────────────────────────────────────────────────────────

// StartHTTPServer starts a standalone HTTP server for paper and live order endpoints.
// Call this in a goroutine. addr example: ":8081"
func (s *PaperWSServer) StartHTTPServer(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	log.Printf("[paper-ws] WebSocket server listening on %s (paper-trades + live-orders)", addr)
	return srv.ListenAndServe()
}
