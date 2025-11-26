package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now (configure properly in production)
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type WebSocketHandler struct {
	redisClient *redis.Client
	logger      *zap.Logger
	clients     map[string]map[*websocket.Conn]bool // user_id -> set of connections
	mu          sync.RWMutex
}

func NewWebSocketHandler(redisClient *redis.Client, logger *zap.Logger) *WebSocketHandler {
	return &WebSocketHandler{
		redisClient: redisClient,
		logger:      logger,
		clients:     make(map[string]map[*websocket.Conn]bool),
	}
}

// HandleMatchesFeed handles WebSocket connections for live match updates
// GET /ws/matches?user_id=xxx
func (h *WebSocketHandler) HandleMatchesFeed(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	h.handleUserMatchFeed(w, r, userID)
}

// HandleAllMatchesFeed handles WebSocket connections for ALL users' match updates
// GET /ws/matches/all
func (h *WebSocketHandler) HandleAllMatchesFeed(w http.ResponseWriter, r *http.Request) {
	h.handleAllUsersMatchFeed(w, r)
}

// handleUserMatchFeed handles match feed for a specific user
func (h *WebSocketHandler) handleUserMatchFeed(w http.ResponseWriter, r *http.Request, userID string) {

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade to WebSocket", zap.Error(err))
		return
	}
	defer conn.Close()

	h.logger.Info("WebSocket connection established",
		zap.String("user_id", userID),
		zap.String("remote_addr", conn.RemoteAddr().String()))

	// Register this connection
	h.registerClient(userID, conn)
	defer h.unregisterClient(userID, conn)

	// Send welcome message
	welcomeMsg := map[string]interface{}{
		"type":    "connected",
		"message": "Connected to live match feed",
		"user_id": userID,
	}
	if err := conn.WriteJSON(welcomeMsg); err != nil {
		h.logger.Error("Failed to send welcome message", zap.Error(err))
		return
	}

	// Subscribe to Redis Pub/Sub for this user
	ctx := context.Background()
	channel := "user:" + userID + ":matches"
	pubsub := h.redisClient.Subscribe(ctx, channel)
	defer pubsub.Close()

	h.logger.Info("Subscribed to Redis channel",
		zap.String("channel", channel),
		zap.String("user_id", userID))

	// Channel to receive messages from Redis
	ch := pubsub.Channel()

	h.listenAndForward(ctx, conn, ch, userID)
}

// handleAllUsersMatchFeed handles match feed for ALL users
func (h *WebSocketHandler) handleAllUsersMatchFeed(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade to WebSocket", zap.Error(err))
		return
	}
	defer conn.Close()

	h.logger.Info("WebSocket connection established for ALL users",
		zap.String("remote_addr", conn.RemoteAddr().String()))

	// Send welcome message
	welcomeMsg := map[string]interface{}{
		"type":    "connected",
		"message": "Connected to ALL users live match feed",
		"scope":   "all_users",
	}
	if err := conn.WriteJSON(welcomeMsg); err != nil {
		h.logger.Error("Failed to send welcome message", zap.Error(err))
		return
	}

	// Subscribe to Redis Pub/Sub pattern for ALL users
	ctx := context.Background()
	pattern := "user:*:matches"
	pubsub := h.redisClient.PSubscribe(ctx, pattern)
	defer pubsub.Close()

	h.logger.Info("Subscribed to Redis pattern",
		zap.String("pattern", pattern))

	// Channel to receive messages from Redis
	ch := pubsub.Channel()

	h.listenAndForward(ctx, conn, ch, "ALL_USERS")
}

// listenAndForward listens for Redis messages and forwards them to WebSocket client
func (h *WebSocketHandler) listenAndForward(ctx context.Context, conn *websocket.Conn, ch <-chan *redis.Message, identifier string) {

	// Heartbeat ticker to keep connection alive
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Listen for messages
	for {
		select {
		case msg := <-ch:
			// Received message from Redis, forward to WebSocket client
			h.logger.Debug("Received message from Redis",
				zap.String("channel", msg.Channel),
				zap.String("payload", msg.Payload))

			// Parse the message
			var matchEvent map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &matchEvent); err != nil {
				h.logger.Error("Failed to parse match event", zap.Error(err))
				continue
			}

			// Add type field and channel info
			matchEvent["type"] = "match"
			matchEvent["redis_channel"] = msg.Channel

			// Send to WebSocket client
			if err := conn.WriteJSON(matchEvent); err != nil {
				h.logger.Error("Failed to send message to WebSocket client",
					zap.Error(err),
					zap.String("identifier", identifier))
				return // Connection closed
			}

			h.logger.Info("Sent match event to WebSocket client",
				zap.String("identifier", identifier),
				zap.String("channel", msg.Channel),
				zap.Any("order_id", matchEvent["order_id"]))

		case <-ticker.C:
			// Send heartbeat to keep connection alive
			heartbeat := map[string]interface{}{
				"type":      "heartbeat",
				"timestamp": time.Now().Unix(),
			}
			if err := conn.WriteJSON(heartbeat); err != nil {
				h.logger.Error("Failed to send heartbeat", zap.Error(err))
				return // Connection closed
			}

		case <-ctx.Done():
			h.logger.Info("Context cancelled, closing WebSocket", zap.String("identifier", identifier))
			return
		}
	}
}

// registerClient registers a new client connection
func (h *WebSocketHandler) registerClient(userID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.clients[userID] == nil {
		h.clients[userID] = make(map[*websocket.Conn]bool)
	}
	h.clients[userID][conn] = true

	h.logger.Debug("Client registered",
		zap.String("user_id", userID),
		zap.Int("total_connections", len(h.clients[userID])))
}

// unregisterClient unregisters a client connection
func (h *WebSocketHandler) unregisterClient(userID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.clients[userID] != nil {
		delete(h.clients[userID], conn)
		if len(h.clients[userID]) == 0 {
			delete(h.clients, userID)
		}
	}

	h.logger.Debug("Client unregistered",
		zap.String("user_id", userID),
		zap.Int("remaining_connections", len(h.clients[userID])))
}

// GetConnectedClients returns the number of connected clients for a user
func (h *WebSocketHandler) GetConnectedClients(userID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.clients[userID] == nil {
		return 0
	}
	return len(h.clients[userID])
}

// GetTotalConnections returns the total number of WebSocket connections
func (h *WebSocketHandler) GetTotalConnections() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, conns := range h.clients {
		total += len(conns)
	}
	return total
}
