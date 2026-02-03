package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
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
	// clients is a per-user map of websocket connections. We store a per-connection
	// write mutex to avoid gorilla/websocket panics on concurrent writes.
	clients     map[string]map[*websocket.Conn]*wsClient // user_id -> conn -> wrapper
	mu          sync.RWMutex

	// Kafka-backed PnL streaming (paper trading)
	kafkaBrokers []string
	pnlTopic     string
	pnlOnce      sync.Once
	pnlCancel    context.CancelFunc

	// latest pnl snapshot per user (raw json), so we can immediately push
	// the last known snapshot on new WebSocket connections.
	lastPnL map[string]json.RawMessage
}

type wsClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func NewWebSocketHandler(redisClient *redis.Client, logger *zap.Logger, kafkaBrokers []string, pnlTopic string) *WebSocketHandler {
	if len(kafkaBrokers) == 0 {
		// fallback to env to keep backwards compat
		brokersStr := os.Getenv("KAFKA_BROKERS")
		if brokersStr == "" {
			brokersStr = "localhost:9092"
		}
		for _, b := range strings.Split(brokersStr, ",") {
			b = strings.TrimSpace(b)
			if b != "" {
				kafkaBrokers = append(kafkaBrokers, b)
			}
		}
	}
	if pnlTopic == "" {
		pnlTopic = os.Getenv("KAFKA_TOPIC_PAPER_PNL")
		if pnlTopic == "" {
			pnlTopic = "paper-pnl.52w"
		}
	}

	return &WebSocketHandler{
		redisClient: redisClient,
		logger:      logger,
		clients:     make(map[string]map[*websocket.Conn]*wsClient),
		kafkaBrokers: kafkaBrokers,
		pnlTopic:     pnlTopic,
		lastPnL:      make(map[string]json.RawMessage),
	}
}

func (h *WebSocketHandler) safeWriteJSON(userID string, conn *websocket.Conn, v any) error {
	h.mu.RLock()
	userConns := h.clients[userID]
	c := userConns[conn]
	h.mu.RUnlock()
	if c == nil {
		return fmt.Errorf("websocket not registered")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	return conn.WriteJSON(v)
}

func (h *WebSocketHandler) safeWriteMessage(userID string, conn *websocket.Conn, messageType int, data []byte) error {
	h.mu.RLock()
	userConns := h.clients[userID]
	c := userConns[conn]
	h.mu.RUnlock()
	if c == nil {
		return fmt.Errorf("websocket not registered")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	return conn.WriteMessage(messageType, data)
}

// startPnLConsumer wires /ws/pnl to Kafka topic paper-pnl.52w.
// It starts exactly once per gateway instance and continuously tails
// all partitions from the latest offset.
func (h *WebSocketHandler) startPnLConsumer() {
	h.pnlOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		h.pnlCancel = cancel

		if len(h.kafkaBrokers) == 0 {
			h.logger.Warn("PnL Kafka consumer not started: no brokers configured")
			return
		}
		if h.pnlTopic == "" {
			h.logger.Warn("PnL Kafka consumer not started: no topic configured")
			return
		}

		// Discover partitions.
		conn, err := kafka.Dial("tcp", h.kafkaBrokers[0])
		if err != nil {
			h.logger.Error("Failed to dial Kafka for PnL partition discovery", zap.Error(err))
			return
		}
		parts, err := conn.ReadPartitions(h.pnlTopic)
		_ = conn.Close()
		if err != nil {
			h.logger.Error("Failed to read Kafka partitions for PnL topic", zap.Error(err), zap.String("topic", h.pnlTopic))
			return
		}

		partitions := make([]int, 0, len(parts))
		for _, p := range parts {
			partitions = append(partitions, p.ID)
		}

		h.logger.Info("Starting Kafka-backed PnL consumer",
			zap.Strings("brokers", h.kafkaBrokers),
			zap.String("topic", h.pnlTopic),
			zap.Ints("partitions", partitions))

		for _, pid := range partitions {
			pid := pid
			go h.consumePnLPartition(ctx, pid)
		}
	})
}

func (h *WebSocketHandler) consumePnLPartition(ctx context.Context, partition int) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     h.kafkaBrokers,
		Topic:       h.pnlTopic,
		Partition:   partition,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.LastOffset, // tail new messages
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			h.logger.Warn("PnL kafka read error", zap.Error(err), zap.Int("partition", partition))
			time.Sleep(250 * time.Millisecond)
			continue
		}

		userID, raw := extractUserID(msg.Value)
		if userID == "" {
			continue
		}

		// Cache last snapshot
		h.mu.Lock()
		h.lastPnL[userID] = raw
		h.mu.Unlock()

		// Broadcast to websocket clients
		h.broadcastPnL(userID, raw)
	}
}

func extractUserID(payload []byte) (string, json.RawMessage) {
	if len(payload) == 0 {
		return "", nil
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return "", nil
	}
	uid, _ := m["user_id"].(string)
	return strings.TrimSpace(uid), json.RawMessage(payload)
}

func (h *WebSocketHandler) broadcastPnL(userID string, raw json.RawMessage) {
	if userID == "" || len(raw) == 0 {
		return
	}

	// Wrap so frontend can rely on a stable envelope.
	out := map[string]any{
		"type":    "pnl",
		"user_id": userID,
		"source":  "kafka",
		"topic":   h.pnlTopic,
		"data":    json.RawMessage(raw),
		"ts":      time.Now().Unix(),
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return
	}

	h.mu.RLock()
	conns := h.clients[userID]
	h.mu.RUnlock()
	for conn, wc := range conns {
		if conn == nil || wc == nil {
			continue
		}
		wc.writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		err := conn.WriteMessage(websocket.TextMessage, encoded)
		wc.writeMu.Unlock()
		if err != nil {
			h.logger.Info("PnL websocket write failed", zap.Error(err), zap.String("user_id", userID))
		}
	}
}

// HandlePnLFeed handles WebSocket connections for live PnL/portfolio
// updates per user.
//
//	GET /ws/pnl?user_id=ISPL19027
//
// Previous implementation streamed PnL snapshots from Redis Pub/Sub
// (user:{user_id}:pnl channels). The rules-engine now publishes 52W
// PnL/portfolio snapshots directly to Kafka, and this endpoint will be
// refactored later to consume from Kafka or from a dedicated HTTP API.
//
// For now, we keep the WebSocket endpoint but do not subscribe to Redis
// or any backend feed. The connection is established and a one-time
// informational message is sent to the client.
func (h *WebSocketHandler) HandlePnLFeed(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	// Ensure our Kafka consumer is running.
	h.startPnLConsumer()

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade to WebSocket (PnL)", zap.Error(err))
		return
	}
	defer conn.Close()

	h.logger.Info("WebSocket PnL connection established",
		zap.String("user_id", userID),
		zap.String("remote_addr", conn.RemoteAddr().String()))

	// Register this connection under the user
	h.registerClient(userID, conn)
	defer h.unregisterClient(userID, conn)

	// Send connected ack.
	msg := map[string]any{
		"type":    "connected",
		"subtype": "pnl",
		"message": fmt.Sprintf("Subscribed to Kafka topic %s for PnL", h.pnlTopic),
		"user_id": userID,
	}
	_ = h.safeWriteJSON(userID, conn, msg)

	// If we already have a last snapshot cached, send it immediately so
	// frontend can render without waiting for the next tick.
	h.mu.RLock()
	last := h.lastPnL[userID]
	h.mu.RUnlock()
	if len(last) > 0 {
		// re-use broadcast envelope but send only to this conn
		env := map[string]any{
			"type":    "pnl",
			"user_id": userID,
			"source":  "kafka-cache",
			"topic":   h.pnlTopic,
			"data":    json.RawMessage(last),
			"ts":      time.Now().Unix(),
		}
		_ = h.safeWriteJSON(userID, conn, env)
	}

	// Keep the socket open; messages are pushed from the background Kafka
	// consumer via broadcastPnL().
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			return
		}
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

// listenAndForwardPnL is similar to listenAndForward but tags outbound
// messages as PnL events instead of match events. The Redis payload is
// expected to already be JSON representing a portfolio/PnL snapshot.
func (h *WebSocketHandler) listenAndForwardPnL(ctx context.Context, conn *websocket.Conn, ch <-chan *redis.Message, userID string) {
	// Heartbeat ticker to keep connection alive
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg := <-ch:
			if msg == nil {
				return
			}

			h.logger.Debug("Received PnL message from Redis",
				zap.String("channel", msg.Channel),
				zap.String("payload", msg.Payload))

			var snapshot map[string]any
			if err := json.Unmarshal([]byte(msg.Payload), &snapshot); err != nil {
				h.logger.Error("Failed to parse PnL snapshot", zap.Error(err))
				continue
			}

			// Wrap with metadata
			wrapper := map[string]any{
				"type":          "pnl",
				"user_id":       userID,
				"redis_channel": msg.Channel,
				"snapshot":      snapshot,
			}

			if err := conn.WriteJSON(wrapper); err != nil {
				// Most commonly this means the client disconnected; log at
				// info level to avoid noisy stacktraces, and exit the loop.
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
					h.logger.Info("PnL WebSocket closed while sending snapshot",
						zap.Error(err),
						zap.String("user_id", userID))
				} else {
					h.logger.Info("PnL WebSocket write error (snapshot)",
						zap.Error(err),
						zap.String("user_id", userID))
				}
				return
			}

		case <-ticker.C:
			heartbeat := map[string]any{
				"type":      "heartbeat",
				"subtype":   "pnl",
				"timestamp": time.Now().Unix(),
			}
			if err := conn.WriteJSON(heartbeat); err != nil {
				// Broken pipe here almost always means the browser/tab closed
				// the connection. Treat it as a normal disconnect rather than
				// an error with stacktrace spam.
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
					h.logger.Info("PnL WebSocket closed by client during heartbeat",
						zap.Error(err),
						zap.String("user_id", userID))
				} else {
					h.logger.Info("PnL WebSocket write error (heartbeat)",
						zap.Error(err),
						zap.String("user_id", userID))
				}
				return
			}

		case <-ctx.Done():
			h.logger.Info("Context cancelled, closing PnL WebSocket", zap.String("user_id", userID))
			return
		}
	}
}

// registerClient registers a new client connection
func (h *WebSocketHandler) registerClient(userID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.clients[userID] == nil {
		h.clients[userID] = make(map[*websocket.Conn]*wsClient)
	}
	if _, exists := h.clients[userID][conn]; !exists {
		h.clients[userID][conn] = &wsClient{conn: conn}
	}

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
