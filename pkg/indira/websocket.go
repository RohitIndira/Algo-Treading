package indira

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	// PDF requires a heartbeat every 50 seconds.
	pingPeriod = 45 * time.Second

	// Maximum message size allowed from peer.
	maxMessageSize = 8192

	wsEndpoint = "wss://livemiddleware.indiratrade.com/order-notify/websocket"
	wsTokenAPI = "/order-notify/ws/createWsToken"

	// tokenRefreshPeriod refreshes the WS token proactively before the typical 1-hour expiry.
	tokenRefreshPeriod = 50 * time.Minute
)

// WSClient manages a WebSocket connection for a single user.
type WSClient struct {
	client *Client
	auth   *AuthContext

	// mu protects conn, stopCh, IsActive, isClosed, OrderToken.
	mu         sync.Mutex
	conn       *websocket.Conn
	stopCh     chan struct{}
	IsActive   bool
	isClosed   bool // set by Close(); prevents any further reconnects
	OrderToken string

	Updates chan *WSOrderStatus
	Wg      sync.WaitGroup
}

// NewWSClient creates a new WebSocket client for a specific user.
func NewWSClient(httpClient *Client, auth *AuthContext) *WSClient {
	return &WSClient{
		client:  httpClient,
		auth:    auth,
		Updates: make(chan *WSOrderStatus, 100),
	}
}

// GetWebSocketToken hits the REST API to exchange the user's session token for a WebSocket Order Token.
func (w *WSClient) GetWebSocketToken(ctx context.Context) (string, error) {
	resp, err := w.client.doRequest(ctx, w.auth, "GET", wsTokenAPI, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call createWsToken API: %w", err)
	}

	var tokenResp WebSocketTokenResponse
	if err := json.Unmarshal(resp.Data, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to unmarshal token response: %w", err)
	}

	if len(tokenResp.Result) == 0 || tokenResp.Result[0].OrderToken == "" {
		return "", fmt.Errorf("no order token received from API. status: %s", tokenResp.Status)
	}

	return tokenResp.Result[0].OrderToken, nil
}

// Connect starts the WebSocket connection and background loops.
// Should be called once per WSClient instance.
func (w *WSClient) Connect(ctx context.Context) error {
	w.mu.Lock()
	if w.IsActive || w.isClosed {
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()

	// Fetch token outside the lock (network call).
	token, err := w.GetWebSocketToken(ctx)
	if err != nil {
		return err
	}

	w.mu.Lock()
	if w.isClosed {
		w.mu.Unlock()
		return fmt.Errorf("client has been permanently closed")
	}
	w.OrderToken = token
	initialStopCh, err := w.dialLocked()
	w.mu.Unlock()

	if err != nil {
		return err
	}

	// Start the monitor goroutine exactly once — it handles all future reconnects.
	w.Wg.Add(1)
	go w.monitorReconnect(initialStopCh)

	log.Printf("[ws] Client connected for user: %s", w.auth.UserId)
	return nil
}

// dialLocked opens the WebSocket connection, sends auth, and starts read/write pumps.
// Must be called with w.mu held. w.OrderToken must already be set.
// Returns the stopCh for the new session.
func (w *WSClient) dialLocked() (chan struct{}, error) {
	conn, _, err := websocket.DefaultDialer.Dial(wsEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket dial failed: %w", err)
	}

	// Send auth message.
	authReq := WSConnectionRequest{
		UserId:     w.auth.UserId,
		OrderToken: w.OrderToken,
	}
	authBytes, _ := json.Marshal(authReq)
	if err := conn.WriteMessage(websocket.TextMessage, authBytes); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send ws auth message: %w", err)
	}

	stopCh := make(chan struct{})
	w.conn = conn
	w.stopCh = stopCh
	w.IsActive = true

	// Start pumps with their own references — prevents stale-conn writes after reconnect.
	w.Wg.Add(2)
	go w.readPump(conn)
	go w.writePump(conn, stopCh)

	return stopCh, nil
}

// Close gracefully stops the WebSocket connection and prevents future reconnects.
func (w *WSClient) Close() {
	w.mu.Lock()
	if w.isClosed {
		w.mu.Unlock()
		return
	}
	w.isClosed = true
	w.IsActive = false
	stopCh := w.stopCh
	conn := w.conn
	w.mu.Unlock()

	// Close outside the lock to avoid deadlocks with goroutines that need the mutex.
	if stopCh != nil {
		close(stopCh)
	}
	if conn != nil {
		conn.Close()
	}
	w.Wg.Wait()
}

// readPump reads messages from the given conn. Owns its own conn reference —
// safe against reconnects that replace w.conn.
func (w *WSClient) readPump(conn *websocket.Conn) {
	defer func() {
		w.Wg.Done()
		conn.Close()
		w.mu.Lock()
		// Only mark inactive if this pump's conn is still the current one.
		if w.conn == conn {
			w.IsActive = false
		}
		w.mu.Unlock()
	}()

	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[ws] Read error user %s: %v", w.auth.UserId, err)
			}
			break
		}
		conn.SetReadDeadline(time.Now().Add(pongWait))

		var orderStatus WSOrderStatus
		if err := json.Unmarshal(message, &orderStatus); err == nil &&
			(orderStatus.UniqueCode != "" || orderStatus.OrderStatus != "") {
			select {
			case w.Updates <- &orderStatus:
			default:
				log.Printf("[ws] Updates channel full for user %s, dropping message", w.auth.UserId)
			}
		} else {
			log.Printf("[ws] Info msg user %s: %s", w.auth.UserId, string(message))
		}
	}
}

// writePump sends heartbeats on the given conn and stops when stopCh is closed.
// Owns its own conn and stopCh references — safe against reconnects.
func (w *WSClient) writePump(conn *websocket.Conn, stopCh chan struct{}) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
		w.Wg.Done()
	}()

	for {
		select {
		case <-stopCh:
			conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			hb := WSHeartbeat{UserId: w.auth.UserId, Heartbeat: "h"}
			hbBytes, _ := json.Marshal(hb)
			if err := conn.WriteMessage(websocket.TextMessage, hbBytes); err != nil {
				return
			}
			conn.SetWriteDeadline(time.Time{})
		}
	}
}

// monitorReconnect auto-reconnects when the connection drops and proactively refreshes
// the token. Started exactly once by Connect — must NOT be called elsewhere.
// initialStopCh is the stopCh of the first session, used as the initial select target.
func (w *WSClient) monitorReconnect(initialStopCh chan struct{}) {
	defer w.Wg.Done()

	tokenRefreshTicker := time.NewTicker(tokenRefreshPeriod)
	defer tokenRefreshTicker.Stop()

	// currentStopCh tracks the stopCh of the active session so we exit when Close() fires.
	currentStopCh := initialStopCh

	for {
		select {
		case <-currentStopCh:
			// stopCh was closed — check if this is a permanent shutdown.
			w.mu.Lock()
			closed := w.isClosed
			w.mu.Unlock()
			if closed {
				return
			}
			// Not permanent (shouldn't normally happen) — update reference and continue.
			w.mu.Lock()
			currentStopCh = w.stopCh
			w.mu.Unlock()

		case <-tokenRefreshTicker.C:
			// Proactively refresh token so the next reconnect uses a fresh one.
			w.mu.Lock()
			closed := w.isClosed
			w.mu.Unlock()
			if closed {
				return
			}
			if token, err := w.GetWebSocketToken(context.Background()); err == nil {
				w.mu.Lock()
				w.OrderToken = token
				w.mu.Unlock()
				log.Printf("[ws] Token proactively refreshed for user %s", w.auth.UserId)
			} else {
				log.Printf("[ws] Token refresh failed for user %s: %v", w.auth.UserId, err)
			}

		case <-time.After(5 * time.Second):
			w.mu.Lock()
			active := w.IsActive
			closed := w.isClosed
			w.mu.Unlock()

			if closed {
				return
			}
			if active {
				continue
			}

			log.Printf("[ws] Auto-reconnecting for user %s...", w.auth.UserId)

			// Fetch fresh token outside the lock (network call).
			token, err := w.GetWebSocketToken(context.Background())
			if err != nil {
				log.Printf("[ws] Token fetch failed for user %s: %v", w.auth.UserId, err)
				continue
			}

			w.mu.Lock()
			if w.isClosed {
				w.mu.Unlock()
				return
			}
			w.OrderToken = token
			newStopCh, dialErr := w.dialLocked()
			w.mu.Unlock()

			if dialErr != nil {
				log.Printf("[ws] Reconnect failed for user %s: %v", w.auth.UserId, dialErr)
				continue
			}

			currentStopCh = newStopCh
			log.Printf("[ws] Reconnected for user %s", w.auth.UserId)
		}
	}
}
