package indira

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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

// WSClient manages a WebSocket connection.
// In shared-connection mode the same WSClient is used for multiple users:
// additional users subscribe via Subscribe(ctx, auth), which sends a
// WSConnectionRequest on the live connection; the server fans updates back
// on the same stream with each message carrying the UserID field for routing.
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
	sendCh  chan []byte // outbound subscription/control messages
	Wg      sync.WaitGroup

	// OnReconnected is called in a new goroutine after every successful reconnect.
	// Used by statusservice to re-subscribe additional users after a drop. May be nil.
	OnReconnected func()

	// OnAuthRefresh is called when a 401 is received during token fetch, indicating
	// the stored BearerToken has expired. The callback should return a fresh AuthContext
	// (e.g. from the DB credentials cache). May be nil — if unset, 401s are not recoverable.
	OnAuthRefresh func(userID string) (*AuthContext, error)
}

// NewWSClient creates a new WebSocket client for a specific user.
func NewWSClient(httpClient *Client, auth *AuthContext) *WSClient {
	return &WSClient{
		client:  httpClient,
		auth:    auth,
		Updates: make(chan *WSOrderStatus, 10000),
		sendCh:  make(chan []byte, 64),
	}
}

// Subscribe authenticates an additional user on the shared connection.
// It fetches a per-user WS order token then sends WSConnectionRequest
// over the existing live connection. Safe to call concurrently.
func (w *WSClient) Subscribe(ctx context.Context, auth *AuthContext) error {
	resp, err := w.client.doRequest(ctx, auth, "GET", wsTokenAPI, nil)
	if err != nil {
		return fmt.Errorf("get WS token for user %s: %w", auth.UserId, err)
	}
	var tokenResp WebSocketTokenResponse
	if err := json.Unmarshal(resp.Data, &tokenResp); err != nil {
		return fmt.Errorf("unmarshal WS token: %w", err)
	}
	if len(tokenResp.Result) == 0 || tokenResp.Result[0].OrderToken == "" {
		return fmt.Errorf("no order token received for user %s", auth.UserId)
	}
	return w.SendMessage(WSConnectionRequest{
		UserId:     auth.UserId,
		OrderToken: tokenResp.Result[0].OrderToken,
	})
}

// SendMessage enqueues a payload to be written on the active WS connection.
// Returns an error if the client is inactive or the send buffer is full.
func (w *WSClient) SendMessage(payload interface{}) error {
	w.mu.Lock()
	active := w.IsActive
	w.mu.Unlock()
	if !active {
		return fmt.Errorf("ws client is not active")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ws message: %w", err)
	}
	select {
	case w.sendCh <- data:
		return nil
	default:
		return fmt.Errorf("ws send buffer full — dropping message")
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
		// Ignore unmarshal errors from type mismatches — the broker sends
		// some fields as int in one message and string in another (e.g.
		// Exchange, Days, ManagerID, ScripCode). The key routing fields
		// (UniqueCode, OrderStatus) are always strings and will be
		// populated correctly regardless of errors on other fields.
		_ = json.Unmarshal(message, &orderStatus)
		if orderStatus.UniqueCode != "" || orderStatus.OrderStatus != "" {
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

// writePump sends heartbeats and queued outbound messages on the given conn.
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
		case payload := <-w.sendCh:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
			conn.SetWriteDeadline(time.Time{})
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

// tryAuthRefresh checks if err is a 401 and, if OnAuthRefresh is set, calls it to
// update w.auth with fresh credentials from the DB. Returns true if auth was refreshed.
func (w *WSClient) tryAuthRefresh(err error) bool {
	if err == nil || w.OnAuthRefresh == nil {
		return false
	}
	if !strings.Contains(err.Error(), "HTTP error 401") {
		return false
	}
	log.Printf("[ws] 401 detected for user %s — attempting auth refresh from DB", w.auth.UserId)
	newAuth, refreshErr := w.OnAuthRefresh(w.auth.UserId)
	if refreshErr != nil {
		log.Printf("[ws] Auth refresh failed for user %s: %v", w.auth.UserId, refreshErr)
		return false
	}
	if newAuth.BearerToken == w.auth.BearerToken {
		log.Printf("[ws] Auth refresh returned same token for user %s — frontend may not have re-logged in yet", w.auth.UserId)
		return false
	}
	w.mu.Lock()
	w.auth = newAuth
	w.mu.Unlock()
	log.Printf("[ws] Auth refreshed for user %s — retrying with new credentials", w.auth.UserId)
	return true
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
			} else if w.tryAuthRefresh(err) {
				// Auth was refreshed after 401 — retry token fetch immediately.
				if token, err2 := w.GetWebSocketToken(context.Background()); err2 == nil {
					w.mu.Lock()
					w.OrderToken = token
					w.mu.Unlock()
					log.Printf("[ws] Token proactively refreshed for user %s (after auth refresh)", w.auth.UserId)
				} else {
					log.Printf("[ws] Token refresh still failed for user %s after auth refresh: %v", w.auth.UserId, err2)
				}
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
				// On 401, try refreshing credentials from DB and retry once.
				if w.tryAuthRefresh(err) {
					token, err = w.GetWebSocketToken(context.Background())
				}
				if err != nil {
					log.Printf("[ws] Token fetch failed for user %s: %v", w.auth.UserId, err)
					continue
				}
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

			if w.OnReconnected != nil {
				go w.OnReconnected()
			}
		}
	}
}
