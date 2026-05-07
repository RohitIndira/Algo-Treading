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

// tokenExpiredDelays defines wait durations before each retry attempt when a 401 is detected.
// Attempt 1: 30s, Attempt 2: 5min, Attempt 3: 10min. After all 3 fail → suspended.
var tokenExpiredDelays = [3]time.Duration{
	30 * time.Second,
	5 * time.Minute,
	10 * time.Minute,
}

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

	// OnTokenExpired is called after the 30-second first retry also fails with 401,
	// confirming the bearer token is genuinely expired. Used to notify the frontend
	// to prompt the user to re-login. May be nil.
	OnTokenExpired func(userID string)

	// resumeCh receives a fresh AuthContext from ResumeWithNewAuth when the frontend
	// provides a new bearer token after the user re-logins. Buffered(1) so the caller never blocks.
	resumeCh chan *AuthContext
}

// NewWSClient creates a new WebSocket client for a specific user.
func NewWSClient(httpClient *Client, auth *AuthContext) *WSClient {
	return &WSClient{
		client:   httpClient,
		auth:     auth,
		Updates:  make(chan *WSOrderStatus, 10000),
		sendCh:   make(chan []byte, 64),
		resumeCh: make(chan *AuthContext, 1),
	}
}

// ResumeWithNewAuth supplies fresh credentials after the WS has been suspended due to
// token expiry. The monitor goroutine picks it up and immediately retries connection.
func (w *WSClient) ResumeWithNewAuth(auth *AuthContext) {
	// Drain any stale pending auth before sending the new one.
	select {
	case <-w.resumeCh:
	default:
	}
	w.resumeCh <- auth
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
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

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
			// Send WebSocket-level ping frame — the server MUST respond with pong,
			// which resets the read deadline in the pong handler (keeps connection alive
			// even when no application messages are flowing).
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
			// Also send application-level heartbeat that the broker expects.
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
//
// Token-expiry retry policy (401 errors):
//   - Attempt 1: wait 30s then retry
//   - Attempt 2: wait 5min then retry; also notifies OnTokenExpired at this point
//   - Attempt 3: wait 10min then retry
//   - After all 3 fail: suspended — waits for ResumeWithNewAuth from frontend
func (w *WSClient) monitorReconnect(initialStopCh chan struct{}) {
	defer w.Wg.Done()

	tokenRefreshTicker := time.NewTicker(tokenRefreshPeriod)
	defer tokenRefreshTicker.Stop()

	currentStopCh := initialStopCh

	// tokenExpiredAttempts counts how many token-expiry retry delays have been scheduled.
	// 0 = normal mode; 1-3 = progressive retry; ≥ len(tokenExpiredDelays) = suspended.
	tokenExpiredAttempts := 0
	var tokenExpiredTimer <-chan time.Time

	// enterExpiredMode schedules the next retry delay after a 401 failure.
	// Returns true if a retry was scheduled, false when all retries exhausted (suspended).
	enterExpiredMode := func() bool {
		if tokenExpiredAttempts >= len(tokenExpiredDelays) {
			tokenExpiredTimer = nil
			log.Printf("[ws] All token-expiry retries exhausted for user %s — suspended until new auth", w.auth.UserId)
			return false
		}
		delay := tokenExpiredDelays[tokenExpiredAttempts]
		tokenExpiredTimer = time.After(delay)
		tokenExpiredAttempts++
		log.Printf("[ws] Token expired for user %s — retry %d/%d in %v",
			w.auth.UserId, tokenExpiredAttempts, len(tokenExpiredDelays), delay)
		// Notify frontend after the 30s first retry also fails (attempt 2 about to start).
		if tokenExpiredAttempts == 2 && w.OnTokenExpired != nil {
			go w.OnTokenExpired(w.auth.UserId)
		}
		return true
	}

	// dialAndUpdate performs the actual dial and updates currentStopCh on success.
	dialAndUpdate := func(token string) (chan struct{}, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.isClosed {
			return nil, fmt.Errorf("client permanently closed")
		}
		w.OrderToken = token
		newStopCh, err := w.dialLocked()
		if err != nil {
			return nil, err
		}
		return newStopCh, nil
	}

	for {
		// normalReconnectCh polls every 5s — disabled while in token-expiry retry mode.
		var normalReconnectCh <-chan time.Time
		if tokenExpiredAttempts == 0 {
			normalReconnectCh = time.After(5 * time.Second)
		}

		select {
		case <-currentStopCh:
			w.mu.Lock()
			closed := w.isClosed
			w.mu.Unlock()
			if closed {
				return
			}
			w.mu.Lock()
			currentStopCh = w.stopCh
			w.mu.Unlock()

		case <-tokenRefreshTicker.C:
			// Proactively refresh token — skip if already in token-expiry retry mode.
			if tokenExpiredAttempts > 0 {
				continue
			}
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
				if token, err2 := w.GetWebSocketToken(context.Background()); err2 == nil {
					w.mu.Lock()
					w.OrderToken = token
					w.mu.Unlock()
					log.Printf("[ws] Token proactively refreshed for user %s (after auth refresh)", w.auth.UserId)
				} else {
					log.Printf("[ws] Token refresh still failed for user %s after auth refresh: %v", w.auth.UserId, err2)
					if strings.Contains(err2.Error(), "401") {
						enterExpiredMode()
					}
				}
			} else {
				log.Printf("[ws] Token refresh failed for user %s: %v", w.auth.UserId, err)
				if strings.Contains(err.Error(), "401") && tokenExpiredAttempts == 0 {
					enterExpiredMode()
				}
			}

		case newAuth := <-w.resumeCh:
			// Frontend provided a new bearer token after user re-login — reset and reconnect.
			w.mu.Lock()
			w.auth = newAuth
			w.mu.Unlock()
			tokenExpiredAttempts = 0
			tokenExpiredTimer = nil
			log.Printf("[ws] Auth resumed for user %s — reconnecting immediately", newAuth.UserId)
			token, err := w.GetWebSocketToken(context.Background())
			if err != nil {
				log.Printf("[ws] Token fetch after resume failed for user %s: %v", newAuth.UserId, err)
				enterExpiredMode()
				continue
			}
			newStopCh, dialErr := dialAndUpdate(token)
			if dialErr != nil {
				log.Printf("[ws] Reconnect after resume failed for user %s: %v", newAuth.UserId, dialErr)
				continue
			}
			currentStopCh = newStopCh
			log.Printf("[ws] Reconnected after auth resume for user %s", newAuth.UserId)
			if w.OnReconnected != nil {
				go w.OnReconnected()
			}

		case <-tokenExpiredTimer:
			// Token-expiry retry timer fired.
			tokenExpiredTimer = nil
			log.Printf("[ws] Retrying token fetch for user %s (attempt %d/%d)...",
				w.auth.UserId, tokenExpiredAttempts, len(tokenExpiredDelays))
			token, err := w.GetWebSocketToken(context.Background())
			if err != nil {
				log.Printf("[ws] Token-expiry retry failed for user %s: %v", w.auth.UserId, err)
				enterExpiredMode()
				continue
			}
			newStopCh, dialErr := dialAndUpdate(token)
			if dialErr != nil {
				log.Printf("[ws] Reconnect failed for user %s: %v", w.auth.UserId, dialErr)
				continue
			}
			currentStopCh = newStopCh
			tokenExpiredAttempts = 0
			log.Printf("[ws] Reconnected for user %s after token-expiry retry", w.auth.UserId)
			if w.OnReconnected != nil {
				go w.OnReconnected()
			}

		case <-normalReconnectCh:
			// Normal 5s reconnect poll — only active when tokenExpiredAttempts == 0.
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
			token, err := w.GetWebSocketToken(context.Background())
			if err != nil {
				if w.tryAuthRefresh(err) {
					token, err = w.GetWebSocketToken(context.Background())
				}
				if err != nil {
					log.Printf("[ws] Token fetch failed for user %s: %v", w.auth.UserId, err)
					if strings.Contains(err.Error(), "401") {
						enterExpiredMode()
					}
					continue
				}
			}

			newStopCh, dialErr := dialAndUpdate(token)
			if dialErr != nil {
				log.Printf("[ws] Reconnect failed for user %s: %v", w.auth.UserId, dialErr)
				continue
			}
			currentStopCh = newStopCh
			tokenExpiredAttempts = 0
			log.Printf("[ws] Reconnected for user %s", w.auth.UserId)
			if w.OnReconnected != nil {
				go w.OnReconnected()
			}
		}
	}
}
