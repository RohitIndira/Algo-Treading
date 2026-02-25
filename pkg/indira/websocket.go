package indira

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
)

// WSClient manages a WebSocket connection for a single user
type WSClient struct {
	client     *Client
	auth       *AuthContext
	conn       *websocket.Conn
	mu         sync.Mutex // Protects the connection
	
	Updates    chan *WSOrderStatus
	stopCh     chan struct{}
	Wg         sync.WaitGroup
	IsActive   bool
	OrderToken string
}

// NewWSClient creates a new WebSocket client for a specific user
func NewWSClient(httpClient *Client, auth *AuthContext) *WSClient {
	return &WSClient{
		client:  httpClient,
		auth:    auth,
		Updates: make(chan *WSOrderStatus, 100), // Buffered channel for live updates
		stopCh:  make(chan struct{}),
	}
}

// GetWebSocketToken hits the REST API to exchange the user's session token for a WebSocket Order Token
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

// Connect starts the WebSocket connection and background loops
func (w *WSClient) Connect(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.IsActive {
		return nil // Already connected
	}

	// 1. Get Token
	token, err := w.w.GetWebSocketToken(ctx)
	if err != nil {
		return err
	}
	w.OrderToken = token

	// 2. Dial WebSocket
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("websocket dial failed: %w", err)
	}
	w.conn = conn

	// 3. Send Connection Auth String (Plain Text JSON)
	authReq := WSConnectionRequest{
		UserId:     w.auth.UserId,
		OrderToken: w.OrderToken,
	}
	authBytes, _ := json.Marshal(authReq)
	
	// Write auth message
	err = w.conn.WriteMessage(websocket.TextMessage, authBytes)
	if err != nil {
		w.conn.Close()
		return fmt.Errorf("failed to send ws auth message: %w", err)
	}

	w.IsActive = true
	w.stopCh = make(chan struct{})

	// 4. Start Pumps
	w.Wg.Add(2)
	go w.readPump()
	go w.writePump()

	// 5. Auto-Reconnect Monitor
	w.Wg.Add(1)
	go w.monitorReconnect()

	log.Printf("WS Client Connected for user: %s", w.auth.UserId)
	return nil
}

// Close gracefully stops the WebSocket connection
func (w *WSClient) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.IsActive {
		return
	}
	w.IsActive = false
	close(w.stopCh)
	if w.conn != nil {
		w.conn.Close()
	}
	w.Wg.Wait() // Wait for pumps to stop
	// close(w.Updates) // Leave it up to caller to manage destruction if needed
}

func (w *WSClient) readPump() {
	defer func() {
		w.Wg.Done()
		w.conn.Close()
		w.IsActive = false
	}()

	w.conn.SetReadLimit(maxMessageSize)
	// PDF doesn't mention pong, it's text heartbeat, but we set a read deadline anyway
	w.conn.SetReadDeadline(time.Now().Add(pongWait))

	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WS read error user %s: %v", w.auth.UserId, err)
			}
			break
		}
		
		w.conn.SetReadDeadline(time.Now().Add(pongWait))

		// Try parsing order status
		var orderStatus WSOrderStatus
		if err := json.Unmarshal(message, &orderStatus); err == nil && orderStatus.OrderSequenceNumber != 0 {
			// Successfully parsed an order update. Push to channel non-blocking
			select {
			case w.Updates <- &orderStatus:
			default:
				log.Printf("WS Updates channel full for user %s, dropping message", w.auth.UserId)
			}
		} else {
			// Could be the { "status":"Ok" } connection response
			log.Printf("WS info msg user %s: %s", w.auth.UserId, string(message))
		}
	}
}

func (w *WSClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		w.conn.Close()
		w.Wg.Done()
	}()

	for {
		select {
		case <-w.stopCh:
			// Send close message gracefully
			w.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case <-ticker.C:
			// Send heartbeat
			w.conn.SetWriteDeadline(time.Now().Add(writeWait))
			hb := WSHeartbeat{
				UserId:    w.auth.UserId,
				Heartbeat: "h",
			}
			hbBytes, _ := json.Marshal(hb)
			if err := w.conn.WriteMessage(websocket.TextMessage, hbBytes); err != nil {
				return
			}
		}
	}
}

// monitorReconnect automatically reconnects if the read/write pumps die while the client should be active
func (w *WSClient) monitorReconnect() {
	defer w.Wg.Done()

	for {
		select {
		case <-w.stopCh:
			return // Expected shutdown
		case <-time.After(5 * time.Second): // Poll state every 5s
			w.mu.Lock()
			active := w.IsActive
			w.mu.Unlock()

			if !active {
				log.Printf("WS Auto-reconnecting for user %s...", w.auth.UserId)
				err := w.Connect(context.Background())
				if err != nil {
					log.Printf("WS Reconnect failed for user %s: %v", w.auth.UserId, err)
				}
				// If it successfully Connects, Connect() spawns new pumps and returns, but this monitor keeps running
			}
		}
	}
}
