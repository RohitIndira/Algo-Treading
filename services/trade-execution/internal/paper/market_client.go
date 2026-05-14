package paper

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/marketws"
	"github.com/gorilla/websocket"
)

// PriceCallback is called whenever a new LTP is received for a symbol.
type PriceCallback func(symbol string, ltp float64)

// Binary message type bytes (matches binaryDecoder.ts and the Go binary WS server)
const (
	binMsgMarketData      byte = 0x01
	binMsgWelcome         byte = 0x02
	binMsgSubscription    byte = 0x03
	binMsgPing            byte = 0x04
	binMsgPong            byte = 0x05
	binMsgError           byte = 0x06
	binMsgWeek52          byte = 0x07
	binMsgMarketStatus    byte = 0x08
	binMsgCachedData      byte = 0x09
	binMsgSubscriptionAck byte = 0x0a
)

// decodeBinaryTick decodes a binary MARKET_DATA frame from the enhanced-stream WebSocket.
//
// Frame layout (little-endian):
//
//	[0]      uint8   message type  (must be 0x01)
//	[1-8]    int64   timestamp
//	[9]      uint8   tokenLength
//	[10...]  string  token  (tokenLength bytes)
//	[next]   uint8   symbolLength
//	[next…]  string  symbol (symbolLength bytes)
//	[next]   uint8   exchange code  (0=nse, 1=bse, 2=nfo, 3=mcx)
//	[next]   float32 ltp
//	…
func decodeBinaryTick(data []byte) (symbol, token, exchange string, ltp float64, ok bool) {
	if len(data) < 10 || data[0] != binMsgMarketData {
		return "", "", "", 0, false
	}

	offset := 1 + 8 // skip type(1) + timestamp(8)

	// token (length-prefixed string)
	if offset >= len(data) {
		return "", "", "", 0, false
	}
	tokenLen := int(data[offset])
	offset++
	if offset+tokenLen > len(data) {
		return "", "", "", 0, false
	}
	token = string(data[offset : offset+tokenLen])
	offset += tokenLen

	// symbol (length-prefixed string)
	if offset >= len(data) {
		return "", "", "", 0, false
	}
	symbolLen := int(data[offset])
	offset++
	if offset+symbolLen > len(data) {
		return "", "", "", 0, false
	}
	symbol = string(data[offset : offset+symbolLen])
	offset += symbolLen

	// exchange (1 byte)
	if offset >= len(data) {
		return "", "", "", 0, false
	}
	exchByte := data[offset]
	offset++
	switch exchByte {
	case 0:
		exchange = "nse"
	case 1:
		exchange = "bse"
	case 2:
		exchange = "nfo"
	case 3:
		exchange = "mcx"
	default:
		exchange = "nse"
	}

	// ltp (float32 LE, 4 bytes)
	if offset+4 > len(data) {
		return "", "", "", 0, false
	}
	ltpBits := binary.LittleEndian.Uint32(data[offset : offset+4])
	ltp = float64(math.Float32frombits(ltpBits))

	return symbol, token, exchange, ltp, ltp > 0
}

// PaperMarketClient maintains a single persistent WebSocket connection to the
// Indira market data stream and delivers LTP updates to a callback.
type PaperMarketClient struct {
	wsURL    string
	clientID string
	callback PriceCallback

	mu         sync.Mutex
	conn       *websocket.Conn
	subscribed map[string]bool // token → subscribed

	// tickCh is an optional send-only channel for full tick history storage.
	// If nil (default), no tick data is forwarded — zero overhead.
	tickCh chan<- marketws.TickData

	stopCh chan struct{}
	once   sync.Once
}

// SetTickWriter wires a send-only channel for full tick history.
// Every decoded tick is forwarded non-blocking — the callback path is never delayed.
func (c *PaperMarketClient) SetTickWriter(ch chan<- marketws.TickData) {
	c.tickCh = ch
}

// NewPaperMarketClient creates the market data client.
// wsURL example: wss://stockkaskwebsocket.indiratrade.com/enhanced-stream
func NewPaperMarketClient(wsURL string, callback PriceCallback) *PaperMarketClient {
	clientID := fmt.Sprintf("PaperTrading_%d", time.Now().UnixMilli())
	return &PaperMarketClient{
		wsURL:      wsURL,
		clientID:   clientID,
		callback:   callback,
		subscribed: make(map[string]bool),
		stopCh:     make(chan struct{}),
	}
}

// Start connects to the market stream and begins processing messages.
// It auto-reconnects on disconnect. Call Stop() to shut down.
func (c *PaperMarketClient) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-c.stopCh:
				log.Println("[paper-market] Stopped")
				return
			case <-ctx.Done():
				return
			default:
				if err := c.connect(ctx); err != nil {
					log.Printf("[paper-market] Connection error: %v — retrying in 5s", err)
					select {
					case <-time.After(5 * time.Second):
					case <-c.stopCh:
						return
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
}

// Stop shuts down the market client.
func (c *PaperMarketClient) Stop() {
	c.once.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.mu.Unlock()
	})
}

// Subscribe registers tokens (numeric instrument token strings, e.g. "2475") for LTP updates.
// Tokens not yet connected are queued and sent after the next successful connection.
func (c *PaperMarketClient) Subscribe(tokens []string) {
	if len(tokens) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var newTokens []string
	for _, t := range tokens {
		if t != "" && !c.subscribed[t] {
			c.subscribed[t] = true
			newTokens = append(newTokens, t)
		}
	}
	if len(newTokens) == 0 {
		return
	}

	if c.conn == nil {
		log.Printf("[paper-market] Not connected yet; will subscribe tokens %v on reconnect", newTokens)
		return
	}

	c.sendSubscribe(newTokens)
}

// Unsubscribe removes tokens from active subscriptions.
func (c *PaperMarketClient) Unsubscribe(tokens []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tokens {
		delete(c.subscribed, t)
	}
}

// connect establishes the WebSocket connection and processes messages.
func (c *PaperMarketClient) connect(ctx context.Context) error {
	url := fmt.Sprintf("%s?client_id=%s", c.wsURL, c.clientID)
	log.Printf("[paper-market] Connecting to %s", url)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	// Re-subscribe all known tokens after reconnect
	allTokens := make([]string, 0, len(c.subscribed))
	for t := range c.subscribed {
		allTokens = append(allTokens, t)
	}
	c.mu.Unlock()

	log.Printf("[paper-market] Connected. Re-subscribing %d tokens", len(allTokens))
	if len(allTokens) > 0 {
		c.mu.Lock()
		c.sendSubscribe(allTokens)
		c.mu.Unlock()
	}

	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		conn.Close()
	}()

	// Send a keep-alive ping every 30 s (per socket.md §8)
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	pingMsg, _ := json.Marshal(map[string]string{"type": "request", "action": "ping"})

	// Read messages (also drain the ping ticker)
	msgCh := make(chan []byte, 16)
	errCh := make(chan error, 1)
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- msg
		}
	}()

	for {
		select {
		case <-c.stopCh:
			return nil
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return fmt.Errorf("read error: %w", err)
		case msg := <-msgCh:
			c.processMessage(msg)
		case <-pingTicker.C:
			c.mu.Lock()
			if c.conn != nil {
				_ = c.conn.WriteMessage(websocket.TextMessage, pingMsg)
			}
			c.mu.Unlock()
		}
	}
}

// sendSubscribe sends a JSON subscribe message (must hold c.mu).
func (c *PaperMarketClient) sendSubscribe(tokens []string) {
	msg := map[string]interface{}{
		"type":   "request",
		"action": "subscribe",
		"stocks": tokens,
	}
	data, _ := json.Marshal(msg)
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("[paper-market] Failed to send subscribe: %v", err)
		return
	}
	log.Printf("[paper-market] Subscribed to tokens %v", tokens)
}

// processMessage parses an incoming market data message and invokes the callback.
//
// The enhanced-stream server sends JSON by default (no ?format=binary in URL).
// Binary frames are also handled for when ?format=binary is used.
//
// JSON market_data structure (from socket.md §5.7):
//
//	{ "type":"market_data", "symbol":"RELIANCE", "token":"11536", "exchange":"NSE",
//	  "data": { "ltp": 2450.75, ... } }
//
// Note: ltp is inside the nested "data" object, NOT at the top level.
func (c *PaperMarketClient) processMessage(msg []byte) {
	if len(msg) == 0 {
		return
	}

	// ── Binary path ────────────────────────────────────────────────────────
	// Active only when the server sends binary frames (?format=binary).
	// JSON messages start with '{' (0x7B) which won't match any binary type byte.
	switch msg[0] {
	case binMsgMarketData:
		symbol, token, exchange, ltp, ok := decodeBinaryTick(msg)
		if ok {
			if c.callback != nil {
				c.callback(symbol, ltp)
			}
			if c.tickCh != nil {
				select {
				case c.tickCh <- marketws.TickData{Symbol: symbol, Token: token, Exchange: exchange, LTP: ltp, Timestamp: time.Now().UnixNano()}:
				default:
				}
			}
		}
		return

	case binMsgWelcome:
		log.Printf("[paper-market] Binary welcome received")
		return

	case binMsgPong, binMsgSubscriptionAck,
		binMsgWeek52, binMsgMarketStatus, binMsgCachedData, binMsgSubscription, binMsgError, binMsgPing:
		return // known binary control frames, not price data
	}

	// ── JSON path (default) ────────────────────────────────────────────────
	var envelope map[string]interface{}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return // unrecognised frame, skip
	}

	msgType, _ := envelope["type"].(string)

	switch msgType {
	case "market_data":
		// Symbol is at the top level.
		symbol := extractString(envelope, "symbol")
		token := extractString(envelope, "token")
		exchange := strings.ToLower(extractString(envelope, "exchange"))

		// ltp is inside the nested "data" object (see socket.md §5.7).
		// Do NOT fall back to "close" — that field carries historical/prev-day prices
		// which would incorrectly trigger SL/TP at stale price levels.
		var ltp float64
		if nested, ok := envelope["data"].(map[string]interface{}); ok {
			ltp = extractFloat(nested, "ltp", "last_traded_price")
		}

		if symbol != "" && ltp > 0 {
			if c.callback != nil {
				c.callback(symbol, ltp)
			}
			if c.tickCh != nil {
				select {
				case c.tickCh <- marketws.TickData{Symbol: symbol, Token: token, Exchange: exchange, LTP: ltp, Timestamp: time.Now().UnixNano()}:
				default:
				}
			}
		}

	case "periodic_52week_data":
		// 52-week high/low messages contain historical price data (e.g. yearly high/low,
		// previous close). These must NOT be used for live SL/TP checks — using them
		// would fire exits at totally wrong prices (e.g. 52-week high as "LTP").
		// Silently ignore.

	case "welcome":
		log.Printf("[paper-market] Connected (JSON mode)")

	case "subscription_response":
		if d, ok := envelope["data"].(map[string]interface{}); ok {
			log.Printf("[paper-market] Subscription: %v (failed: %v)",
				d["subscribed_stocks"], d["failed_stocks"])
		}

	case "pong", "market_status", "subscriptions", "market_opened", "market_closed",
		"cached_data":
		// control/info/snapshot messages — no price action needed

	case "error":
		log.Printf("[paper-market] Server error: %v", envelope["message"])

	default:
		// All unrecognised message types are intentionally ignored.
		// ONLY the explicit "market_data" case above may fire the SL/TP callback.
	}
}

func extractString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

func extractFloat(m map[string]interface{}, keys ...string) float64 {
	for _, k := range keys {
		switch v := m[k].(type) {
		case float64:
			return v
		case json.Number:
			f, _ := v.Float64()
			return f
		}
	}
	return 0
}
