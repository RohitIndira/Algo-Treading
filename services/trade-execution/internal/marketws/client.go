package marketws

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// marketwsRecover recovers a panic in a detached market-data goroutine and logs
// it with a stack trace. The market-data feed shares the process with order
// management, so an unrecovered panic here would crash live trading. Logs and
// returns only — it never changes feed/parse logic.
func marketwsRecover(where string) {
	if r := recover(); r != nil {
		log.Printf("[market-ws] PANIC RECOVERED in %s: %v\n%s", where, r, debug.Stack())
	}
}

// Binary message type bytes (matches enhanced-stream binary encoding)
const (
	binMsgMarketData byte = 0x01
	binMsgWelcome    byte = 0x02
)

// TickData is a single market tick captured from the WebSocket stream.
// It is sent to the tick store (localhost Redis) for full tick history.
type TickData struct {
	Symbol    string  `json:"symbol"`
	Token     string  `json:"token"`
	Exchange  string  `json:"exchange"`
	LTP       float64 `json:"ltp"`
	Timestamp int64   `json:"ts"` // Unix nanoseconds
}

// Client maintains a persistent WebSocket connection to the enhanced-stream
// market data server and keeps an in-memory LTP cache keyed by "exchange:token"
// (e.g. "nse:2475"). It supports dynamic subscribe/unsubscribe and auto-reconnects.
//
// Designed to be the primary price source for PriceMonitor with Redis as fallback.
type Client struct {
	wsURL    string
	clientID string

	mu         sync.Mutex
	conn       *websocket.Conn
	subscribed map[string]bool // token string → subscribed

	// ltpCache maps "exchange:token" → LTP (updated on every tick).
	// Reads use RLock; writes use Lock.
	cacheMu  sync.RWMutex
	ltpCache map[string]float64 // e.g. "nse:2475" → 2450.75

	// symbolToKey maps "SYMBOL" → "exchange:token" for cache lookups.
	// Populated from market_data messages which carry symbol, token, and exchange.
	symbolKeyMu sync.RWMutex
	symbolToKey map[string]string // e.g. "RELIANCE" → "nse:11536"

	// refCount tracks how many orders are watching each token.
	// When refCount reaches 0, the token is unsubscribed.
	refMu    sync.Mutex
	refCount map[string]int // token → reference count

	// onPriceUpdate is called on every market_data tick with (key, ltp).
	// key format: "exchange:token" (e.g. "nse:2475").
	// Used by PriceMonitor for event-driven evaluation.
	onPriceUpdate func(key string, ltp float64)

	// tickCh is an optional send-only channel for full tick history storage.
	// If nil (default), no tick data is forwarded — zero overhead.
	tickCh chan<- TickData

	healthy   bool
	healthyMu sync.RWMutex

	stopCh chan struct{}
	once   sync.Once
}

// New creates a new enhanced-stream WebSocket client.
// wsURL example: wss://stockkaskwebsocket.indiratrade.com/enhanced-stream
func New(wsURL string) *Client {
	clientID := fmt.Sprintf("PriceMonitor_%d", time.Now().UnixMilli())
	return &Client{
		wsURL:       wsURL,
		clientID:    clientID,
		subscribed:  make(map[string]bool),
		ltpCache:    make(map[string]float64),
		symbolToKey: make(map[string]string),
		refCount:    make(map[string]int),
		stopCh:      make(chan struct{}),
	}
}

// SetOnPriceUpdate sets a callback invoked on every market_data tick.
// key format: "exchange:token" (e.g. "nse:2475"), ltp is the latest traded price.
func (c *Client) SetOnPriceUpdate(fn func(key string, ltp float64)) {
	c.onPriceUpdate = fn
}

// SetTickWriter wires a send-only channel for full tick history.
// Every decoded tick is forwarded non-blocking — the algo path is never delayed.
func (c *Client) SetTickWriter(ch chan<- TickData) {
	c.tickCh = ch
}

// Start connects and begins consuming market data. Auto-reconnects on failure.
func (c *Client) Start(ctx context.Context) {
	go func() {
		defer marketwsRecover("Client.Start")
		const maxRetries = 5
		failures := 0
		for {
			select {
			case <-c.stopCh:
				return
			case <-ctx.Done():
				return
			default:
				start := time.Now()
				err := c.connectAndConsume(ctx)
				if err == nil {
					return // clean shutdown
				}
				c.setHealthy(false)
				// A connection that stayed up for a while then dropped is a
				// fresh incident, not a flapping failure — reset the budget.
				if time.Since(start) > time.Minute {
					failures = 0
				}
				failures++
				if failures >= maxRetries {
					log.Printf("[marketws] Giving up after %d consecutive connection failures: %v", failures, err)
					return
				}
				log.Printf("[marketws] Connection error (attempt %d/%d): %v — retrying in 3s", failures, maxRetries, err)
				select {
				case <-time.After(3 * time.Second):
				case <-c.stopCh:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}

// Stop shuts down the client.
func (c *Client) Stop() {
	c.once.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.mu.Unlock()
	})
}

// IsHealthy returns true if the WebSocket connection is active and receiving data.
func (c *Client) IsHealthy() bool {
	c.healthyMu.RLock()
	defer c.healthyMu.RUnlock()
	return c.healthy
}

func (c *Client) setHealthy(v bool) {
	c.healthyMu.Lock()
	c.healthy = v
	c.healthyMu.Unlock()
}

// GetLTP returns the cached LTP for a stock key ("exchange:token", e.g. "nse:2475").
// Returns 0, false if no price is cached.
func (c *Client) GetLTP(key string) (float64, bool) {
	c.cacheMu.RLock()
	ltp, ok := c.ltpCache[key]
	c.cacheMu.RUnlock()
	return ltp, ok && ltp > 0
}

// GetLTPs returns cached LTPs for multiple keys. Missing keys are omitted.
func (c *Client) GetLTPs(keys []string) map[string]float64 {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	result := make(map[string]float64, len(keys))
	for _, k := range keys {
		if ltp, ok := c.ltpCache[k]; ok && ltp > 0 {
			result[k] = ltp
		}
	}
	return result
}

// Subscribe adds tokens to the subscription list with reference counting.
// token is the numeric instrument token string (e.g. "2475").
// Multiple calls with the same token increment the ref count; the actual
// WebSocket subscribe is only sent on the first reference.
func (c *Client) Subscribe(tokens []string) {
	if len(tokens) == 0 {
		return
	}

	c.refMu.Lock()
	var newTokens []string
	for _, t := range tokens {
		if t == "" {
			continue
		}
		c.refCount[t]++
		if c.refCount[t] == 1 {
			newTokens = append(newTokens, t) // first reference → actually subscribe
		}
	}
	c.refMu.Unlock()

	if len(newTokens) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range newTokens {
		c.subscribed[t] = true
	}

	if c.conn == nil {
		return
	}

	c.sendSubscribe(newTokens)
}

// Unsubscribe decrements the ref count for tokens. When a token's ref count
// reaches 0, it is unsubscribed from the WebSocket.
func (c *Client) Unsubscribe(tokens []string) {
	if len(tokens) == 0 {
		return
	}

	c.refMu.Lock()
	var removeTokens []string
	for _, t := range tokens {
		if t == "" {
			continue
		}
		c.refCount[t]--
		if c.refCount[t] <= 0 {
			delete(c.refCount, t)
			removeTokens = append(removeTokens, t)
		}
	}
	c.refMu.Unlock()

	if len(removeTokens) == 0 {
		return
	}

	c.mu.Lock()
	for _, t := range removeTokens {
		delete(c.subscribed, t)
	}
	conn := c.conn
	c.mu.Unlock()

	if conn == nil || len(removeTokens) == 0 {
		return
	}

	msg := map[string]interface{}{
		"type":   "request",
		"action": "unsubscribe",
		"stocks": removeTokens,
	}
	data, _ := json.Marshal(msg)
	c.mu.Lock()
	if c.conn != nil {
		if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("[marketws] Failed to send unsubscribe: %v", err)
		} else {
			log.Printf("[marketws] Unsubscribed from tokens %v", removeTokens)
		}
	}
	c.mu.Unlock()
}

// connectAndConsume establishes the WebSocket connection and processes messages.
func (c *Client) connectAndConsume(ctx context.Context) error {
	url := fmt.Sprintf("%s?client_id=%s", c.wsURL, c.clientID)
	log.Printf("[marketws] Connecting to %s", url)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	allTokens := make([]string, 0, len(c.subscribed))
	for t := range c.subscribed {
		allTokens = append(allTokens, t)
	}
	c.mu.Unlock()

	c.setHealthy(true)

	if len(allTokens) > 0 {
		log.Printf("[marketws] Connected. Re-subscribing %d tokens", len(allTokens))
		c.mu.Lock()
		c.sendSubscribe(allTokens)
		c.mu.Unlock()
	} else {
		log.Printf("[marketws] Connected (no tokens to subscribe yet)")
	}

	defer func() {
		c.setHealthy(false)
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		conn.Close()
	}()

	// Keep-alive ping every 30s
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()
	pingMsg, _ := json.Marshal(map[string]string{"type": "request", "action": "ping"})

	msgCh := make(chan []byte, 64)
	errCh := make(chan error, 1)
	go func() {
		defer marketwsRecover("Client.readLoop")
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

// sendSubscribe sends a JSON subscribe message (caller must hold c.mu).
func (c *Client) sendSubscribe(tokens []string) {
	msg := map[string]interface{}{
		"type":   "request",
		"action": "subscribe",
		"stocks": tokens,
	}
	data, _ := json.Marshal(msg)
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("[marketws] Failed to send subscribe: %v", err)
		return
	}
	log.Printf("[marketws] Subscribed to %d tokens", len(tokens))
}

// processMessage handles incoming market data and updates the LTP cache.
func (c *Client) processMessage(msg []byte) {
	if len(msg) == 0 {
		return
	}

	// Binary path
	if msg[0] == binMsgMarketData {
		symbol, token, exchange, ltp, ok := decodeBinaryTick(msg)
		if ok {
			c.updateCache(symbol, token, exchange, ltp)
		}
		return
	}
	if msg[0] == binMsgWelcome {
		log.Printf("[marketws] Binary welcome received")
		return
	}
	// Skip other binary control frames
	if msg[0] < 0x20 {
		return
	}

	// JSON path
	var envelope map[string]interface{}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return
	}

	msgType, _ := envelope["type"].(string)
	if msgType == "periodic_52week_data" {
		return
	}
	switch msgType {
	case "market_data":
		symbol, _ := envelope["symbol"].(string)
		token, _ := envelope["token"].(string)
		exchange, _ := envelope["exchange"].(string)

		var ltp float64
		if nested, ok := envelope["data"].(map[string]interface{}); ok {
			if v, ok := nested["ltp"].(float64); ok {
				ltp = v
			}
		}

		if symbol != "" && ltp > 0 {
			c.updateCache(symbol, token, exchange, ltp)
		}

	case "welcome":
		log.Printf("[marketws] Connected (JSON mode)")
	case "subscription_response":
		if d, ok := envelope["data"].(map[string]interface{}); ok {
			log.Printf("[marketws] Subscription: subscribed=%v failed=%v total=%v",
				d["subscribed_stocks"], d["failed_stocks"], d["total_subscribed"])
		}
	case "error":
		log.Printf("[marketws] Server error: %v", envelope["message"])
	}
}

// updateCache updates the in-memory LTP cache for a stock and fires the callback.
func (c *Client) updateCache(symbol, token, exchange string, ltp float64) {
	key := fmt.Sprintf("%s:%s", strings.ToLower(exchange), token)

	c.cacheMu.Lock()
	c.ltpCache[key] = ltp
	c.cacheMu.Unlock()

	// Map symbol → key for reverse lookups
	if symbol != "" {
		c.symbolKeyMu.Lock()
		c.symbolToKey[strings.ToUpper(symbol)] = key
		c.symbolKeyMu.Unlock()
	}

	// Fire event-driven callback (PriceMonitor.OnPriceUpdate)
	if c.onPriceUpdate != nil {
		c.onPriceUpdate(key, ltp)
	}

	// Forward full tick to the tick store (non-blocking — never delays algo path).
	// tickCh is nil by default; this block costs only a nil pointer check when disabled.
	if c.tickCh != nil {
		select {
		case c.tickCh <- TickData{
			Symbol:    symbol,
			Token:     token,
			Exchange:  strings.ToLower(exchange),
			LTP:       ltp,
			Timestamp: time.Now().UnixNano(),
		}:
		default: // channel full — drop tick, never block
		}
	}
}

// decodeBinaryTick decodes a binary MARKET_DATA frame.
// Returns symbol, token, exchange, ltp.
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
