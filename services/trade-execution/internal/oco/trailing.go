package oco

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

	"github.com/gorilla/websocket"
)

// ════════════════════════════════════════════════════════════════════════════
// Price Source Interface (WSS primary, Redis fallback)
// ════════════════════════════════════════════════════════════════════════════

// RedisLTPProvider fetches LTP from Redis. Used as fallback when WSS is down.
type RedisLTPProvider interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

// ════════════════════════════════════════════════════════════════════════════
// Enhanced-Stream WSS Client (Primary Price Source)
// ════════════════════════════════════════════════════════════════════════════

// Binary message type bytes (from GetLivePrice.md)
const (
	wsMsgMarketData      byte = 0x01
	wsMsgWelcome         byte = 0x02
	wsMsgSubscription    byte = 0x03
	wsMsgPing            byte = 0x04
	wsMsgPong            byte = 0x05
	wsMsgError           byte = 0x06
	wsMsgMarketStatus    byte = 0x08
	wsMsgSubscriptionAck byte = 0x0a
)

// OCOMarketClient connects to the enhanced-stream binary WebSocket and delivers
// real-time LTP updates to the trailing SL monitor. It also maintains an in-memory
// LTP cache that serves as the "live view" of prices.
//
// Architecture:
//
//	enhanced-stream WSS ──binary tick──→ OCOMarketClient.onTick()
//	                                        │
//	                                        ├──→ update ltpCache (symbol → LTP)
//	                                        └──→ call priceCallback(symbol, ltp)
//	                                                   │
//	                                                   ▼
//	                                        TrailingMonitor.OnPriceUpdate()
type OCOMarketClient struct {
	wsURL    string
	clientID string

	// priceCallback is called for every LTP update received from WSS.
	priceCallback func(symbol string, ltp float64)

	mu         sync.Mutex
	conn       *websocket.Conn
	subscribed map[string]bool // token (e.g. "11536") → subscribed

	// ltpCache: symbol (e.g. "RELIANCE") → latest LTP
	// Updated on every WSS tick; read by the trailing monitor as primary price source.
	ltpCacheMu sync.RWMutex
	ltpCache   map[string]float64

	// healthy is true when WSS is connected and receiving data.
	// When false, the trailing monitor falls back to Redis.
	healthy bool

	stopCh chan struct{}
	once   sync.Once
}

// NewOCOMarketClient creates a market data WSS client for OCO price monitoring.
//
//	wsURL: e.g. "wss://stockkaskwebsocket.indiratrade.com/enhanced-stream"
//	callback: called on every LTP update (symbol, ltp)
func NewOCOMarketClient(wsURL string, callback func(symbol string, ltp float64)) *OCOMarketClient {
	clientID := fmt.Sprintf("OCO_%d", time.Now().UnixMilli())
	return &OCOMarketClient{
		wsURL:         wsURL,
		clientID:      clientID,
		priceCallback: callback,
		subscribed:    make(map[string]bool),
		ltpCache:      make(map[string]float64),
		stopCh:        make(chan struct{}),
	}
}

// Start connects to the enhanced-stream WSS and auto-reconnects on failure.
func (c *OCOMarketClient) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-c.stopCh:
				log.Println("[oco-market] Stopped")
				return
			case <-ctx.Done():
				return
			default:
				if err := c.connect(ctx); err != nil {
					c.healthy = false
					log.Printf("[oco-market] Connection error: %v — retrying in 3s", err)
					select {
					case <-time.After(3 * time.Second):
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

// Stop shuts down the WSS client.
func (c *OCOMarketClient) Stop() {
	c.once.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.mu.Unlock()
	})
}

// IsHealthy returns true if the WSS connection is active and receiving data.
func (c *OCOMarketClient) IsHealthy() bool {
	return c.healthy
}

// GetLTP returns the cached LTP for a symbol from the WSS feed.
// Returns 0 if the symbol is not in cache or WSS is not healthy.
func (c *OCOMarketClient) GetLTP(symbol string) (float64, bool) {
	c.ltpCacheMu.RLock()
	defer c.ltpCacheMu.RUnlock()
	ltp, ok := c.ltpCache[symbol]
	return ltp, ok && ltp > 0
}

// Subscribe registers tokens for LTP updates on the WSS connection.
// tokens are numeric instrument tokens (e.g. "11536" for RELIANCE).
func (c *OCOMarketClient) Subscribe(tokens []string) {
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
		log.Printf("[oco-market] Not connected; will subscribe %v on reconnect", newTokens)
		return
	}
	c.sendSubscribe(newTokens)
}

// Unsubscribe removes tokens from subscriptions.
func (c *OCOMarketClient) Unsubscribe(tokens []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tokens {
		delete(c.subscribed, t)
	}
}

// connect establishes the WSS connection and processes messages.
func (c *OCOMarketClient) connect(ctx context.Context) error {
	url := fmt.Sprintf("%s?client_id=%s&format=binary", c.wsURL, c.clientID)
	log.Printf("[oco-market] Connecting to %s", url)

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

	c.healthy = true
	log.Printf("[oco-market] Connected (binary mode). Re-subscribing %d tokens", len(allTokens))

	if len(allTokens) > 0 {
		c.mu.Lock()
		c.sendSubscribe(allTokens)
		c.mu.Unlock()
	}

	defer func() {
		c.healthy = false
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		conn.Close()
	}()

	// Keep-alive ping every 30s (per socket.md: client→server messages are JSON)
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()
	pingMsg, _ := json.Marshal(map[string]string{"type": "request", "action": "ping"})

	msgCh := make(chan []byte, 64)
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

// sendSubscribe sends a JSON subscribe message. Must hold c.mu.
func (c *OCOMarketClient) sendSubscribe(tokens []string) {
	msg := map[string]interface{}{
		"type":   "request",
		"action": "subscribe",
		"stocks": tokens,
	}
	data, _ := json.Marshal(msg)
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("[oco-market] Subscribe failed: %v", err)
		return
	}
	log.Printf("[oco-market] Subscribed to %d tokens", len(tokens))
}

// processMessage decodes an incoming binary/JSON message from enhanced-stream.
func (c *OCOMarketClient) processMessage(msg []byte) {
	if len(msg) == 0 {
		return
	}

	// ── Binary path ──────────────────────────────────────────────────────
	switch msg[0] {
	case wsMsgMarketData:
		symbol, ltp, ok := decodeBinaryTick(msg)
		if !ok {
			return
		}
		// Update LTP cache
		c.ltpCacheMu.Lock()
		c.ltpCache[symbol] = ltp
		c.ltpCacheMu.Unlock()

		// Invoke callback
		if c.priceCallback != nil {
			c.priceCallback(symbol, ltp)
		}
		return

	case wsMsgWelcome:
		log.Println("[oco-market] Welcome received (binary)")
		return

	case wsMsgPong, wsMsgSubscriptionAck, wsMsgSubscription,
		wsMsgError, wsMsgPing, wsMsgMarketStatus:
		return // control frames
	}

	// ── JSON fallback ────────────────────────────────────────────────────
	var envelope map[string]interface{}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return
	}

	msgType, _ := envelope["type"].(string)
	switch msgType {
	case "market_data":
		symbol := extractString(envelope, "symbol")
		var ltp float64
		if nested, ok := envelope["data"].(map[string]interface{}); ok {
			ltp = extractFloat(nested, "ltp", "last_traded_price")
		}
		if symbol != "" && ltp > 0 {
			c.ltpCacheMu.Lock()
			c.ltpCache[symbol] = ltp
			c.ltpCacheMu.Unlock()
			if c.priceCallback != nil {
				c.priceCallback(symbol, ltp)
			}
		}

	case "welcome":
		log.Println("[oco-market] Connected (JSON mode)")

	case "subscription_response":
		if d, ok := envelope["data"].(map[string]interface{}); ok {
			log.Printf("[oco-market] Subscription: %v", d["subscribed_stocks"])
			if failed, ok := d["failed_stocks"]; ok && failed != nil {
				log.Printf("[oco-market] WARNING: failed subscriptions: %v", failed)
			}
		}

	case "error":
		log.Printf("[oco-market] Server error: %v", envelope["message"])
	}
}

// decodeBinaryTick decodes a binary MARKET_DATA frame.
// Layout per GetLivePrice.md: type(1) + timestamp(8) + tokenLen(1) + token + symLen(1) + symbol + exchange(1) + ltp(float32 LE)
func decodeBinaryTick(data []byte) (symbol string, ltp float64, ok bool) {
	if len(data) < 10 || data[0] != wsMsgMarketData {
		return "", 0, false
	}
	offset := 1 + 8 // skip type + timestamp

	// token (length-prefixed)
	if offset >= len(data) {
		return "", 0, false
	}
	tokenLen := int(data[offset])
	offset++
	offset += tokenLen

	// symbol (length-prefixed)
	if offset >= len(data) {
		return "", 0, false
	}
	symbolLen := int(data[offset])
	offset++
	if offset+symbolLen > len(data) {
		return "", 0, false
	}
	symbol = string(data[offset : offset+symbolLen])
	offset += symbolLen

	// exchange (1 byte) — skip
	offset++

	// ltp (float32 LE)
	if offset+4 > len(data) {
		return "", 0, false
	}
	ltpBits := binary.LittleEndian.Uint32(data[offset : offset+4])
	ltp = float64(math.Float32frombits(ltpBits))

	return symbol, ltp, ltp > 0
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

// ════════════════════════════════════════════════════════════════════════════
// Trailing SL Monitor
// ════════════════════════════════════════════════════════════════════════════

// TrailingMonitor adjusts SL orders for active OCO groups based on live price.
//
// Price source priority:
//  1. Enhanced-stream WSS (real-time, ~0ms latency) — PRIMARY
//  2. Redis LTP cache (polled by data-ingestion service) — FALLBACK
//
// Architecture:
//
//	                 ┌────────────────────────────┐
//	 WSS tick ──→    │   TrailingMonitor           │
//	                 │                              │
//	  (primary)      │  OnPriceUpdate(sym, ltp)     │──→ OCOManager.ModifySLLeg()
//	                 │  - check all groups for sym   │    (broker ModifyOrder API)
//	                 │  - if newHigh → trail SL up   │
//	                 │                              │
//	  (fallback)     │  pollRedis() every 500ms     │
//	                 │  - only when WSS unhealthy    │
//	                 └────────────────────────────┘
type TrailingMonitor struct {
	ocoManager    *OCOManager
	marketClient  *OCOMarketClient
	redisProvider RedisLTPProvider // fallback
	pollInterval  time.Duration

	// symbolGroups: symbol → []*OCOGroup (groups watching this symbol)
	// Updated when groups become ACTIVE or COMPLETED.
	symbolGroupsMu sync.RWMutex
	symbolGroups   map[string][]*OCOGroup

	stopCh chan struct{}
}

// NewTrailingMonitor creates a new trailing SL monitor.
//
//	ocoManager: the OCO manager (for ModifySLLeg calls)
//	marketClient: enhanced-stream WSS client (primary price source)
//	redisProvider: Redis LTP provider (fallback, may be nil)
//	pollInterval: how often to poll Redis when WSS is down (default 500ms)
func NewTrailingMonitor(
	ocoManager *OCOManager,
	marketClient *OCOMarketClient,
	redisProvider RedisLTPProvider,
	pollInterval time.Duration,
) *TrailingMonitor {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	return &TrailingMonitor{
		ocoManager:    ocoManager,
		marketClient:  marketClient,
		redisProvider: redisProvider,
		pollInterval:  pollInterval,
		symbolGroups:  make(map[string][]*OCOGroup),
		stopCh:        make(chan struct{}),
	}
}

// Start begins the trailing monitor. It:
//  1. Subscribes to WSS for all active OCO symbols
//  2. Reacts to WSS price ticks (via OnPriceUpdate callback)
//  3. Falls back to Redis polling when WSS is unhealthy
func (t *TrailingMonitor) Start(ctx context.Context) {
	log.Println("[oco-trailing] Starting trailing SL monitor")

	// Initial load of active groups
	t.refreshGroups()

	// Periodic refresh of group list + Redis fallback polling
	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()

	// Refresh group subscriptions less frequently
	refreshTicker := time.NewTicker(5 * time.Second)
	defer refreshTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[oco-trailing] Stopped (context)")
			return
		case <-t.stopCh:
			log.Println("[oco-trailing] Stopped (signal)")
			return
		case <-refreshTicker.C:
			t.refreshGroups()
		case <-ticker.C:
			// Redis fallback: poll for symbols missing WSS ticks.
			// WSS may report healthy overall while specific symbols
			// receive no ticks (subscription silently dropped).
			t.pollRedisForMissingSymbols(ctx)
		}
	}
}

// Stop stops the trailing monitor.
func (t *TrailingMonitor) Stop() {
	close(t.stopCh)
}

// OnPriceUpdate is the callback invoked by OCOMarketClient on every WSS tick.
// This is the PRIMARY price source — sub-millisecond reaction.
func (t *TrailingMonitor) OnPriceUpdate(symbol string, ltp float64) {
	t.symbolGroupsMu.RLock()
	groups, ok := t.symbolGroups[symbol]
	t.symbolGroupsMu.RUnlock()

	if !ok || len(groups) == 0 {
		return
	}

	for _, group := range groups {
		if group.State != StateActive || !group.TrailingSL {
			continue
		}
		// Log trailing evaluation for diagnostics
		if ltp > group.HighestPrice {
			log.Printf("[oco-trailing] New high for %s: ltp=%.2f highest=%.2f sl_trigger=%.2f group=%s",
				symbol, ltp, group.HighestPrice, group.SLTriggerPrice, group.GroupID)
		}
		t.evaluateTrailing(group, ltp)
	}
}

// evaluateTrailing checks if the trailing SL should be adjusted for a group.
func (t *TrailingMonitor) evaluateTrailing(group *OCOGroup, ltp float64) {
	// Acquire group mutex for thread-safe HighestPrice read/write.
	// CalculateTrailingSL reads HighestPrice/SLTriggerPrice which can be
	// modified concurrently by handleEntryUpdate or other WS handlers.
	mu := t.ocoManager.GetGroupMu(group.GroupID)
	mu.Lock()

	newTrigger, newLimit, shouldUpdate := group.CalculateTrailingSL(ltp)
	if !shouldUpdate {
		mu.Unlock()
		return
	}

	// DO NOT update HighestPrice here — it must only be advanced after the
	// broker modify succeeds. ModifySLLeg updates both SLTriggerPrice and
	// HighestPrice atomically under the group mutex on success.
	// If we updated HighestPrice here and ModifySLLeg failed, the mismatch
	// between advanced HighestPrice and stale SLTriggerPrice would permanently
	// stall the trailing SL (changePct would never reach the threshold again).

	mu.Unlock()

	// Modify the SL leg at broker (ModifySLLeg acquires its own lock)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := t.ocoManager.ModifySLLeg(ctx, group, newTrigger, newLimit, ltp); err != nil {
		log.Printf("[oco-trailing] Failed to modify SL for group %s: %v", group.GroupID, err)
	}
}

// refreshGroups syncs the symbol→groups mapping with the OCO manager's active groups.
// Also subscribes new symbols to the WSS market client.
func (t *TrailingMonitor) refreshGroups() {
	activeGroups := t.ocoManager.GetActiveGroups()

	newMap := make(map[string][]*OCOGroup)
	var newTokens []string

	for _, g := range activeGroups {
		if !g.TrailingSL {
			continue
		}
		newMap[g.Symbol] = append(newMap[g.Symbol], g)

		// Check if we need to subscribe this token
		token := fmt.Sprintf("%d", g.StockCode)
		if t.marketClient != nil {
			t.marketClient.Subscribe([]string{token})
		}
		newTokens = append(newTokens, token)
	}

	t.symbolGroupsMu.Lock()
	t.symbolGroups = newMap
	t.symbolGroupsMu.Unlock()

	if len(newTokens) > 0 {
		// Log WSS LTP cache status for each watched symbol to diagnose tick delivery
		var ltpInfo []string
		for sym, groups := range newMap {
			if t.marketClient != nil {
				ltp, ok := t.marketClient.GetLTP(sym)
				if ok {
					ltpInfo = append(ltpInfo, fmt.Sprintf("%s=%.2f(highest=%.2f)", sym, ltp, groups[0].HighestPrice))
				} else {
					ltpInfo = append(ltpInfo, fmt.Sprintf("%s=NO_TICK(highest=%.2f)", sym, groups[0].HighestPrice))
				}
			}
		}
		log.Printf("[oco-trailing] Watching %d symbols for trailing SL across %d groups | WSS cache: %v | WSS healthy: %v",
			len(newMap), len(activeGroups), strings.Join(ltpInfo, ", "), t.marketClient != nil && t.marketClient.IsHealthy())
	}
}

// pollRedisForMissingSymbols fetches LTPs from Redis for symbols that have
// no WSS tick in the LTP cache. This handles the case where WSS is globally
// healthy (some symbols receive ticks) but specific subscriptions silently
// fail to deliver data.
func (t *TrailingMonitor) pollRedisForMissingSymbols(ctx context.Context) {
	if t.redisProvider == nil {
		return
	}

	t.symbolGroupsMu.RLock()
	groups := make(map[string][]*OCOGroup, len(t.symbolGroups))
	for k, v := range t.symbolGroups {
		groups[k] = v
	}
	t.symbolGroupsMu.RUnlock()

	if len(groups) == 0 {
		return
	}

	wssHealthy := t.marketClient != nil && t.marketClient.IsHealthy()

	for _, groupList := range groups {
		for _, group := range groupList {
			if group.State != StateActive || !group.TrailingSL {
				continue
			}

			// If WSS is healthy, only poll Redis for symbols missing from WSS cache.
			// If WSS is down, poll Redis for everything.
			if wssHealthy && t.marketClient != nil {
				if _, hasWSSTick := t.marketClient.GetLTP(group.Symbol); hasWSSTick {
					continue // WSS is delivering ticks for this symbol — skip Redis
				}
			}

			ltp, err := t.redisProvider.GetLTP(ctx, strings.ToLower(group.Exchange), group.StockCode)
			if err != nil {
				continue // Redis miss — skip this tick
			}

			t.evaluateTrailing(group, ltp)
		}
	}
}
