package historical

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	enhancedStreamURL = "wss://stockkaskwebsocket.indiratrade.com/enhanced-stream"
	subscribeBatchSize = 50  // Subscribe 50 stocks at a time — safe, no data loss
	batchDelay         = 300 * time.Millisecond // Delay between subscription batches
	pingInterval       = 30 * time.Second
	reconnectDelay     = 5 * time.Second
	maxReconnectDelay  = 60 * time.Second
)

// WSMonitor connects to the enhanced-stream WebSocket, subscribes to all NSE stocks,
// detects 52W breakouts in real-time, and stores them in DB + Redis.
type WSMonitor struct {
	repo   *Repository
	redis  *redis.Client
	logger *zap.Logger

	symbols []string // All NSE symbols to subscribe

	mu   sync.Mutex
	conn *websocket.Conn

	// Track which stocks already broke out today (avoid duplicate DB writes)
	breakoutToday sync.Map // key: "SYMBOL:52W_HIGH" or "SYMBOL:52W_LOW"

	// Stats
	statsMu    sync.Mutex
	tickCount  int64
	breakouts  int64
	lastTick   time.Time
	connected  bool
}

// NewWSMonitor creates a new WebSocket monitor.
func NewWSMonitor(repo *Repository, redisClient *redis.Client, logger *zap.Logger) *WSMonitor {
	return &WSMonitor{
		repo:   repo,
		redis:  redisClient,
		logger: logger,
	}
}

// Start connects to the WebSocket and begins monitoring. Blocks until ctx is cancelled.
// Auto-reconnects on failure. Resets breakout tracking at midnight.
func (m *WSMonitor) Start(ctx context.Context) error {
	// Load all NSE symbols from DB
	symbols, err := m.repo.GetAllNSESymbols(ctx)
	if err != nil {
		return fmt.Errorf("load NSE symbols: %w", err)
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no NSE symbols found in instruments table")
	}
	m.symbols = symbols
	m.logger.Info("Loaded NSE symbols for monitoring", zap.Int("count", len(symbols)))

	// Start midnight reset goroutine (clear daily breakout tracking)
	go m.midnightReset(ctx)

	// Connect with auto-reconnect
	reconnectWait := reconnectDelay
	for {
		select {
		case <-ctx.Done():
			m.logger.Info("WSMonitor stopped")
			return nil
		default:
		}

		err := m.connectAndMonitor(ctx)
		if ctx.Err() != nil {
			return nil
		}

		m.setConnected(false)
		m.logger.Warn("WebSocket disconnected, reconnecting",
			zap.Error(err),
			zap.Duration("wait", reconnectWait))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectWait):
		}

		// Exponential backoff capped at maxReconnectDelay
		reconnectWait = reconnectWait * 2
		if reconnectWait > maxReconnectDelay {
			reconnectWait = maxReconnectDelay
		}
	}
}

func (m *WSMonitor) connectAndMonitor(ctx context.Context) error {
	clientID := fmt.Sprintf("52W_Monitor_%d", time.Now().UnixMilli())
	url := fmt.Sprintf("%s?client_id=%s", enhancedStreamURL, clientID)

	m.logger.Info("Connecting to enhanced-stream", zap.String("url", url))

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	m.mu.Lock()
	m.conn = conn
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.conn = nil
		m.mu.Unlock()
		conn.Close()
	}()

	m.setConnected(true)
	m.logger.Info("Connected to enhanced-stream")

	// Subscribe in batches of 50
	totalSubscribed := 0
	for i := 0; i < len(m.symbols); i += subscribeBatchSize {
		end := i + subscribeBatchSize
		if end > len(m.symbols) {
			end = len(m.symbols)
		}
		batch := m.symbols[i:end]

		msg := map[string]interface{}{
			"type":   "request",
			"action": "subscribe",
			"stocks": batch,
		}
		data, _ := json.Marshal(msg)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return fmt.Errorf("subscribe batch %d-%d: %w", i, end, err)
		}
		totalSubscribed += len(batch)

		// Log progress every 500 stocks
		if totalSubscribed%500 == 0 || end >= len(m.symbols) {
			m.logger.Info("Subscription progress",
				zap.Int("subscribed", totalSubscribed),
				zap.Int("total", len(m.symbols)))
		}

		if end < len(m.symbols) {
			time.Sleep(batchDelay)
		}
	}

	m.logger.Info("All stocks subscribed",
		zap.Int("total", totalSubscribed))

	// Start ping goroutine
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		pingMsg, _ := json.Marshal(map[string]string{"type": "request", "action": "ping"})
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.mu.Lock()
				if m.conn != nil {
					m.conn.WriteMessage(websocket.TextMessage, pingMsg)
				}
				m.mu.Unlock()
			}
		}
	}()

	// Read messages
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		m.processMessage(ctx, msg)
	}
}

func (m *WSMonitor) processMessage(ctx context.Context, msg []byte) {
	if len(msg) == 0 {
		return
	}

	// Skip binary control frames
	if msg[0] < 0x20 {
		return
	}

	var envelope map[string]interface{}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return
	}

	msgType, _ := envelope["type"].(string)
	switch msgType {
	case "market_data":
		m.handleTick(ctx, envelope)
	case "subscription_response":
		if d, ok := envelope["data"].(map[string]interface{}); ok {
			failed, _ := d["failed_stocks"]
			total, _ := d["total_subscribed"]
			m.logger.Debug("Subscription response",
				zap.Any("failed", failed),
				zap.Any("total", total))
		}
	case "error":
		m.logger.Warn("WebSocket server error", zap.Any("message", envelope["message"]))
	}
}

func (m *WSMonitor) handleTick(ctx context.Context, envelope map[string]interface{}) {
	symbol, _ := envelope["symbol"].(string)
	token, _ := envelope["token"].(string)
	exchange, _ := envelope["exchange"].(string)

	data, ok := envelope["data"].(map[string]interface{})
	if !ok || symbol == "" {
		return
	}

	ltp := getFloatField(data, "ltp")
	if ltp <= 0 {
		return
	}

	m.statsMu.Lock()
	m.tickCount++
	m.lastTick = time.Now()
	m.statsMu.Unlock()

	ws52High := getFloatField(data, "week_52_high")
	ws52Low := getFloatField(data, "week_52_low")
	volume := getIntField(data, "volume")
	pctChange := getFloatField(data, "percent_change")
	isNew52WH, _ := data["is_new_week_52_high"].(bool)
	isNew52WL, _ := data["is_new_week_52_low"].(bool)

	// Determine 52W values to use:
	//   Priority 1: WebSocket values (broker's official data, most accurate)
	//   Priority 2: Bhavcopy Redis values (fallback for stocks where WS has no 52W data)
	redis52H, redis52L := m.getRedis52W(ctx, token)

	use52H := ws52High
	use52L := ws52Low
	if use52H <= 0 {
		use52H = redis52H // Fallback to bhavcopy
	}
	if use52L <= 0 {
		use52L = redis52L // Fallback to bhavcopy
	}

	// Detect 52W HIGH breakout (LTP crosses above 52W high)
	if use52H > 0 && (isNew52WH || ltp > use52H) {
		m.recordBreakout(ctx, BreakoutEvent{
			Token:         token,
			Symbol:        symbol,
			Exchange:      exchange,
			BreakoutType:  "52W_HIGH",
			BreakoutPrice: ltp,
			Prev52WH:      use52H,
			Prev52WL:      use52L,
			Volume:        volume,
			PctChange:     pctChange,
			Source:        "websocket",
		})
	}

	// Detect 52W LOW breakout (LTP falls below 52W low)
	if use52L > 0 && (isNew52WL || ltp < use52L) {
		m.recordBreakout(ctx, BreakoutEvent{
			Token:         token,
			Symbol:        symbol,
			Exchange:      exchange,
			BreakoutType:  "52W_LOW",
			BreakoutPrice: ltp,
			Prev52WH:      use52H,
			Prev52WL:      use52L,
			Volume:        volume,
			PctChange:     pctChange,
			Source:        "websocket",
		})
	}

	// Update Redis 52W values if WebSocket has fresher data
	if ws52High > 0 || ws52Low > 0 {
		m.updateRedis52W(ctx, token, symbol, exchange, ws52High, ws52Low, ltp)

		// Fix 2: If WebSocket 52W high is now HIGHER than a breakout we recorded,
		// that breakout was false — delete it from DB and Redis
		m.cleanupFalseBreakouts(ctx, symbol, ws52High, ws52Low, ltp)
	}
}

func (m *WSMonitor) recordBreakout(ctx context.Context, evt BreakoutEvent) {
	// Check if already recorded today (in-memory dedup — fast)
	dedupKey := fmt.Sprintf("%s:%s", evt.Symbol, evt.BreakoutType)
	if _, exists := m.breakoutToday.LoadOrStore(dedupKey, true); exists {
		return // Already recorded today
	}

	// Lookup instrument_id from DB
	instID, dbToken, err := m.repo.GetInstrumentBySymbol(ctx, evt.Symbol, evt.Exchange)
	if err != nil || instID == 0 {
		// Try without exchange (symbol might exist under different exchange)
		instID, dbToken, err = m.repo.GetInstrumentBySymbol(ctx, evt.Symbol, "NSE")
		if err != nil || instID == 0 {
			m.logger.Debug("Unknown instrument, skipping breakout",
				zap.String("symbol", evt.Symbol))
			return
		}
	}
	evt.InstrumentID = instID
	if evt.Token == "" {
		evt.Token = dbToken
	}

	// Store in DB
	isNew, err := m.repo.InsertBreakoutEvent(ctx, evt)
	if err != nil {
		m.logger.Error("Failed to store breakout event",
			zap.String("symbol", evt.Symbol),
			zap.Error(err))
		return
	}

	if isNew {
		m.statsMu.Lock()
		m.breakouts++
		m.statsMu.Unlock()

		m.logger.Info("BREAKOUT DETECTED",
			zap.String("type", evt.BreakoutType),
			zap.String("symbol", evt.Symbol),
			zap.String("exchange", evt.Exchange),
			zap.Float64("price", evt.BreakoutPrice),
			zap.Float64("prev_52w_high", evt.Prev52WH),
			zap.Float64("prev_52w_low", evt.Prev52WL),
			zap.Int64("volume", evt.Volume))

		// Store in Redis for live screener
		m.storeBreakoutInRedis(ctx, evt)
	}
}

// cleanupFalseBreakouts removes breakout events that are now invalid because
// the WebSocket provided a more accurate 52W value.
// Example: We detected CCL breakout at 1116.20 (bhavcopy said 52W high was 1116),
// but WebSocket later says 52W high is actually 1117 → 1116.20 < 1117 → false breakout.
func (m *WSMonitor) cleanupFalseBreakouts(ctx context.Context, symbol string, ws52High, ws52Low, ltp float64) {
	// Check 52W_HIGH: if WebSocket 52W high is now higher than today's breakout price,
	// the breakout was false
	if ws52High > 0 {
		dedupKey := symbol + ":52W_HIGH"
		if _, exists := m.breakoutToday.Load(dedupKey); exists {
			// We recorded a breakout today — check if it's still valid
			if ltp <= ws52High {
				// LTP is now below the real 52W high → false breakout
				deleted, err := m.repo.DeleteFalseBreakout(ctx, symbol, "52W_HIGH")
				if err != nil {
					m.logger.Error("Failed to delete false breakout", zap.Error(err))
				} else if deleted {
					m.breakoutToday.Delete(dedupKey) // Allow re-detection
					m.removeFalseBreakoutFromRedis(ctx, symbol, "52W_HIGH")
					m.logger.Warn("Removed false 52W_HIGH breakout",
						zap.String("symbol", symbol),
						zap.Float64("ws_52w_high", ws52High),
						zap.Float64("ltp", ltp))
				}
			}
		}
	}

	// Check 52W_LOW: if WebSocket 52W low is now lower than today's breakout price,
	// the breakout was false
	if ws52Low > 0 {
		dedupKey := symbol + ":52W_LOW"
		if _, exists := m.breakoutToday.Load(dedupKey); exists {
			if ltp >= ws52Low {
				deleted, err := m.repo.DeleteFalseBreakout(ctx, symbol, "52W_LOW")
				if err != nil {
					m.logger.Error("Failed to delete false breakout", zap.Error(err))
				} else if deleted {
					m.breakoutToday.Delete(dedupKey)
					m.removeFalseBreakoutFromRedis(ctx, symbol, "52W_LOW")
					m.logger.Warn("Removed false 52W_LOW breakout",
						zap.String("symbol", symbol),
						zap.Float64("ws_52w_low", ws52Low),
						zap.Float64("ltp", ltp))
				}
			}
		}
	}
}

// removeFalseBreakoutFromRedis removes a false breakout from the Redis sorted set.
func (m *WSMonitor) removeFalseBreakoutFromRedis(ctx context.Context, symbol, breakoutType string) {
	if m.redis == nil {
		return
	}

	key := fmt.Sprintf("breakouts:today:%s", breakoutType)
	// Scan the sorted set to find and remove the entry for this symbol
	members, err := m.redis.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		return
	}
	for _, member := range members {
		var data map[string]interface{}
		if json.Unmarshal([]byte(member), &data) == nil {
			if data["symbol"] == symbol {
				m.redis.ZRem(ctx, key, member)
				break
			}
		}
	}
}

// storeBreakoutInRedis adds the breakout to a Redis sorted set for today's live screener.
// Key: "breakouts:today:{type}" → sorted set by detection time
// Also publishes to a channel for real-time push to frontend.
func (m *WSMonitor) storeBreakoutInRedis(ctx context.Context, evt BreakoutEvent) {
	if m.redis == nil {
		return
	}

	val, _ := json.Marshal(map[string]interface{}{
		"symbol":    evt.Symbol,
		"token":     evt.Token,
		"exchange":  evt.Exchange,
		"price":     evt.BreakoutPrice,
		"prev_high": evt.Prev52WH,
		"prev_low":  evt.Prev52WL,
		"volume":    evt.Volume,
		"pct_change": evt.PctChange,
	})

	key := fmt.Sprintf("breakouts:today:%s", evt.BreakoutType)
	score := float64(time.Now().Unix())

	pipe := m.redis.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: string(val)})
	pipe.ExpireAt(ctx, key, midnightIST()) // Expire at midnight IST
	// Publish for real-time listeners (frontend WebSocket can subscribe)
	pipe.Publish(ctx, "breakout_events", string(val))
	pipe.Exec(ctx)
}

// getRedis52W fetches our computed 52W values from Redis (from bhavcopy pipeline).
func (m *WSMonitor) getRedis52W(ctx context.Context, token string) (high, low float64) {
	if m.redis == nil || token == "" {
		return 0, 0
	}

	val, err := m.redis.Get(ctx, fmt.Sprintf("52w:token:%s", token)).Result()
	if err != nil {
		return 0, 0
	}

	var data map[string]interface{}
	if json.Unmarshal([]byte(val), &data) != nil {
		return 0, 0
	}

	high, _ = data["high"].(float64)
	low, _ = data["low"].(float64)
	return high, low
}

// updateRedis52W updates Redis with fresh 52W values from WebSocket tick.
func (m *WSMonitor) updateRedis52W(ctx context.Context, token, symbol, exchange string, high, low, ltp float64) {
	if m.redis == nil || token == "" {
		return
	}

	key := fmt.Sprintf("52w:token:%s", token)
	val, _ := json.Marshal(map[string]interface{}{
		"high":       high,
		"low":        low,
		"last_close": ltp,
		"symbol":     symbol,
		"token":      token,
		"exchange":   exchange,
		"as_of":      time.Now().Format("2006-01-02"),
		"source":     "websocket",
	})
	m.redis.Set(ctx, key, val, 120*time.Hour)
}

// midnightReset clears the daily breakout tracking at midnight IST.
func (m *WSMonitor) midnightReset(ctx context.Context) {
	for {
		now := time.Now()
		loc, _ := time.LoadLocation("Asia/Kolkata")
		midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
		sleepDur := midnight.Sub(now.In(loc))

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepDur):
			m.breakoutToday = sync.Map{} // Reset for new day
			m.logger.Info("Midnight reset: cleared daily breakout tracking")
		}
	}
}

// GetStats returns current monitor statistics.
func (m *WSMonitor) GetStats() (ticks int64, breakouts int64, lastTick time.Time, connected bool) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	return m.tickCount, m.breakouts, m.lastTick, m.connected
}

func (m *WSMonitor) setConnected(v bool) {
	m.statsMu.Lock()
	m.connected = v
	m.statsMu.Unlock()
}

// --- Helpers ---

func midnightIST() time.Time {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(loc)
	return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
}

func getFloatField(m map[string]interface{}, key string) float64 {
	v, _ := m[key].(float64)
	return v
}

func getIntField(m map[string]interface{}, key string) int64 {
	v, _ := m[key].(float64)
	return int64(v)
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
