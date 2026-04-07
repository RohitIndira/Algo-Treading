package historical

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
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
// detects 52W breakouts in real-time, and stores them in DB + Redis + Kafka.
type WSMonitor struct {
	repo        *Repository
	redis       *redis.Client
	kafkaWriter *kafka.Writer
	logger      *zap.Logger

	symbols []string // All NSE symbols to subscribe

	mu   sync.Mutex
	conn *websocket.Conn

	// Track which stocks already broke out today (avoid duplicate DB writes)
	breakoutToday sync.Map // key: "SYMBOL:52W_HIGH" or "SYMBOL:52W_LOW"

	// O(1) split detection: prev_close per instrument (loaded from bhavcopy on startup)
	prevClose    map[string]float64 // key: symbol → prev_close from bhavcopy
	splitChecked sync.Map           // key: symbol → true (done checking)
	splitTicks   sync.Map           // key: symbol → tick count (check first 10 ticks)

	// Stats
	statsMu    sync.Mutex
	tickCount  int64
	breakouts  int64
	lastTick   time.Time
	connected  bool
}

// NewWSMonitor creates a new WebSocket monitor.
// kafkaBrokers can be empty — Kafka publishing will be skipped if not configured.
func NewWSMonitor(repo *Repository, redisClient *redis.Client, kafkaBrokers []string, logger *zap.Logger) *WSMonitor {
	m := &WSMonitor{
		repo:   repo,
		redis:  redisClient,
		logger: logger,
	}

	// Initialize Kafka writer if brokers are configured
	if len(kafkaBrokers) > 0 && kafkaBrokers[0] != "" {
		m.kafkaWriter = &kafka.Writer{
			Addr:         kafka.TCP(kafkaBrokers...),
			Topic:        "market.data.52w_breakouts",
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 100 * time.Millisecond, // Low latency for real-time events
			Async:        true,                     // Non-blocking writes
		}
		logger.Info("Kafka producer initialized for 52W breakout events",
			zap.Strings("brokers", kafkaBrokers),
			zap.String("topic", "market.data.52w_breakouts"))
	}

	return m
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

	// Load prev_close for all instruments (for O(1) split detection)
	m.prevClose, err = m.repo.GetAllPrevClose(ctx)
	if err != nil {
		m.logger.Warn("Failed to load prev_close for split detection", zap.Error(err))
		m.prevClose = make(map[string]float64)
	} else {
		m.logger.Info("Loaded prev_close for O(1) split detection",
			zap.Int("instruments", len(m.prevClose)))
	}

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

	// O(1) split detection on first tick per stock
	m.detectSplitO1(ctx, symbol, ltp)

	ws52High := getFloatField(data, "week_52_high")
	ws52Low := getFloatField(data, "week_52_low")
	volume := getIntField(data, "volume")
	pctChange := getFloatField(data, "percent_change")

	// Broker's flags — true only at the exact tick of breakout
	isNew52WH, _ := data["is_new_week_52_high"].(bool)
	isNew52WL, _ := data["is_new_week_52_low"].(bool)

	// Broker's dates and timestamps — when the 52W high/low was actually hit
	ws52HighDate, _ := data["week_52_high_date"].(string)         // "2026-04-06"
	ws52LowDate, _ := data["week_52_low_date"].(string)
	ws52HighTS, _ := data["week_52_high_timestamp"].(string)      // "2026-04-06T10:32:15+05:30"
	ws52LowTS, _ := data["week_52_low_timestamp"].(string)

	todayStr := time.Now().Format("2006-01-02")

	// Get previous 52W values from Redis (from 6AM API sync — exchange-verified)
	prev52H, prev52L, redisSource := m.getRedis52WWithSource(ctx, token, symbol)

	// Update Redis with broker's WebSocket values (always keep fresh)
	if ws52High > 0 || ws52Low > 0 {
		m.updateRedis52W(ctx, token, symbol, exchange, ws52High, ws52Low, ltp)
	}

	// --- 52W HIGH Breakout Detection ---
	// Three detection methods (from most to least reliable):
	//
	// Method 1: is_new_week_52_high = true
	//   → Broker's flag, true only at the exact tick of breakout. Most reliable.
	//
	// Method 2: week_52_high_date = TODAY (strict today only, not yesterday)
	//   → Backup catch for when we missed the flag tick.
	//
	// Method 3: LTP > Redis 52W high (from 6AM API sync)
	//   → Fallback for stocks where WebSocket doesn't send the flag at all.
	//   → ONLY safe when Redis source is "nse_api" or "bse_api" (exchange-verified).
	//   → NOT safe against old bhavcopy/websocket values (could be stale).

	highBreakout := false
	var breakoutAtHigh time.Time

	if isNew52WH {
		// Method 1: broker flag
		highBreakout = true
		breakoutAtHigh = parseTimestamp(ws52HighTS)
	} else if ws52HighDate == todayStr {
		// Method 2: date = today (strict)
		highBreakout = true
		breakoutAtHigh = parseTimestamp(ws52HighTS)
	} else if prev52H > 0 && ltp > prev52H &&
		(redisSource == "nse_api" || redisSource == "bse_api") {
		// Method 3: LTP exceeds exchange-verified 52W high from 6AM sync
		highBreakout = true
		breakoutAtHigh = time.Now()
	}

	if highBreakout {
		m.recordBreakout(ctx, BreakoutEvent{
			Token:         token,
			Symbol:        symbol,
			Exchange:      exchange,
			BreakoutType:  "52W_HIGH",
			BreakoutPrice: ltp,             // Use actual LTP (highest seen)
			Prev52WH:      prev52H,         // Previous 52W high from 6AM sync
			Prev52WL:      prev52L,
			Volume:        volume,
			PctChange:     pctChange,
			Source:        "websocket",
			BreakoutAt:    breakoutAtHigh,
		})
	}

	// --- 52W LOW Breakout Detection ---
	lowBreakout := false
	var breakoutAtLow time.Time

	if isNew52WL {
		lowBreakout = true
		breakoutAtLow = parseTimestamp(ws52LowTS)
	} else if ws52LowDate == todayStr {
		lowBreakout = true
		breakoutAtLow = parseTimestamp(ws52LowTS)
	} else if prev52L > 0 && ltp < prev52L && ltp > 0 &&
		(redisSource == "nse_api" || redisSource == "bse_api") {
		lowBreakout = true
		breakoutAtLow = time.Now()
	}

	if lowBreakout {
		m.recordBreakout(ctx, BreakoutEvent{
			Token:         token,
			Symbol:        symbol,
			Exchange:      exchange,
			BreakoutType:  "52W_LOW",
			BreakoutPrice: ltp,
			Prev52WH:      prev52H,
			Prev52WL:      prev52L,
			Volume:        volume,
			PctChange:     pctChange,
			Source:        "websocket",
			BreakoutAt:    breakoutAtLow,
		})
	}
}

// detectSplitO1 detects stock splits in O(1) time using price/prev_close ratio.
// Checks first 10 ticks per stock (not just first — first tick can be stale).
// If split detected, resets instrument history immediately.
//
// Common split ratios in Indian markets:
//   1:2 → ratio ~0.50    1:5  → ratio ~0.20    2:1 reverse → ratio ~2.0
//   1:3 → ratio ~0.33    1:10 → ratio ~0.10    3:1 reverse → ratio ~3.0
//   1:4 → ratio ~0.25
func (m *WSMonitor) detectSplitO1(ctx context.Context, symbol string, ltp float64) {
	// Already done checking for this stock
	if _, done := m.splitChecked.Load(symbol); done {
		return
	}

	// Increment tick count for this stock
	countVal, _ := m.splitTicks.LoadOrStore(symbol, new(int32))
	count := countVal.(*int32)
	tickNum := atomic.AddInt32(count, 1)

	// After 10 ticks without detecting a split, stop checking
	if tickNum > 10 {
		m.splitChecked.Store(symbol, true)
		return
	}

	prevClose, ok := m.prevClose[symbol]
	if !ok || prevClose <= 0 || ltp <= 0 {
		return
	}

	ratio := ltp / prevClose

	// Check against known split ratios (pure O(1) — no loops, just comparisons)
	var splitType string
	switch {
	case ratio > 0.08 && ratio < 0.12:
		splitType = "1:10"
	case ratio > 0.18 && ratio < 0.22:
		splitType = "1:5"
	case ratio > 0.23 && ratio < 0.28:
		splitType = "1:4"
	case ratio > 0.30 && ratio < 0.36:
		splitType = "1:3"
	case ratio > 0.45 && ratio < 0.55:
		splitType = "1:2"
	case ratio > 1.8 && ratio < 2.2:
		splitType = "2:1 reverse"
	case ratio > 2.8 && ratio < 3.2:
		splitType = "3:1 reverse"
	case ratio > 4.5 && ratio < 5.5:
		splitType = "5:1 reverse"
	case ratio > 9.0 && ratio < 11.0:
		splitType = "10:1 reverse"
	default:
		return // No split detected
	}

	m.logger.Warn("STOCK SPLIT DETECTED (O(1))",
		zap.String("symbol", symbol),
		zap.String("split", splitType),
		zap.Float64("ltp", ltp),
		zap.Float64("prev_close", prevClose),
		zap.Float64("ratio", ratio))

	// Reset instrument history — old pre-split data is now invalid
	instID, _, err := m.repo.GetInstrumentBySymbol(ctx, symbol, "NSE")
	if err != nil || instID == 0 {
		return
	}

	if err := m.repo.ResetInstrumentHistory(ctx, instID, time.Now()); err != nil {
		m.logger.Error("Failed to reset split instrument",
			zap.String("symbol", symbol), zap.Error(err))
	}

	// Clear stale Redis 52W data — WebSocket will repopulate with correct values
	if m.redis != nil {
		m.redis.Del(ctx, fmt.Sprintf("52w:token:%s", symbol))
	}
}

// isETFOrFund returns true if the symbol is an ETF, index fund, or liquid fund.
// These are excluded from breakout detection because they make new 52W highs/lows
// almost daily (just interest/index movement), not real trading signals.
func isETFOrFund(symbol string) bool {
	etfPatterns := []string{
		"BEES", "ETF", "LIQUID", "GOLD", "SILVER",
		"CASE", "GROWW", "AONE", "MONQ", "NPBET",
		"ABSL", "MOM50", "MOMGF", "ADD", "HEALTHCARE",
		"NIFTY1", "NEXT50", "QUAL30", "MID150", "TOP10",
		"TOP15", "TOP20", "EQUAL", "LOWVOL", "ALPHA",
		"VALUE", "MIDQ", "PSUBANK", "FMCG", "INFRA",
		"BANKI", "SETF", "MOVALUE", "MOALPHA", "MODEFENCE",
		"MOMOMENTUM", "MOMENTUM", "CASHIETF", "AUTOBEES",
		"EBANKNIFTY", "HDFCSENSEX", "BSLSENETF", "SENSEX",
	}
	for _, p := range etfPatterns {
		if len(symbol) >= len(p) {
			for i := 0; i <= len(symbol)-len(p); i++ {
				if symbol[i:i+len(p)] == p {
					return true
				}
			}
		}
	}
	return false
}

func (m *WSMonitor) recordBreakout(ctx context.Context, evt BreakoutEvent) {
	// Skip ETFs, index funds, liquid funds — not real breakouts
	if isETFOrFund(evt.Symbol) {
		return
	}

	// Dedup: first detection logs + stores in Redis/Kafka.
	// Subsequent detections only update DB (if price is higher/lower).
	dedupKey := fmt.Sprintf("%s:%s", evt.Symbol, evt.BreakoutType)
	_, alreadyRecorded := m.breakoutToday.LoadOrStore(dedupKey, true)

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

	// Always update all 3 sources with latest price
	// DB: upserts — keeps highest high or lowest low for the day
	_, err = m.repo.InsertBreakoutEvent(ctx, evt)
	if err != nil {
		m.logger.Error("Failed to store breakout event",
			zap.String("symbol", evt.Symbol),
			zap.Error(err))
		return
	}

	// Redis: always update with latest breakout price
	m.storeBreakoutInRedis(ctx, evt)

	// Kafka: always publish latest price (consumers get real-time updates)
	m.publishBreakoutToKafka(ctx, evt)

	// Log only on first detection
	if !alreadyRecorded {
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
	}
}

// publishBreakoutToKafka publishes breakout event to Kafka topic market.data.52w_breakouts.
// Rules-engine, notification service, etc. can consume this for real-time actions.
func (m *WSMonitor) publishBreakoutToKafka(ctx context.Context, evt BreakoutEvent) {
	if m.kafkaWriter == nil {
		return
	}

	breakoutAt := evt.BreakoutAt
	if breakoutAt.IsZero() {
		breakoutAt = time.Now()
	}

	loc, _ := time.LoadLocation("Asia/Kolkata")
	val, _ := json.Marshal(map[string]interface{}{
		"event_type":    evt.BreakoutType,
		"symbol":        evt.Symbol,
		"token":         evt.Token,
		"exchange":      evt.Exchange,
		"price":         evt.BreakoutPrice,
		"prev_52w_high": evt.Prev52WH,
		"prev_52w_low":  evt.Prev52WL,
		"volume":        evt.Volume,
		"pct_change":    evt.PctChange,
		"trade_date":    time.Now().In(loc).Format("2006-01-02"),
		"breakout_at":   breakoutAt.In(loc).Format("2006-01-02 15:04:05 IST"),
		"detected_at":   time.Now().In(loc).Format("2006-01-02 15:04:05 IST"),
	})

	err := m.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.Symbol),
		Value: val,
	})
	if err != nil {
		m.logger.Warn("Failed to publish breakout to Kafka",
			zap.String("symbol", evt.Symbol),
			zap.Error(err))
	}
}

// storeBreakoutInRedis adds the breakout to a Redis sorted set for today's live screener.
// Key: "breakouts:today:{type}" → sorted set by detection time
// Also publishes to a channel for real-time push to frontend.
func (m *WSMonitor) storeBreakoutInRedis(ctx context.Context, evt BreakoutEvent) {
	if m.redis == nil {
		return
	}

	breakoutAt := evt.BreakoutAt
	if breakoutAt.IsZero() {
		breakoutAt = time.Now()
	}

	loc, _ := time.LoadLocation("Asia/Kolkata")

	// Store breakout details in a separate hash key per stock
	// This way updates to price/volume don't create duplicates
	detailKey := fmt.Sprintf("breakout:%s:%s:%s", evt.BreakoutType, evt.Symbol, time.Now().In(loc).Format("2006-01-02"))
	val, _ := json.Marshal(map[string]interface{}{
		"symbol":      evt.Symbol,
		"token":       evt.Token,
		"exchange":    evt.Exchange,
		"price":       evt.BreakoutPrice,
		"prev_high":   evt.Prev52WH,
		"prev_low":    evt.Prev52WL,
		"volume":      evt.Volume,
		"pct_change":  evt.PctChange,
		"breakout_at": breakoutAt.In(loc).Format("2006-01-02 15:04:05 IST"),
		"updated_at":  time.Now().In(loc).Format("2006-04-02 15:04:05 IST"),
	})

	listKey := fmt.Sprintf("breakouts:today:%s", evt.BreakoutType)
	score := float64(breakoutAt.Unix())

	pipe := m.redis.Pipeline()

	// Store/update detail key (always latest price)
	pipe.Set(ctx, detailKey, val, 25*time.Hour)

	// Sorted set: use SYMBOL as member (not JSON) — prevents duplicates
	// Remove old entry first, then add with new score
	pipe.ZRem(ctx, listKey, evt.Symbol)
	pipe.ZAdd(ctx, listKey, redis.Z{Score: score, Member: evt.Symbol})
	pipe.ExpireAt(ctx, listKey, midnightIST())

	// Publish for real-time listeners
	pipe.Publish(ctx, "breakout_events", string(val))
	pipe.Exec(ctx)
}

// getRedis52W fetches 52W values from Redis.
func (m *WSMonitor) getRedis52W(ctx context.Context, token string) (high, low float64) {
	high, low, _ = m.getRedis52WWithSource(ctx, token, "")
	return
}

// getRedis52WWithSource fetches 52W values from Redis and returns the source.
// Tries token key first, then symbol key as fallback.
func (m *WSMonitor) getRedis52WWithSource(ctx context.Context, token, symbol string) (high, low float64, source string) {
	if m.redis == nil {
		return 0, 0, ""
	}

	// Try by token first
	keys := []string{}
	if token != "" {
		keys = append(keys, fmt.Sprintf("52w:token:%s", token))
	}
	if symbol != "" && symbol != token {
		keys = append(keys, fmt.Sprintf("52w:token:%s", symbol))
	}

	for _, key := range keys {
		val, err := m.redis.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var data map[string]interface{}
		if json.Unmarshal([]byte(val), &data) != nil {
			continue
		}

		high, _ = data["high"].(float64)
		low, _ = data["low"].(float64)
		source, _ = data["source"].(string)
		if high > 0 {
			return high, low, source
		}
	}

	return 0, 0, ""
}

// updateRedis52W updates Redis with fresh 52W values from WebSocket tick.
// Writes to BOTH token-based key (52w:token:11452) and symbol-based key (52w:token:CCL)
// so both bhavcopy consumers and WebSocket consumers see the same data.
func (m *WSMonitor) updateRedis52W(ctx context.Context, token, symbol, exchange string, high, low, ltp float64) {
	if m.redis == nil {
		return
	}

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

	ttl := 120 * time.Hour
	pipe := m.redis.Pipeline()

	// Update by exchange token number (e.g., 52w:token:11452)
	if token != "" {
		pipe.Set(ctx, fmt.Sprintf("52w:token:%s", token), val, ttl)
	}

	// Also update by symbol name (e.g., 52w:token:CCL)
	// This is the key bhavcopy uses — keeps them in sync
	if symbol != "" && symbol != token {
		pipe.Set(ctx, fmt.Sprintf("52w:token:%s", symbol), val, ttl)
	}

	pipe.Exec(ctx)
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
			m.breakoutToday = sync.Map{}  // Reset breakout dedup for new day
			m.splitChecked = sync.Map{}   // Reset split detection for new day
			m.splitTicks = sync.Map{}     // Reset tick counters
			m.logger.Info("Midnight reset: cleared daily breakout + split tracking")
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

// parseTimestamp parses broker's timestamp string (e.g., "2026-04-06T10:32:15+05:30").
// Returns zero time if parsing fails.
func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	// Try RFC3339 first
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return t
	}
	// Try without timezone
	t, err = time.Parse("2006-01-02T15:04:05", s)
	if err == nil {
		loc, _ := time.LoadLocation("Asia/Kolkata")
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, loc)
	}
	return time.Time{}
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
