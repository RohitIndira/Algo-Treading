package wss

// Dealer broadcast listener.
//
// One subscription to wss://.../order-notify/websocket/broadcast delivers the
// order-status frames of EVERY client of the dealer, whether or not that
// client has a session with us. Frames are the same ORD_NRML shape the
// per-user socket delivers, so they go through the same ingest
// (Listener.ingest) with source WSS_BROADCAST and dedupe on
// (broker_order_id, event_seq) against the per-user socket and the REST polls.
//
// Protocol (Indira "Real-time Order Status WebSocket Guide", verified live
// 2026-10-26):
//
//	GET  /order-notify/ws/createWsToken   Authorization: Bearer <session>
//	     → {"status":"Ok","result":[{"orderToken":"…"}]}
//	WSS  /order-notify/websocket/broadcast
//	     → text {"userId":"<id>","orderToken":"<token>"}   reply {"status":"Ok"}
//	     → text {"userId":"<id>","heartbeat":"h"} every 50 s
//	     server closes after 5 min without heartbeat or when the session dies
//
// Read-only toward the broker: this file never places, modifies or cancels
// anything. Writes to our store / Kafka happen only in own_orders and all
// modes, and only through Listener.ingest.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/alert"
	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

// Mode is BROADCAST_FEED_MODE.
type Mode string

const (
	// ModeOff — listener not started; service behaves exactly as before.
	ModeOff Mode = "off"
	// ModeShadow — connect, parse, compare against the per-user path, write
	// nothing. Default.
	ModeShadow Mode = "shadow"
	// ModeOwnOrders — write/publish only frames whose UniqueCode or AMOOrderID
	// is one of OUR orders (trade-execution LookupOrderMeta).
	ModeOwnOrders Mode = "own_orders"
	// ModeAll — write/publish every frame of a managed client.
	ModeAll Mode = "all"
)

// ParseMode parses BROADCAST_FEED_MODE; empty means ModeShadow.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(strings.ToLower(strings.TrimSpace(s))); m {
	case "":
		return ModeShadow, nil
	case ModeOff, ModeShadow, ModeOwnOrders, ModeAll:
		return m, nil
	default:
		return "", fmt.Errorf("BROADCAST_FEED_MODE=%q: want off|shadow|own_orders|all", s)
	}
}

// Clock is the injectable time source (heartbeat cadence, backoff, stale
// checks). realClock is the default.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// OwnOrderLookup answers "is this broker order id one of ours?" (own_orders
// mode). tradeexec.Client implements it over gRPC.
type OwnOrderLookup interface {
	IsOurOrder(ctx context.Context, brokerOrderID string) (bool, error)
}

// ShadowLookup finds the row the per-user path stored for the same event
// (shadow mode). *store.Writer implements it.
type ShadowLookup interface {
	LookupObserved(ctx context.Context, brokerOrderID string, eventSeq int64) (*store.ObservedRow, error)
}

// BroadcastConfig tunes the listener. Zero values take the defaults below.
type BroadcastConfig struct {
	Mode       Mode
	AnchorUser string // BROADCAST_ANCHOR_USER; optional dedicated dealer login

	HandshakeTimeout  time.Duration // wait for {"status":"Ok"}; default 10 s
	HeartbeatInterval time.Duration // default 50 s
	MaxSilence        time.Duration // no bytes at all for this long → reconnect with a new token; default 6 min
	Backoff           []time.Duration
	ShadowWindow      time.Duration // how long to give the per-user path before comparing; default 5 s
	ManagedRefresh    time.Duration // re-list managed users; default 5 min
	StaleAfter        time.Duration // feed-stale alert threshold in market hours; default 3 min
	StaleCheckEvery   time.Duration // default 30 s
	DeadAnchorTTL     time.Duration // retry a rejected anchor after this; default 30 min
	TokenFailAlert    time.Duration // alert when token endpoint failing for longer; default 5 min

	MarketOpen func(time.Time) bool // default indira.IsMarketOpen
	Clock      Clock
}

func (c *BroadcastConfig) defaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.HandshakeTimeout, 10*time.Second)
	def(&c.HeartbeatInterval, 50*time.Second)
	def(&c.MaxSilence, 6*time.Minute)
	def(&c.ShadowWindow, 5*time.Second)
	def(&c.ManagedRefresh, 5*time.Minute)
	def(&c.StaleAfter, 3*time.Minute)
	def(&c.StaleCheckEvery, 30*time.Second)
	def(&c.DeadAnchorTTL, 30*time.Minute)
	def(&c.TokenFailAlert, 5*time.Minute)
	if len(c.Backoff) == 0 {
		c.Backoff = []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}
	}
	if c.MarketOpen == nil {
		c.MarketOpen = indira.IsMarketOpen
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.Mode == "" {
		c.Mode = ModeShadow
	}
}

// BroadcastStats are plain counters mirrored into Prometheus; tests read them.
type BroadcastStats struct {
	Frames, Unknown, Control, Malformed       atomic.Uint64
	Inserted, Deduped, Dropped, WriteErrors   atomic.Uint64
	SkippedNotOwn, ShadowSeen, ShadowMissed   atomic.Uint64
	Reconnects, AnchorSwitches, Heartbeats    atomic.Uint64
	TokenErrors, Connects, StaleAlerts, Noops atomic.Uint64
}

// BroadcastListener owns the broadcast socket.
type BroadcastListener struct {
	cfg     BroadcastConfig
	client  *indira.Client
	sink    *Listener
	users   UserSource
	own     OwnOrderLookup // own_orders mode; nil = fail closed
	shadow  ShadowLookup   // shadow mode; nil = count only
	alerter alert.Alerter
	logger  *zap.Logger

	managedMu sync.RWMutex
	managed   map[string]struct{}

	anchorMu         sync.Mutex
	current          string // anchor user id of the live connection
	deadAnchors      map[string]time.Time
	alertedDead      map[string]bool
	noAnchorAlerted  bool
	tokenFailSince   time.Time
	tokenFailAlerted bool

	lastFrameNano atomic.Int64 // 0 before the first frame
	connected     atomic.Bool
	connectedAt   atomic.Int64
	stats         BroadcastStats
	controlLogged sync.Once
	ownNilLogged  sync.Once

	shadowQ chan shadowItem
	wg      sync.WaitGroup
}

type shadowItem struct {
	ucc, brokerOrderID, status string
	seq                        int64
	at                         time.Time
}

// NewBroadcastListener wires the listener. sink is the per-user Listener
// whose ingest path is reused; alerter may be nil (log only).
func NewBroadcastListener(cfg BroadcastConfig, client *indira.Client, sink *Listener, users UserSource,
	own OwnOrderLookup, shadow ShadowLookup, alerter alert.Alerter, logger *zap.Logger) *BroadcastListener {
	cfg.defaults()
	if alerter == nil {
		alerter = alert.New(nil, logger)
	}
	return &BroadcastListener{
		cfg:         cfg,
		client:      client,
		sink:        sink,
		users:       users,
		own:         own,
		shadow:      shadow,
		alerter:     alerter,
		logger:      logger.With(zap.String("component", "broadcast"), zap.String("mode", string(cfg.Mode))),
		managed:     map[string]struct{}{},
		deadAnchors: map[string]time.Time{},
		alertedDead: map[string]bool{},
		shadowQ:     make(chan shadowItem, 10000),
	}
}

// Stats returns the live counters.
func (b *BroadcastListener) Stats() *BroadcastStats { return &b.stats }

// Connected reports whether the socket is up and the handshake was accepted.
func (b *BroadcastListener) Connected() bool { return b.connected.Load() }

// CurrentAnchor returns the user id anchoring the live connection ("" if none).
func (b *BroadcastListener) CurrentAnchor() string {
	b.anchorMu.Lock()
	defer b.anchorMu.Unlock()
	return b.current
}

// HandshakeFrame is the exact text sent after the dial.
func HandshakeFrame(userID, orderToken string) []byte {
	b, _ := json.Marshal(indira.WSConnectionRequest{UserId: userID, OrderToken: orderToken})
	return b
}

// HeartbeatFrame is the exact text sent every HeartbeatInterval.
func HeartbeatFrame(userID string) []byte {
	b, _ := json.Marshal(indira.WSHeartbeat{UserId: userID, Heartbeat: "h"})
	return b
}

// Start runs the listener until ctx is cancelled (mode off → returns at once).
// Wait blocks until every goroutine has stopped.
func (b *BroadcastListener) Start(ctx context.Context) {
	if b.cfg.Mode == ModeOff {
		b.logger.Info("broadcast listener off")
		return
	}
	b.refreshManaged(ctx)
	b.wg.Add(3)
	go func() { defer b.wg.Done(); b.managedLoop(ctx) }()
	go func() { defer b.wg.Done(); b.staleLoop(ctx) }()
	go func() { defer b.wg.Done(); b.shadowLoop(ctx) }()
	b.wg.Add(1)
	go func() { defer b.wg.Done(); b.run(ctx) }()
	b.logger.Info("broadcast listener started", zap.String("anchor_user", b.cfg.AnchorUser))
}

// Wait blocks until Start's goroutines have exited.
func (b *BroadcastListener) Wait() { b.wg.Wait() }

// run is the connect / read / reconnect loop.
func (b *BroadcastListener) run(ctx context.Context) {
	attempt := 0
	for ctx.Err() == nil {
		healthy, err := b.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if healthy {
			attempt = 0
		}
		b.stats.Reconnects.Add(1)
		mBroadcastReconnects.Inc()
		wait := b.cfg.Backoff[min(attempt, len(b.cfg.Backoff)-1)]
		attempt++
		b.logger.Warn("broadcast session ended — reconnecting",
			zap.Error(err), zap.Duration("backoff", wait), zap.Int("attempt", attempt))
		select {
		case <-ctx.Done():
			return
		case <-b.cfg.Clock.After(wait):
		}
	}
}

var errNoAnchor = errors.New("no usable anchor session")

// session does one full connection: anchor → token → dial → handshake →
// heartbeat + read until the connection ends. healthy=true when the session
// got as far as an accepted handshake (resets the backoff).
func (b *BroadcastListener) session(ctx context.Context) (healthy bool, err error) {
	auth := b.pickAnchor(ctx)
	if auth == nil {
		b.anchorMu.Lock()
		alerted := b.noAnchorAlerted
		b.noAnchorAlerted = true
		b.anchorMu.Unlock()
		if !alerted {
			b.alerter.Alert(ctx, "BROADCAST_ANCHOR_LOST", b.cfg.AnchorUser,
				"No live session can anchor the dealer broadcast feed (configured anchor unusable and no managed user has a valid session). Feed is down; per-user sockets and REST polls continue.")
		}
		return false, errNoAnchor
	}
	b.anchorMu.Lock()
	b.noAnchorAlerted = false
	b.anchorMu.Unlock()

	tokenCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	token, err := b.client.GetWebSocketToken(tokenCtx, auth)
	cancel()
	if err != nil {
		b.stats.TokenErrors.Add(1)
		mBroadcastTokenErrors.Inc()
		b.noteTokenFailure(ctx, auth.UserId, err)
		if isAuthFailure(err) {
			b.markAnchorDead(ctx, auth.UserId, err)
		}
		return false, fmt.Errorf("createWsToken as %s: %w", auth.UserId, err)
	}
	b.clearTokenFailure()

	dialer := &websocket.Dialer{HandshakeTimeout: b.cfg.HandshakeTimeout, Proxy: http.ProxyFromEnvironment}
	conn, resp, err := dialer.DialContext(ctx, b.wsURL(), nil)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		return false, fmt.Errorf("dial broadcast (http %d): %w", code, err)
	}
	defer conn.Close()

	var writeMu sync.Mutex
	write := func(msgType int, data []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteMessage(msgType, data)
	}

	if err := write(websocket.TextMessage, HandshakeFrame(auth.UserId, token)); err != nil {
		return false, fmt.Errorf("handshake write: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(b.cfg.HandshakeTimeout))
	_, reply, err := conn.ReadMessage()
	if err != nil {
		return false, fmt.Errorf("handshake reply: %w", err)
	}
	var hs struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(reply, &hs) != nil || !strings.EqualFold(strings.TrimSpace(hs.Status), "ok") {
		// The broker refused the (userId, orderToken) pair: treat as a dead
		// session so the next attempt uses another anchor.
		herr := fmt.Errorf("handshake rejected: %.200s", string(reply))
		b.markAnchorDead(ctx, auth.UserId, herr)
		return false, herr
	}

	b.setConnected(auth.UserId, true)
	defer b.setConnected(auth.UserId, false)
	b.stats.Connects.Add(1)
	b.logger.Info("broadcast connected", zap.String("anchor_user", auth.UserId), zap.String("url", b.wsURL()))

	// Heartbeat goroutine; a failed write closes the socket so the read loop
	// returns and we reconnect.
	hbStop := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		for {
			select {
			case <-hbStop:
				return
			case <-b.cfg.Clock.After(b.cfg.HeartbeatInterval):
				if err := write(websocket.TextMessage, HeartbeatFrame(auth.UserId)); err != nil {
					b.logger.Warn("broadcast heartbeat write failed", zap.Error(err))
					_ = conn.Close()
					return
				}
				b.stats.Heartbeats.Add(1)
			}
		}
	}()
	defer func() { close(hbStop); <-hbDone }()

	// Clean shutdown: on ctx cancel send a close frame, then close the socket
	// to unblock ReadMessage.
	closeDone := make(chan struct{})
	defer close(closeDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = write(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"))
			_ = conn.Close()
		case <-closeDone:
		}
	}()

	// Read loop. The read deadline IS the silence cutoff: gorilla treats a
	// deadline timeout as a permanent connection failure (a second
	// ReadMessage after it panics), so a quiet socket is not "continued" —
	// it ends the session and we reconnect with a fresh token. MaxSilence is
	// longer than the broker's own 5-minute idle close so a legitimately
	// quiet feed is not churned.
	for {
		_ = conn.SetReadDeadline(time.Now().Add(b.cfg.MaxSilence))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			var ne interface{ Timeout() bool }
			if errors.As(err, &ne) && ne.Timeout() {
				return true, fmt.Errorf("no bytes for %s — reconnecting", b.cfg.MaxSilence)
			}
			return true, fmt.Errorf("read: %w", err)
		}
		b.handleFrame(ctx, msg)
	}
}

// wsURL derives the broadcast socket URL from the REST base URL so one
// INDIRA_BASE_URL (or a test server) steers both.
func (b *BroadcastListener) wsURL() string {
	base := strings.TrimRight(b.client.BaseURL(), "/")
	switch {
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base + indira.BroadcastWSPath
}

func (b *BroadcastListener) setConnected(user string, up bool) {
	b.connected.Store(up)
	b.anchorMu.Lock()
	if up {
		b.current = user
		b.connectedAt.Store(b.cfg.Clock.Now().UnixNano())
		mBroadcastAnchor.Reset()
		mBroadcastAnchor.WithLabelValues(user).Set(1)
		mBroadcastConnected.Set(1)
	} else {
		b.current = ""
		mBroadcastAnchor.WithLabelValues(user).Set(0)
		mBroadcastConnected.Set(0)
	}
	b.anchorMu.Unlock()
}

func (b *BroadcastListener) noteTokenFailure(ctx context.Context, user string, err error) {
	now := b.cfg.Clock.Now()
	b.anchorMu.Lock()
	if b.tokenFailSince.IsZero() {
		b.tokenFailSince = now
	}
	fire := !b.tokenFailAlerted && now.Sub(b.tokenFailSince) > b.cfg.TokenFailAlert
	if fire {
		b.tokenFailAlerted = true
	}
	b.anchorMu.Unlock()
	if fire {
		b.alerter.Alert(ctx, "BROADCAST_TOKEN_FAILING", user,
			fmt.Sprintf("createWsToken has been failing for %s (last: %v). Broadcast feed is down; per-user sockets and REST polls continue.", b.cfg.TokenFailAlert, err))
	}
}

func (b *BroadcastListener) clearTokenFailure() {
	b.anchorMu.Lock()
	b.tokenFailSince = time.Time{}
	b.tokenFailAlerted = false
	b.anchorMu.Unlock()
}

// ── managed-client set ──────────────────────────────────────────────────

func (b *BroadcastListener) refreshManaged(ctx context.Context) {
	lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	ids, err := b.users.ListActiveUserIDs(lctx)
	cancel()
	if err != nil {
		b.logger.Warn("broadcast: managed user list refresh failed — keeping previous set", zap.Error(err))
		return
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			set[strings.ToUpper(id)] = struct{}{}
		}
	}
	b.managedMu.Lock()
	b.managed = set
	b.managedMu.Unlock()
	b.logger.Info("broadcast: managed client set refreshed", zap.Int("count", len(set)))
}

func (b *BroadcastListener) isManaged(ucc string) bool {
	b.managedMu.RLock()
	defer b.managedMu.RUnlock()
	_, ok := b.managed[strings.ToUpper(ucc)]
	return ok
}

func (b *BroadcastListener) managedLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.cfg.Clock.After(b.cfg.ManagedRefresh):
			b.refreshManaged(ctx)
		}
	}
}

// ── frame handling ──────────────────────────────────────────────────────

// handleFrame parses one text frame and routes it by mode.
func (b *BroadcastListener) handleFrame(ctx context.Context, raw []byte) {
	var probe struct {
		UCC string `json:"UCC"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		b.stats.Malformed.Add(1)
		mBroadcastMalformed.Inc()
		b.logger.Warn("broadcast: malformed frame skipped", zap.String("head", head(raw, 120)))
		return
	}
	if strings.TrimSpace(probe.UCC) == "" {
		b.stats.Control.Add(1)
		mBroadcastControl.Inc()
		b.controlLogged.Do(func() {
			b.logger.Info("broadcast: control frame (logged once)", zap.String("frame", head(raw, 200)))
		})
		return
	}
	var ws indira.WSOrderStatus
	if err := json.Unmarshal(raw, &ws); err != nil {
		b.stats.Malformed.Add(1)
		mBroadcastMalformed.Inc()
		b.logger.Warn("broadcast: order frame did not parse", zap.Error(err), zap.String("head", head(raw, 120)))
		return
	}
	now := b.cfg.Clock.Now()
	b.lastFrameNano.Store(now.UnixNano())
	b.stats.Frames.Add(1)
	ucc := strings.ToUpper(strings.TrimSpace(ws.UCC))

	if !b.isManaged(ucc) {
		b.stats.Unknown.Add(1)
		mBroadcastUnknownUCC.Inc()
		mBroadcastFrames.WithLabelValues("other").Inc()
		return
	}
	mBroadcastFrames.WithLabelValues(ucc).Inc()

	switch b.cfg.Mode {
	case ModeShadow:
		b.enqueueShadow(ucc, &ws, now)
	case ModeOwnOrders:
		if !b.isOurs(ctx, &ws) {
			b.stats.SkippedNotOwn.Add(1)
			mBroadcastSkippedNotOwn.Inc()
			return
		}
		b.write(ctx, &ws)
	case ModeAll:
		b.write(ctx, &ws)
	}
}

// isOurs asks trade-execution whether UniqueCode or AMOOrderID is a Manthan
// order. nil lookup or lookup error → false (fail closed).
func (b *BroadcastListener) isOurs(ctx context.Context, ws *indira.WSOrderStatus) bool {
	if b.own == nil {
		b.ownNilLogged.Do(func() {
			b.logger.Error("broadcast own_orders mode without a trade-execution lookup — every frame is skipped (set TRADE_EXEC_GRPC_ADDR)")
		})
		return false
	}
	for _, id := range []string{strings.TrimSpace(ws.UniqueCode), strings.TrimSpace(ws.AMOOrderID)} {
		if id == "" || id == "0" {
			continue
		}
		ok, err := b.own.IsOurOrder(ctx, id)
		if err != nil {
			b.logger.Warn("broadcast: own-order lookup failed — frame skipped", zap.String("broker_order_id", id), zap.Error(err))
			continue
		}
		if ok {
			return true
		}
	}
	return false
}

// write hands the frame to the shared ingest with source WSS_BROADCAST.
func (b *BroadcastListener) write(ctx context.Context, ws *indira.WSOrderStatus) {
	id := strings.TrimSpace(ws.UniqueCode)
	if id == "" || id == "0" {
		b.stats.Dropped.Add(1)
		mBroadcastWritten.WithLabelValues("dropped").Inc()
		return
	}
	inserted, err := b.sink.ingest(ctx, ws, store.SourceWSSBroadcast)
	switch {
	case err != nil:
		b.stats.WriteErrors.Add(1)
		mBroadcastWritten.WithLabelValues("error").Inc()
	case inserted:
		b.stats.Inserted.Add(1)
		mBroadcastWritten.WithLabelValues("inserted").Inc()
	default:
		b.stats.Deduped.Add(1)
		mBroadcastWritten.WithLabelValues("deduped").Inc()
	}
}

// ── shadow comparison ───────────────────────────────────────────────────

func (b *BroadcastListener) enqueueShadow(ucc string, ws *indira.WSOrderStatus, at time.Time) {
	id := strings.TrimSpace(ws.UniqueCode)
	if id == "" || id == "0" {
		b.stats.Dropped.Add(1)
		return
	}
	item := shadowItem{ucc: ucc, brokerOrderID: id, status: strings.ToUpper(strings.TrimSpace(ws.OrderStatus)), seq: eventSeqFrom(ws), at: at}
	select {
	case b.shadowQ <- item:
	default:
		b.stats.Dropped.Add(1)
		b.logger.Warn("broadcast shadow queue full — comparison dropped", zap.String("broker_order_id", id))
	}
}

func (b *BroadcastListener) shadowLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-b.shadowQ:
			wait := b.cfg.ShadowWindow - b.cfg.Clock.Now().Sub(item.at)
			if wait > 0 {
				select {
				case <-ctx.Done():
					return
				case <-b.cfg.Clock.After(wait):
				}
			}
			b.compareShadow(ctx, item)
		}
	}
}

func (b *BroadcastListener) compareShadow(ctx context.Context, item shadowItem) {
	if b.shadow == nil {
		b.stats.Noops.Add(1)
		return
	}
	lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	row, err := b.shadow.LookupObserved(lctx, item.brokerOrderID, item.seq)
	cancel()
	if err != nil {
		b.logger.Warn("broadcast shadow lookup failed", zap.String("broker_order_id", item.brokerOrderID), zap.Error(err))
		return
	}
	if row == nil {
		b.stats.ShadowMissed.Add(1)
		mBroadcastShadowRecall.WithLabelValues("false").Inc()
		// The review list for stage 3: events only the broadcast saw.
		b.logger.Info("broadcast shadow: frame not seen by per-user path",
			zap.String("ucc", item.ucc), zap.String("broker_order_id", item.brokerOrderID),
			zap.Int64("event_seq", item.seq), zap.String("status", item.status))
		return
	}
	b.stats.ShadowSeen.Add(1)
	mBroadcastShadowRecall.WithLabelValues("true").Inc()
	lead := row.ObservedAt.Sub(item.at).Seconds() // >0: broadcast was earlier
	mBroadcastShadowLead.Observe(lead)
	b.logger.Debug("broadcast shadow: matched",
		zap.String("broker_order_id", item.brokerOrderID), zap.Int64("event_seq", item.seq),
		zap.String("stored_by", string(row.Source)), zap.Float64("lead_seconds", lead))
}

// ── feed-stale monitor ──────────────────────────────────────────────────

// staleLoop raises BROADCAST_FEED_STALE once when no frame has arrived for
// StaleAfter during market hours on a trading day, and re-arms when frames
// resume. Also keeps broadcast_last_frame_age_seconds fresh.
func (b *BroadcastListener) staleLoop(ctx context.Context) {
	alerted := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.cfg.Clock.After(b.cfg.StaleCheckEvery):
		}
		now := b.cfg.Clock.Now()
		last := b.lastFrameNano.Load()
		if last == 0 {
			mBroadcastLastFrameAge.Set(-1)
		} else {
			mBroadcastLastFrameAge.Set(now.Sub(time.Unix(0, last)).Seconds())
		}
		if !b.cfg.MarketOpen(now) {
			alerted = false
			continue
		}
		ref := last
		if ref == 0 {
			ref = b.connectedAt.Load()
		}
		if ref == 0 {
			continue // never connected yet; the anchor/token alerts cover that
		}
		age := now.Sub(time.Unix(0, ref))
		if age <= b.cfg.StaleAfter {
			alerted = false
			continue
		}
		if !alerted {
			alerted = true
			b.stats.StaleAlerts.Add(1)
			b.alerter.Alert(ctx, "BROADCAST_FEED_STALE", b.CurrentAnchor(),
				fmt.Sprintf("No order frame on the dealer broadcast for %s during market hours (connected=%v). Per-user sockets and REST polls continue to cover.", age.Truncate(time.Second), b.connected.Load()))
		}
	}
}

func head(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
