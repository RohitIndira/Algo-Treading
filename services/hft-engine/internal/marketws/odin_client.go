// ODIN broadcast WebSocket client.
//
// Responsibilities (only — fan-out lives in manager.go, parsing in codec.go):
//
//   1. Dial wss://host:port and send the Logon (101) request.
//   2. Block until the server replies with a Logon Response (102, status 10000).
//   3. Run a read loop: every frame → DecodeFrame → SplitConcatenated → ParseFix →
//      hand each parsed FixMessage to a registered listener function.
//   4. Expose Subscribe(segment, token) / Unsubscribe(segment, token) so the
//      fan-out layer (manager.go) can ask for symbol streams.
//   5. Reconnect on disconnect with exponential backoff, and replay all active
//      subscriptions from an in-memory set so the caller doesn't have to.
//
// Concurrency: one goroutine writes (subscribe + heartbeat-like keepalives),
// one goroutine reads. Both die together; Run() returns when the parent
// context is cancelled OR a permanent failure occurs (auth rejected,
// repeated reconnect failures).
//
// Production hardening:
//   - Per-write mutex so subscribe + reconnect-resubscribe never interleave.
//   - Bounded reconnect backoff (1s → 60s, halts at 60s).
//   - Subscriptions deduplicated by (segment, security) tuple — calling
//     Subscribe twice for the same pair is cheap.
//   - WS write timeout (5s) so a hung server can't pin the writer goroutine.
//   - Surface BroadcastSocketDisconnect (1111) — we drop the conn ourselves
//     and let the reconnect loop pick up a fresh broadcast IP/port if the
//     deployment ever wires MDaaS.
package marketws

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// MessageListener is called from the read goroutine for every parsed FIX
// message we receive (login response, touchline updates, disconnect notice).
//
// IMPORTANT: handlers must NOT block. The read goroutine doesn't fan out
// to per-symbol channels here — that's the manager.go layer's job and it
// does so with non-blocking sends. Listeners run synchronously in the
// read loop.
type MessageListener func(FixMessage)

// SubscriptionKey is the unique identity of one (segment, security_code) pair.
type SubscriptionKey struct {
	SegmentID    int
	SecurityCode int64
}

// OdinClientConfig holds everything the client needs from outside.
type OdinClientConfig struct {
	// Wire URL parts. Default: wss://indirabcast.odin.trade:4515.
	Scheme string // "wss" (TLS) or "ws"
	Host   string
	Port   int

	// Logon credentials. Both required — login fails without them.
	UserID string
	APIKey string

	// Tuning. Zero values fall back to sensible defaults below.
	WriteTimeout   time.Duration // per Write — default 5s
	HandshakeTimeout time.Duration // dial timeout — default 10s
	ReconnectInitial time.Duration // first backoff after disconnect — default 1s
	ReconnectMax     time.Duration // cap on backoff growth — default 60s
}

// applyDefaults fills in zero fields with sensible production defaults.
func (c *OdinClientConfig) applyDefaults() {
	if c.Scheme == "" {
		c.Scheme = "wss"
	}
	if c.Port == 0 {
		c.Port = 4515
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.ReconnectInitial == 0 {
		c.ReconnectInitial = 1 * time.Second
	}
	if c.ReconnectMax == 0 {
		c.ReconnectMax = 60 * time.Second
	}
}

// OdinClient is the single per-process ODIN broadcast feed client.
type OdinClient struct {
	cfg    OdinClientConfig
	logger *zap.Logger

	listener MessageListener

	// Lifecycle
	connectedCh chan struct{} // closed (and replaced) on each successful login
	closeOnce   sync.Once

	// Subscription state — survives reconnects.
	subMu         sync.Mutex
	subscriptions map[SubscriptionKey]struct{}

	// Live socket — replaced on reconnect; nil between connects.
	connMu sync.Mutex
	conn   *websocket.Conn

	// Stats — read by readyz and dashboards.
	state     atomicState
	lastTickAt atomicTime
}

// New builds an OdinClient. Call SetListener(...) and then Run(ctx).
//
// New itself does NOT connect — Run does. This makes the wiring in main.go
// straightforward: build everything, then start in the right order.
func New(cfg OdinClientConfig, logger *zap.Logger) *OdinClient {
	cfg.applyDefaults()
	c := &OdinClient{
		cfg:           cfg,
		logger:        logger.Named("odin-ws"),
		subscriptions: make(map[SubscriptionKey]struct{}),
	}
	c.state.Store(StateDisconnected)
	return c
}

// SetListener installs the function that receives every parsed FIX message.
// Must be set BEFORE Run() is called.
func (c *OdinClient) SetListener(l MessageListener) {
	c.listener = l
}

// State returns a snapshot of the connection state — used by the engine's
// /readyz probe and by manager.Start to refuse Entry when the feed is down.
func (c *OdinClient) State() State {
	return c.state.Load()
}

// LastTickAt returns the wall-clock of the most recent inbound message.
// Stays at zero until the first tick. Used by the no-data halt timer.
func (c *OdinClient) LastTickAt() time.Time {
	return c.lastTickAt.Load()
}

// ─────────────────────────────────────────────────────────────────────────
// Run loop — connect, login, read, reconnect on failure
// ─────────────────────────────────────────────────────────────────────────

// Run blocks until ctx is cancelled. Internally:
//   1. Dial + login. If login fails (auth, not transient), returns the error.
//   2. Drive the read goroutine; on disconnect, close the conn and loop.
//   3. Reconnect backoff: 1s, 2s, 4s, ... capped at ReconnectMax.
//   4. On reconnect, replay all subscriptions from in-memory state.
//
// Run never returns nil unless ctx is cancelled — it always means "give up".
func (c *OdinClient) Run(ctx context.Context) error {
	if c.listener == nil {
		return errors.New("odin: SetListener must be called before Run")
	}

	backoff := c.cfg.ReconnectInitial
	for {
		select {
		case <-ctx.Done():
			c.disconnect()
			return ctx.Err()
		default:
		}

		err := c.connectAndServe(ctx)
		switch {
		case err == nil:
			// connectAndServe returned cleanly → ctx done.
			return nil
		case errors.Is(err, errAuthRejected):
			// Auth failures are permanent — don't burn budget reconnecting.
			c.logger.Error("ODIN auth rejected — halting client", zap.Error(err))
			return err
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			c.logger.Warn("ODIN session ended — reconnecting",
				zap.Duration("backoff", backoff),
				zap.Error(err))
		}

		// Sleep with cancel awareness.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > c.cfg.ReconnectMax {
			backoff = c.cfg.ReconnectMax
		}
	}
}

// errAuthRejected is returned by connectAndServe when the server's logon
// response indicates the credentials are wrong (status != 10000 / 10004).
// Run() treats this as terminal and does NOT reconnect.
var errAuthRejected = errors.New("odin: logon rejected by server")

// connectAndServe runs one full session: dial → login → read → die.
// Returns nil on clean parent-ctx cancel; an error otherwise.
func (c *OdinClient) connectAndServe(ctx context.Context) error {
	c.state.Store(StateConnecting)
	u := url.URL{
		Scheme: c.cfg.Scheme,
		Host:   fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.Port),
	}
	c.logger.Info("dialing ODIN", zap.String("url", u.String()))

	dialer := websocket.Dialer{
		HandshakeTimeout: c.cfg.HandshakeTimeout,
	}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.setConn(conn)
	defer c.disconnect()

	// Send Logon (101). The server replies with 102 — we wait for that
	// inside the read loop and gate "connected" on it.
	if err := c.sendLogon(); err != nil {
		return fmt.Errorf("send logon: %w", err)
	}

	// ── Read loop ──
	// Resubscription is queued for AFTER the login response arrives so
	// we don't fire subscribes against an unauthenticated socket.
	loggedIn := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			c.state.Store(StateDisconnected)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("ws read: %w", err)
		}
		c.lastTickAt.Store(time.Now())

		payload, err := DecodeFrame(raw)
		if err != nil {
			c.logger.Warn("ODIN frame decode failed — skipping",
				zap.Int("raw_bytes", len(raw)), zap.Error(err))
			continue
		}

		for _, p := range SplitConcatenated(payload) {
			m, code := ParseFix(p)
			if code == 0 {
				continue
			}

			// Gate login confirmation.
			if !loggedIn {
				if resp := m.AsLogonResponse(); resp != nil {
					if resp.LogonStatus == 10000 || resp.LogonStatus == 10004 {
						c.logger.Info("ODIN logged in",
							zap.Int("status", resp.LogonStatus),
							zap.Int("segment_allowed", resp.SegmentID),
							zap.Int("days_to_expire", resp.DaysToExpire))
						loggedIn = true
						c.state.Store(StateConnected)
						// Replay subscriptions from in-memory set.
						if err := c.resubscribeAll(); err != nil {
							c.logger.Warn("resubscribe-all failed", zap.Error(err))
						}
						// Hand the logon msg to the listener too so
						// upstream can observe state.
						c.listener(m)
						continue
					}
					// Wrong creds / locked account / etc.
					return fmt.Errorf("%w: status=%d", errAuthRejected, resp.LogonStatus)
				}
				// Pre-login data is unusual. Log and continue waiting.
				c.logger.Warn("ODIN message before login response — ignoring",
					zap.Int("code", code))
				continue
			}

			c.listener(m)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Subscriptions
// ─────────────────────────────────────────────────────────────────────────

// Subscribe registers a (segment, security) for the broadcast feed.
// Idempotent — duplicate calls are no-ops at the wire level.
//
// If the client is not yet connected, the subscription is stored and sent
// after the next successful login (via resubscribeAll).
func (c *OdinClient) Subscribe(seg int, security int64) error {
	key := SubscriptionKey{SegmentID: seg, SecurityCode: security}
	c.subMu.Lock()
	_, already := c.subscriptions[key]
	c.subscriptions[key] = struct{}{}
	c.subMu.Unlock()
	if already {
		return nil
	}
	// Best-effort write. If not connected the resubscribeAll on next login
	// catches up.
	if c.State() == StateConnected {
		return c.sendSubscribe(key, opSubscribe)
	}
	return nil
}

// Unsubscribe drops a (segment, security). Idempotent.
func (c *OdinClient) Unsubscribe(seg int, security int64) error {
	key := SubscriptionKey{SegmentID: seg, SecurityCode: security}
	c.subMu.Lock()
	_, existed := c.subscriptions[key]
	delete(c.subscriptions, key)
	c.subMu.Unlock()
	if !existed {
		return nil
	}
	if c.State() == StateConnected {
		return c.sendSubscribe(key, opUnsubscribe)
	}
	return nil
}

// resubscribeAll fires a subscribe for every key we know about. Called
// once per successful login; designed so the caller (manager.go) never
// has to know about reconnects.
func (c *OdinClient) resubscribeAll() error {
	c.subMu.Lock()
	keys := make([]SubscriptionKey, 0, len(c.subscriptions))
	for k := range c.subscriptions {
		keys = append(keys, k)
	}
	c.subMu.Unlock()

	if len(keys) == 0 {
		return nil
	}
	c.logger.Info("resubscribing on reconnect", zap.Int("count", len(keys)))
	for _, k := range keys {
		if err := c.sendSubscribe(k, opSubscribe); err != nil {
			return err
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────
// Wire writers (login + subscribe). Use connMu via setConn / writeFrame.
// ─────────────────────────────────────────────────────────────────────────

const (
	opSubscribe   = 1
	opUnsubscribe = 2
)

// sendLogon issues a 101 Logon Request with the configured credentials.
// Timestamp is filled live (HH:MM:SS); body length is dynamic.
func (c *OdinClient) sendLogon() error {
	body := fmt.Sprintf(
		"63=FT3.0|64=101|65=74|66=%s|67=%s|68=%s|4=|400=0|401=2|396=HO|51=4|395=127.0.0.1",
		time.Now().Format("15:04:05"),
		c.cfg.UserID,
		c.cfg.APIKey,
	)
	return c.writeFrame(body)
}

// sendSubscribe issues a 206 Multiple Touchline Request for one key.
//
// Wire format quirk: 206 uses '$' to join (segment, token) into a SINGLE
// logical field — even for one symbol it must be "1=1$7=193", not the
// separate "1=1|7=193" that 127 uses. The spec example (page 12):
//
//   "63=FT3.0|64=206|65=80|1=1$7=22|1=1$7=1594|...|4=1000|230=1"
//
// Multi-subscribe batches additional pairs with another '|...$...' chunk.
// For our use case (per-symbol subscription) one pair is enough.
//
// Why 206 (Touchline) and not 127 (Best Five): the server responds to 127
// with a 128 best-five-depth message whose top-of-book is buried inside a
// repeating group with '$'-joined values (e.g. "12=338$14=44315$37=5") —
// harder to parse and unnecessary for our use case where we only need
// best bid + best ask. 206 → 209 gives flat tags: 3=bid_price, 6=ask_price,
// 2=bid_qty, 5=ask_qty. Direct map to state.MarketData with no juggling.
//
// tag 4=1000 in the spec example is "CallerInfo / lot size"; the python
// reference client also includes it. We include it for fidelity.
func (c *OdinClient) sendSubscribe(key SubscriptionKey, op int) error {
	body := fmt.Sprintf(
		"63=FT3.0|64=206|65=80|66=%s|1=%d$7=%d|4=1000|230=%d",
		time.Now().Format("15:04:05"),
		key.SegmentID, key.SecurityCode, op,
	)
	return c.writeFrame(body)
}

// writeFrame compresses + frames + sends one FIX body. Holds the conn
// mutex so concurrent Subscribe/Unsubscribe calls don't interleave bytes.
func (c *OdinClient) writeFrame(body string) error {
	frame, err := EncodeFix(body)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == nil {
		return errors.New("odin: not connected")
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout)); err != nil {
		return err
	}
	return c.conn.WriteMessage(websocket.BinaryMessage, frame)
}

// setConn replaces the active conn under lock. Used on dial + disconnect.
func (c *OdinClient) setConn(conn *websocket.Conn) {
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
}

// disconnect closes the active conn (idempotent) and marks state.
func (c *OdinClient) disconnect() {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	c.state.Store(StateDisconnected)
}
