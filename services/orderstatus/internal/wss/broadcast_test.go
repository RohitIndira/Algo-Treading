package wss

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"

	"github.com/RohitIndira/Algo-Treading/services/orderstatus/internal/store"
)

// ── fakes ────────────────────────────────────────────────────────────────

// fakeBroker speaks the broadcast protocol: token endpoint + broadcast socket.
type fakeBroker struct {
	srv *httptest.Server

	mu         sync.Mutex
	sessions   map[string]bool // bearer → valid
	au004      bool            // reject with HTTP 200 + AU004 body instead of HTTP 401
	tokens     map[string]string
	tokenCount int
	conns      []*websocket.Conn
	handshakes []string // raw handshake frames in order
	heartbeats []string
	rejectNext bool   // reply {"status":"Not Ok"} to the next handshake
	earlyFrame string // order frame pushed BEFORE the handshake reply
	dialStatus int    // non-zero: refuse the websocket upgrade with this HTTP status
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	fb := &fakeBroker{sessions: map[string]bool{}, tokens: map[string]string{}}
	up := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/order-notify/ws/createWsToken", func(w http.ResponseWriter, r *http.Request) {
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		fb.mu.Lock()
		defer fb.mu.Unlock()
		if !fb.sessions[bearer] {
			if fb.au004 {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"infoID":"AU004","infoMsg":"Session expired","data":{}}`))
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"status":"Not Ok","message":"unauthorized"}`))
			return
		}
		fb.tokenCount++
		tok := fmt.Sprintf("TOK-%d", fb.tokenCount)
		fb.tokens[tok] = bearer
		_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"Ok","result":[{"orderToken":"%s"}]}`, tok)))
	})
	mux.HandleFunc(indira.BroadcastWSPath, func(w http.ResponseWriter, r *http.Request) {
		fb.mu.Lock()
		ds := fb.dialStatus
		fb.mu.Unlock()
		if ds != 0 {
			w.WriteHeader(ds)
			return
		}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_, msg, err := c.ReadMessage()
		if err != nil {
			c.Close()
			return
		}
		var hs struct{ UserId, OrderToken string }
		_ = json.Unmarshal(msg, &hs)
		fb.mu.Lock()
		fb.handshakes = append(fb.handshakes, string(msg))
		_, tokOK := fb.tokens[hs.OrderToken]
		reject := fb.rejectNext || !tokOK
		fb.rejectNext = false
		fb.mu.Unlock()
		if reject {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"Not Ok"}`))
			c.Close()
			return
		}
		fb.mu.Lock()
		early := fb.earlyFrame
		fb.mu.Unlock()
		if early != "" {
			_ = c.WriteMessage(websocket.TextMessage, []byte(early))
		}
		// Reply first, register after: from here on only send/dropAll (under
		// fb.mu) write on this conn, so writers never overlap.
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"Ok"}`))
		fb.mu.Lock()
		fb.conns = append(fb.conns, c)
		fb.mu.Unlock()
		for {
			_, in, err := c.ReadMessage()
			if err != nil {
				return
			}
			var hb struct{ Heartbeat string }
			if json.Unmarshal(in, &hb) == nil && hb.Heartbeat != "" {
				fb.mu.Lock()
				fb.heartbeats = append(fb.heartbeats, string(in))
				fb.mu.Unlock()
			}
		}
	})
	fb.srv = httptest.NewServer(mux)
	t.Cleanup(fb.srv.Close)
	return fb
}

func (fb *fakeBroker) allow(bearer string) { fb.mu.Lock(); fb.sessions[bearer] = true; fb.mu.Unlock() }
func (fb *fakeBroker) revoke(bearer string) {
	fb.mu.Lock()
	delete(fb.sessions, bearer)
	fb.mu.Unlock()
}
func (fb *fakeBroker) connCount() int    { fb.mu.Lock(); defer fb.mu.Unlock(); return len(fb.conns) }
func (fb *fakeBroker) tokensIssued() int { fb.mu.Lock(); defer fb.mu.Unlock(); return fb.tokenCount }
func (fb *fakeBroker) heartbeatCount() int {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return len(fb.heartbeats)
}
func (fb *fakeBroker) lastHandshake() string {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return fb.handshakes[len(fb.handshakes)-1]
}
func (fb *fakeBroker) tokenOwner(tok string) string {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return fb.tokens[tok]
}

// send pushes one raw frame on every live connection.
func (fb *fakeBroker) send(raw string) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	for _, c := range fb.conns {
		_ = c.WriteMessage(websocket.TextMessage, []byte(raw))
	}
}

// dropAll closes every live connection server-side (idle close / outage).
func (fb *fakeBroker) dropAll() {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	for _, c := range fb.conns {
		_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "idle"))
		c.Close()
	}
	fb.conns = nil
}

// fakeJWT builds an unsigned JWT-shaped token with iat/exp claims.
func fakeJWT(user string, iat, exp time.Time) string {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS512"}`))
	claims := map[string]any{"sub": user, "iat": iat.Unix()}
	if !exp.IsZero() {
		claims["exp"] = exp.Unix()
	}
	body, _ := json.Marshal(claims)
	return hdr + "." + base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString([]byte("sig"))
}

// fakeUsers is the user-config stand-in.
type fakeUsers struct {
	mu    sync.Mutex
	users []string
	creds map[string]*indira.AuthContext
}

func (f *fakeUsers) ListActiveUserIDs(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.users...), nil
}

func (f *fakeUsers) FetchUserCredentials(_ context.Context, id string) (*indira.AuthContext, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creds[id], nil
}

func (f *fakeUsers) set(id, bearer string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creds[id] = &indira.AuthContext{UserId: id, BearerToken: bearer, Source: "WEB"}
}

// memWriter is an in-memory broker_events with UNIQUE (broker_order_id, event_seq).
type memWriter struct {
	mu   sync.Mutex
	rows []*store.Event
	seen map[string]time.Time
	src  map[string]store.Source
}

func newMemWriter() *memWriter {
	return &memWriter{seen: map[string]time.Time{}, src: map[string]store.Source{}}
}

func (m *memWriter) key(id string, seq int64) string { return fmt.Sprintf("%s|%d", id, seq) }

func (m *memWriter) Insert(_ context.Context, ev *store.Event) (int64, bool, error) {
	if ev.EventSeq == 0 || ev.BrokerOrderID == "" || ev.RawPayload == nil {
		return 0, false, errors.New("missing required fields")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(ev.BrokerOrderID, ev.EventSeq)
	if _, dup := m.seen[k]; dup {
		return 0, false, nil
	}
	m.seen[k] = time.Now()
	m.src[k] = ev.Source
	cp := *ev
	m.rows = append(m.rows, &cp)
	return int64(len(m.rows)), true, nil
}

func (m *memWriter) MarkPublished(context.Context, int64) error { return nil }

func (m *memWriter) LookupObserved(_ context.Context, id string, seq int64) (*store.ObservedRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(id, seq)
	at, ok := m.seen[k]
	if !ok {
		return nil, nil
	}
	return &store.ObservedRow{Source: m.src[k], ObservedAt: at}, nil
}

func (m *memWriter) LookupEquivalent(_ context.Context, key store.EquivalentKey, within time.Duration) (*store.EquivalentRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var best *store.EquivalentRow
	for _, r := range m.rows {
		at := m.seen[m.key(r.BrokerOrderID, r.EventSeq)]
		if time.Since(at) > within {
			continue
		}
		var raw struct{ LastModifiedTimeStamp, OrderTimeStamp string }
		_ = json.Unmarshal(r.RawPayload, &raw)
		if r.BrokerOrderID == key.BrokerOrderID && r.Status == key.Status && r.FilledQty == key.FilledQty &&
			r.TradedPrice == key.TradedPrice && raw.LastModifiedTimeStamp == key.LastModifiedTimeStamp && raw.OrderTimeStamp == key.OrderTimeStamp {
			if best == nil || at.After(best.ObservedAt) {
				best = &store.EquivalentRow{Source: r.Source, EventSeq: r.EventSeq, ObservedAt: at}
			}
		}
	}
	return best, nil
}

func (m *memWriter) count() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.rows) }
func (m *memWriter) all() []*store.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*store.Event(nil), m.rows...)
}

// fakeOwn is the trade-execution lookup stand-in.
type fakeOwn struct{ ids map[string]bool }

func (f fakeOwn) IsOurOrder(_ context.Context, id string) (bool, error) { return f.ids[id], nil }

// fakeAlerter records alerts.
type fakeAlerter struct {
	mu    sync.Mutex
	kinds []string
}

func (a *fakeAlerter) Alert(_ context.Context, kind, user, _ string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kinds = append(a.kinds, kind+":"+user)
}
func (a *fakeAlerter) list() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.kinds...)
}

// fakeClock: After registers a waiter; Advance fires those that are due.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
}
type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.waiters = append(c.waiters, fakeWaiter{at: c.now.Add(d), ch: ch})
	return ch
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var keep []fakeWaiter
	for _, w := range c.waiters {
		if !w.at.After(c.now) {
			w.ch <- c.now
		} else {
			keep = append(keep, w)
		}
	}
	c.waiters = keep
	c.mu.Unlock()
}
func (c *fakeClock) waiting() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.waiters) }

// ── harness ──────────────────────────────────────────────────────────────

type harness struct {
	fb     *fakeBroker
	users  *fakeUsers
	mem    *memWriter
	alerts *fakeAlerter
	bl     *BroadcastListener
	sink   *Listener
	cancel context.CancelFunc
}

const (
	userS4450 = "S4450"
	userFIV99 = "FIV99"
)

func newHarness(t *testing.T, mode Mode, cfgMut func(*BroadcastConfig), own OwnOrderLookup) *harness {
	t.Helper()
	return newHarnessWith(t, mode, cfgMut, own, nil)
}

func newHarnessWith(t *testing.T, mode Mode, cfgMut func(*BroadcastConfig), own OwnOrderLookup, fbMut func(*fakeBroker)) *harness {
	t.Helper()
	fb := newFakeBroker(t)
	if fbMut != nil {
		fbMut(fb)
	}
	now := time.Now()
	users := &fakeUsers{users: []string{userS4450, userFIV99}, creds: map[string]*indira.AuthContext{}}
	tokS := fakeJWT(userS4450, now.Add(-2*time.Hour), now.Add(10*time.Hour))
	tokF := fakeJWT(userFIV99, now.Add(-1*time.Hour), now.Add(10*time.Hour)) // newest login
	users.set(userS4450, tokS)
	users.set(userFIV99, tokF)
	fb.allow(tokS)
	fb.allow(tokF)

	logger := zap.NewNop()
	client := indira.NewClient(indira.Config{BaseURL: fb.srv.URL, Timeout: 5 * time.Second})
	mem := newMemWriter()
	sink := NewListener(client, mem, nil, logger)
	cfg := BroadcastConfig{
		Mode:              mode,
		HandshakeTimeout:  2 * time.Second,
		HeartbeatInterval: time.Hour, // real clock tests: never fires
		MaxSilence:        5 * time.Second,
		Backoff:           []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
		NoAnchorBackoff:   30 * time.Millisecond,
		PingInterval:      time.Hour,
		ShadowWindow:      50 * time.Millisecond,
		StaleCheckEvery:   time.Hour,
		ManagedRefresh:    time.Hour,
		MarketOpen:        func(time.Time) bool { return false },
	}
	if cfgMut != nil {
		cfgMut(&cfg)
	}
	alerts := &fakeAlerter{}
	bl := NewBroadcastListener(cfg, client, sink, users, own, mem, alerts, logger)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); bl.Wait() })
	h := &harness{fb: fb, users: users, mem: mem, alerts: alerts, bl: bl, sink: sink, cancel: cancel}
	bl.Start(ctx)
	return h
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func frame(ucc, id, status string, seq int) string {
	return fmt.Sprintf(`{"UCC":"%s","Symbol":"KEI","Buy_Sell":"2","OrderStatus":"%s","OrderOriginalQty":10,"TradedQTY":"0","TradedPrice":"101.5","UniqueCode":"%s","AMOOrderID":"","MessageSequenceNumber":%d,"OrderSequenceNumber":7,"MessageType":"ORD_NRML"}`,
		ucc, status, id, seq)
}

// ── stage 1 / 2 tests ───────────────────────────────────────────────────

// 1.1 exact handshake / heartbeat text.
func TestHandshakeAndHeartbeatFrames(t *testing.T) {
	if got := string(HandshakeFrame("S4450", "T1")); got != `{"userId":"S4450","orderToken":"T1"}` {
		t.Fatalf("handshake frame = %s", got)
	}
	if got := string(HeartbeatFrame("S4450")); got != `{"userId":"S4450","heartbeat":"h"}` {
		t.Fatalf("heartbeat frame = %s", got)
	}
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]Mode{"": ModeShadow, "off": ModeOff, "SHADOW": ModeShadow, "own_orders": ModeOwnOrders, "all": ModeAll} {
		got, err := ParseMode(in)
		if err != nil || got != want {
			t.Fatalf("ParseMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if got, err := ParseMode("Shadow   # compare only"); err != nil || got != ModeShadow {
		t.Fatalf("ParseMode with trailing text = %q, %v", got, err)
	}
	if _, err := ParseMode("maybe"); err == nil {
		t.Fatal("ParseMode(maybe) should error")
	}
}

// 2.1 happy path + 1.3 routing + 2.9 unknown UCC counted only.
func TestHappyPathRoutingAndUnknownUCC(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	if got := h.bl.CurrentAnchor(); got != userFIV99 {
		t.Fatalf("anchor = %s, want newest session %s", got, userFIV99)
	}
	// Handshake used the token issued to the anchor's bearer.
	var hs struct{ UserId, OrderToken string }
	_ = json.Unmarshal([]byte(h.fb.lastHandshake()), &hs)
	if hs.UserId != userFIV99 || h.fb.tokenOwner(hs.OrderToken) == "" {
		t.Fatalf("handshake %+v not bound to the anchor's session", hs)
	}

	h.fb.send(frame("S4450", "ORD-1", "PENDING", 101))
	h.fb.send(frame("ZZZ99", "ORD-2", "EXECUTED", 102)) // not managed
	h.fb.send(frame("S4450", "ORD-3", "EXECUTED", 103))
	waitFor(t, "2 rows", func() bool { return h.mem.count() == 2 })
	time.Sleep(20 * time.Millisecond)
	if h.mem.count() != 2 {
		t.Fatalf("rows = %d, want 2 (unknown UCC must not be stored)", h.mem.count())
	}
	rows := h.mem.all()
	for _, r := range rows {
		if r.Source != store.SourceWSSBroadcast || r.UserID != "S4450" {
			t.Fatalf("row %+v: want source WSS_BROADCAST for user S4450", r)
		}
	}
	if rows[1].EventType != store.EventFilled || rows[1].TradedPrice != 101.5 {
		t.Fatalf("EXECUTED frame → %s @ %v, want FILLED @ 101.5", rows[1].EventType, rows[1].TradedPrice)
	}
	if h.bl.Stats().Unknown.Load() != 1 || h.bl.Stats().Frames.Load() != 3 {
		t.Fatalf("unknown=%d frames=%d", h.bl.Stats().Unknown.Load(), h.bl.Stats().Frames.Load())
	}
}

// 1.4 same frame via per-user socket and broadcast → one stored row.
// 2.10 replay twice → idempotent.
func TestDedupeWithPerUserPathAndReplay(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)

	var ws indira.WSOrderStatus
	_ = json.Unmarshal([]byte(frame("S4450", "ORD-9", "EXECUTED", 900)), &ws)
	h.sink.handleWSEvent(context.Background(), &ws) // per-user socket wins
	if h.mem.count() != 1 || h.mem.all()[0].Source != store.SourceWSS {
		t.Fatalf("per-user insert failed: %d rows", h.mem.count())
	}

	h.fb.send(frame("S4450", "ORD-9", "EXECUTED", 900)) // same event on broadcast
	h.fb.send(frame("S4450", "ORD-9", "EXECUTED", 900)) // and replayed
	waitFor(t, "2 deduped", func() bool { return h.bl.Stats().Deduped.Load() == 2 })
	if h.mem.count() != 1 {
		t.Fatalf("rows = %d, want 1", h.mem.count())
	}
	// Broadcast first, then per-user: still one row.
	h.fb.send(frame("S4450", "ORD-10", "PENDING", 1000))
	waitFor(t, "ORD-10 inserted", func() bool { return h.mem.count() == 2 })
	_ = json.Unmarshal([]byte(frame("S4450", "ORD-10", "PENDING", 1000)), &ws)
	h.sink.handleWSEvent(context.Background(), &ws)
	if h.mem.count() != 2 {
		t.Fatalf("rows = %d after per-user replay, want 2", h.mem.count())
	}
}

// 1.5 out-of-order sequences are both kept.
func TestOutOfOrderSequencesKept(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.fb.send(frame("S4450", "ORD-5", "EXECUTED", 5))
	h.fb.send(frame("S4450", "ORD-5", "PENDING", 4))
	waitFor(t, "2 rows", func() bool { return h.mem.count() == 2 })
	rows := h.mem.all()
	if rows[0].EventSeq != 5 || rows[1].EventSeq != 4 {
		t.Fatalf("seqs = %d,%d; want 5,4 stored as received", rows[0].EventSeq, rows[1].EventSeq)
	}
}

// 1.6 / 1.7 AMO frame: both ids preserved in the raw payload; FIX 7 fill qty.
func TestAMOFramePreservesBothIDsAndFillQty(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.fb.send(`{"UCC":"FIV99","Symbol":"IOLCP","Buy_Sell":"1","OrderStatus":"AMO SUBMITTED","OrderOriginalQty":25,"UniqueCode":"NEW-ID-1","AMOOrderID":"AMO-77","MessageSequenceNumber":"5001"}`)
	h.fb.send(`{"UCC":"FIV99","Symbol":"IOLCP","Buy_Sell":"1","OrderStatus":"EXECUTED","MessageType":"TRD_MSG","OrderOriginalQty":25,"TradeQty":"10","QuantityTradedToday":"10","TradedQTY":"0","TradedPrice":"88.25","UniqueCode":"NEW-ID-1","AMOOrderID":"AMO-77","MessageSequenceNumber":"5002"}`)
	waitFor(t, "2 rows", func() bool { return h.mem.count() == 2 })
	rows := h.mem.all()
	amo := rows[0]
	if amo.BrokerOrderID != "NEW-ID-1" || amo.Status != "AMO SUBMITTED" || amo.EventSeq != 5001 {
		t.Fatalf("amo row = %+v", amo)
	}
	var raw map[string]any
	_ = json.Unmarshal(amo.RawPayload, &raw)
	if raw["AMOOrderID"] != "AMO-77" || raw["UniqueCode"] != "NEW-ID-1" {
		t.Fatalf("raw payload lost ids: %v / %v", raw["AMOOrderID"], raw["UniqueCode"])
	}
	fill := rows[1]
	if fill.FilledQty != 10 || fill.TradedPrice != 88.25 || fill.Quantity != 25 {
		t.Fatalf("fill row filled=%d price=%v qty=%d; want 10 / 88.25 / 25", fill.FilledQty, fill.TradedPrice, fill.Quantity)
	}
}

// 1.11 malformed frame is skipped, connection stays up.
func TestMalformedFrameSkipped(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.fb.send(`this is not json`)
	h.fb.send(`{"status":"notice"}`) // control frame: no UCC
	h.fb.send(frame("S4450", "ORD-M", "PENDING", 77))
	waitFor(t, "row after garbage", func() bool { return h.mem.count() == 1 })
	s := h.bl.Stats()
	if s.Malformed.Load() != 1 || s.Control.Load() != 1 || !h.bl.Connected() {
		t.Fatalf("malformed=%d control=%d connected=%v", s.Malformed.Load(), s.Control.Load(), h.bl.Connected())
	}
	if h.fb.connCount() != 1 {
		t.Fatalf("connections = %d, want the original one only", h.fb.connCount())
	}
}

// 1.8 / 2.6 server close → reconnect with a NEW token; frames resume.
func TestReconnectAfterServerCloseWithNewToken(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	if h.fb.tokensIssued() != 1 {
		t.Fatalf("tokens = %d", h.fb.tokensIssued())
	}
	h.fb.dropAll()
	waitFor(t, "reconnect", func() bool { return h.fb.connCount() == 1 && h.fb.tokensIssued() == 2 && h.bl.Connected() })
	if h.bl.Stats().Reconnects.Load() < 1 {
		t.Fatal("reconnect not counted")
	}
	h.fb.send(frame("S4450", "ORD-R", "PENDING", 1))
	waitFor(t, "row after reconnect", func() bool { return h.mem.count() == 1 })
}

// 1.8 backoff schedule is honoured (no busy loop) when the broker is down.
func TestBackoffNoBusyLoop(t *testing.T) {
	var calls atomic.Int32
	h := newHarness(t, ModeAll, func(c *BroadcastConfig) {
		c.Backoff = []time.Duration{30 * time.Millisecond, 60 * time.Millisecond}
		c.NoAnchorBackoff = 60 * time.Millisecond
	}, nil)
	waitFor(t, "connect", h.bl.Connected)
	// Point the listener at a dead broker by revoking every session: token
	// endpoint returns 401 each time → every attempt fails fast.
	h.fb.mu.Lock()
	h.fb.sessions = map[string]bool{}
	h.fb.mu.Unlock()
	h.fb.dropAll()
	start := time.Now()
	waitFor(t, "a few attempts", func() bool {
		calls.Store(int32(h.bl.Stats().Reconnects.Load()))
		return calls.Load() >= 4
	})
	// 4 attempts need at least 30+60+60 ms of backoff.
	if el := time.Since(start); el < 120*time.Millisecond {
		t.Fatalf("4 attempts in %s — backoff not applied", el)
	}
}

// 1.9 / 2.7 token 401 → anchor marked dead, next candidate used, one alert.
func TestTokenAuthFailureFallsBackToNextAnchor(t *testing.T) {
	for _, au004 := range []bool{false, true} {
		t.Run(fmt.Sprintf("au004=%v", au004), func(t *testing.T) {
			h := newHarness(t, ModeAll, func(c *BroadcastConfig) {
				c.AnchorUser = "DEALER1"
				c.MarketOpen = func(time.Time) bool { return true }
			}, nil)
			waitFor(t, "initial connect (DEALER1 has no creds yet → fallback)", h.bl.Connected)
			// Configured dealer login exists but its session is dead at the broker.
			h.fb.mu.Lock()
			h.fb.au004 = au004
			h.fb.mu.Unlock()
			// The dealer's creds were set after Start; the first attempt may
			// already have fallen back — either way the configured anchor
			// must be tried and retired, and the feed must come up on FIV99.
			dead := fakeJWT("DEALER1", time.Now(), time.Now().Add(time.Hour))
			h.users.set("DEALER1", dead)
			h.fb.dropAll()
			waitFor(t, "connected on fallback", func() bool { return h.bl.Connected() && h.bl.CurrentAnchor() == userFIV99 })
			waitFor(t, "anchor-lost alert", func() bool {
				for _, k := range h.alerts.list() {
					if k == "BROADCAST_ANCHOR_LOST:DEALER1" {
						return true
					}
				}
				return false
			})
			// Several more reconnects must not re-alert for the same dead anchor.
			h.fb.dropAll()
			waitFor(t, "reconnect 2", func() bool { return h.bl.Connected() && h.bl.Stats().Reconnects.Load() >= 2 })
			n := 0
			for _, k := range h.alerts.list() {
				if k == "BROADCAST_ANCHOR_LOST:DEALER1" {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("anchor-lost alerts = %d, want exactly 1", n)
			}
		})
	}
}

// 1.10 anchor chooser: configured first, else newest live; never expired.
func TestAnchorChooser(t *testing.T) {
	now := time.Now()
	users := &fakeUsers{users: []string{"OLD", "NEW", "EXPIRED", "NOCREDS"}, creds: map[string]*indira.AuthContext{}}
	users.set("OLD", fakeJWT("OLD", now.Add(-5*time.Hour), now.Add(time.Hour)))
	users.set("NEW", fakeJWT("NEW", now.Add(-1*time.Hour), now.Add(time.Hour)))
	users.set("EXPIRED", fakeJWT("EXPIRED", now.Add(-30*time.Minute), now.Add(-time.Minute))) // newest iat but expired
	logger := zap.NewNop()
	client := indira.NewClient(indira.Config{BaseURL: "http://127.0.0.1:1"})
	mk := func(anchor string) *BroadcastListener {
		return NewBroadcastListener(BroadcastConfig{Mode: ModeShadow, AnchorUser: anchor}, client, NewListener(client, newMemWriter(), nil, logger), users, nil, nil, &fakeAlerter{}, logger)
	}
	if got := mk("").pickAnchor(context.Background()); got == nil || got.UserId != "NEW" {
		t.Fatalf("pickAnchor = %v, want NEW (newest non-expired)", got)
	}
	if got := mk("OLD").pickAnchor(context.Background()); got == nil || got.UserId != "OLD" {
		t.Fatalf("configured anchor not preferred: %v", got)
	}
	if got := mk("EXPIRED").pickAnchor(context.Background()); got == nil || got.UserId != "NEW" {
		t.Fatalf("expired configured anchor must fall back to NEW: %v", got)
	}
	b := mk("")
	b.markAnchorDead(context.Background(), "NEW", errors.New("401"))
	if got := b.pickAnchor(context.Background()); got == nil || got.UserId != "OLD" {
		t.Fatalf("dead NEW should yield OLD: %v", got)
	}
	// A re-login (newer iat than the dead mark) revives the anchor.
	users.set("NEW", fakeJWT("NEW", now.Add(time.Minute), now.Add(2*time.Hour)))
	if got := b.pickAnchor(context.Background()); got == nil || got.UserId != "NEW" {
		t.Fatalf("re-logged-in NEW should be picked again: %v", got)
	}
	// Nothing usable at all.
	none := &fakeUsers{users: []string{"EXPIRED"}, creds: users.creds}
	b.users = none
	if got := b.pickAnchor(context.Background()); got != nil {
		t.Fatalf("expected nil anchor, got %v", got)
	}
}

// Handshake rejected ({"status":"Not Ok"}) also retires the anchor.
func TestHandshakeRejectedRetiresAnchor(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.fb.mu.Lock()
	h.fb.rejectNext = true
	h.fb.mu.Unlock()
	h.fb.dropAll()
	waitFor(t, "fallback to S4450", func() bool { return h.bl.Connected() && h.bl.CurrentAnchor() == userS4450 })
	if h.bl.Stats().AnchorSwitches.Load() != 1 {
		t.Fatalf("anchor switches = %d", h.bl.Stats().AnchorSwitches.Load())
	}
}

// Shadow mode: writes nothing; recall counted against the per-user path.
func TestShadowModeWritesNothingAndCountsRecall(t *testing.T) {
	h := newHarness(t, ModeShadow, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	// Per-user path already stored ORD-A; ORD-B is broadcast-only.
	var ws indira.WSOrderStatus
	_ = json.Unmarshal([]byte(frame("S4450", "ORD-A", "EXECUTED", 11)), &ws)
	h.sink.handleWSEvent(context.Background(), &ws)

	h.fb.send(frame("S4450", "ORD-A", "EXECUTED", 11))
	h.fb.send(frame("S4450", "ORD-B", "EXECUTED", 12))
	h.fb.send(frame("ZZZ", "ORD-C", "EXECUTED", 13))
	waitFor(t, "shadow compared", func() bool {
		return h.bl.Stats().ShadowSeen.Load() == 1 && h.bl.Stats().ShadowMissed.Load() == 1
	})
	if h.mem.count() != 1 {
		t.Fatalf("shadow wrote: rows = %d, want 1 (the per-user row only)", h.mem.count())
	}
	if h.bl.Stats().Inserted.Load() != 0 || h.bl.Stats().Unknown.Load() != 1 {
		t.Fatalf("inserted=%d unknown=%d", h.bl.Stats().Inserted.Load(), h.bl.Stats().Unknown.Load())
	}
}

// own_orders: only frames whose UniqueCode or AMOOrderID is ours are written.
func TestOwnOrdersFilters(t *testing.T) {
	h := newHarness(t, ModeOwnOrders, nil, fakeOwn{ids: map[string]bool{"OURS-1": true, "AMO-9": true}})
	waitFor(t, "connect", h.bl.Connected)
	h.fb.send(frame("S4450", "OURS-1", "EXECUTED", 1))
	h.fb.send(frame("S4450", "MANUAL-2", "EXECUTED", 2))
	h.fb.send(`{"UCC":"S4450","OrderStatus":"PENDING","UniqueCode":"CONV-3","AMOOrderID":"AMO-9","MessageSequenceNumber":3,"OrderOriginalQty":5}`)
	waitFor(t, "2 rows", func() bool { return h.mem.count() == 2 })
	waitFor(t, "1 skipped", func() bool { return h.bl.Stats().SkippedNotOwn.Load() == 1 })
	ids := map[string]bool{}
	for _, r := range h.mem.all() {
		ids[r.BrokerOrderID] = true
	}
	if !ids["OURS-1"] || !ids["CONV-3"] || ids["MANUAL-2"] {
		t.Fatalf("stored ids = %v", ids)
	}
}

// own_orders with no lookup wired fails closed.
func TestOwnOrdersWithoutLookupFailsClosed(t *testing.T) {
	h := newHarness(t, ModeOwnOrders, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.fb.send(frame("S4450", "OURS-1", "EXECUTED", 1))
	waitFor(t, "skipped", func() bool { return h.bl.Stats().SkippedNotOwn.Load() == 1 })
	if h.mem.count() != 0 {
		t.Fatal("wrote without a lookup")
	}
}

// Mode off never starts.
func TestModeOffDoesNotStart(t *testing.T) {
	fb := newFakeBroker(t)
	client := indira.NewClient(indira.Config{BaseURL: fb.srv.URL})
	logger := zap.NewNop()
	bl := NewBroadcastListener(BroadcastConfig{Mode: ModeOff}, client, NewListener(client, newMemWriter(), nil, logger),
		&fakeUsers{creds: map[string]*indira.AuthContext{}}, nil, nil, nil, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bl.Start(ctx)
	bl.Wait()
	if fb.tokensIssued() != 0 || bl.Connected() {
		t.Fatal("mode off touched the broker")
	}
}

// 1.2 heartbeat cadence with an injected clock: fires at 50 s, 100 s; stops on close.
func TestHeartbeatCadence(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, ModeShadow, func(c *BroadcastConfig) {
		c.Clock = clk
		c.HeartbeatInterval = 50 * time.Second
	}, nil)
	waitFor(t, "connect", h.bl.Connected)
	// Waiters registered: heartbeat + managed refresh + stale check (+ none for shadow yet).
	waitFor(t, "heartbeat timer armed", func() bool { return clk.waiting() >= 3 })
	clk.Advance(49 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if h.fb.heartbeatCount() != 0 {
		t.Fatal("heartbeat fired before 50 s")
	}
	clk.Advance(1 * time.Second)
	waitFor(t, "first heartbeat", func() bool { return h.fb.heartbeatCount() == 1 })
	if hb := h.fb.heartbeats[0]; hb != `{"userId":"FIV99","heartbeat":"h"}` {
		t.Fatalf("heartbeat text = %s", hb)
	}
	waitFor(t, "timer re-armed", func() bool { return clk.waiting() >= 3 })
	clk.Advance(50 * time.Second)
	waitFor(t, "second heartbeat", func() bool { return h.fb.heartbeatCount() == 2 })
	// Stop: no heartbeat after close.
	h.cancel()
	h.bl.Wait()
	clk.Advance(50 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if h.fb.heartbeatCount() != 2 {
		t.Fatalf("heartbeat after close: %d", h.fb.heartbeatCount())
	}
}

// 2.8 feed stale in market hours → one alert, re-armed when frames resume.
func TestFeedStaleAlertOnce(t *testing.T) {
	h := newHarness(t, ModeShadow, func(c *BroadcastConfig) {
		c.StaleAfter = 30 * time.Millisecond
		c.StaleCheckEvery = 10 * time.Millisecond
		c.MarketOpen = func(time.Time) bool { return true }
	}, nil)
	waitFor(t, "connect", h.bl.Connected)
	waitFor(t, "stale alert", func() bool { return h.bl.Stats().StaleAlerts.Load() == 1 })
	time.Sleep(60 * time.Millisecond)
	if h.bl.Stats().StaleAlerts.Load() != 1 {
		t.Fatalf("stale alerts = %d, want exactly 1 while still stale", h.bl.Stats().StaleAlerts.Load())
	}
	found := false
	for _, k := range h.alerts.list() {
		if strings.HasPrefix(k, "BROADCAST_FEED_STALE:") {
			found = true
		}
	}
	if !found {
		t.Fatalf("alerts = %v", h.alerts.list())
	}
	// Frames resume → re-armed → goes stale again → second alert.
	h.fb.send(frame("S4450", "ORD-S", "PENDING", 1))
	waitFor(t, "second stale alert after resume", func() bool { return h.bl.Stats().StaleAlerts.Load() == 2 })
}

// Clean shutdown sends a close frame and stops every goroutine.
func TestCleanShutdown(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.cancel()
	done := make(chan struct{})
	go func() { h.bl.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not stop")
	}
	if h.bl.Connected() {
		t.Fatal("still marked connected")
	}
}

// sessionTimes reads iat/exp; garbage → zero.
func TestSessionTimes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	iat, exp := sessionTimes(fakeJWT("x", now, now.Add(time.Hour)))
	if !iat.Equal(now) || !exp.Equal(now.Add(time.Hour)) {
		t.Fatalf("iat=%v exp=%v", iat, exp)
	}
	if iat, exp := sessionTimes("not-a-jwt"); !iat.IsZero() || !exp.IsZero() {
		t.Fatal("garbage should give zero times")
	}
}

// frameTS is frame() plus broker timestamps and a per-trade fill (for the
// content-equivalence guard).
func frameTS(ucc, id, status string, seq int, filled int, price string) string {
	return fmt.Sprintf(`{"UCC":"%s","Symbol":"KEI","Buy_Sell":"2","OrderStatus":"%s","OrderOriginalQty":10,"TradeQty":"%d","TradedQTY":"0","TradedPrice":"%s","UniqueCode":"%s","MessageSequenceNumber":%d,"LastModifiedTimeStamp":"26-Oct-2026 10:01:02","OrderTimeStamp":"26-Oct-2026 10:00:00"}`,
		ucc, status, filled, price, id, seq)
}

// Review item 1: the same event numbered differently on the two sockets must
// not be written twice — content equivalence within the window skips it.
func TestEquivalentEventUnderDifferentSeqIsSkipped(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	var ws indira.WSOrderStatus
	_ = json.Unmarshal([]byte(frameTS("S4450", "ORD-EQ", "EXECUTED", 500, 10, "101.5")), &ws)
	h.sink.handleWSEvent(context.Background(), &ws) // per-user socket: seq 500

	h.fb.send(frameTS("S4450", "ORD-EQ", "EXECUTED", 501, 10, "101.5")) // same event, seq 501
	waitFor(t, "equivalent skip", func() bool { return h.bl.Stats().EquivalentSkipped.Load() == 1 })
	if h.mem.count() != 1 {
		t.Fatalf("rows = %d, want 1 — manual sell would be booked twice", h.mem.count())
	}
	// A genuinely different event of the same order (second partial fill at
	// another price) is still written.
	h.fb.send(frameTS("S4450", "ORD-EQ", "EXECUTED", 502, 10, "101.7"))
	waitFor(t, "distinct event stored", func() bool { return h.mem.count() == 2 })
	// And the identical seq path still dedupes via the primary key.
	h.fb.send(frameTS("S4450", "ORD-EQ", "EXECUTED", 500, 10, "101.5"))
	waitFor(t, "primary dedup", func() bool { return h.bl.Stats().Deduped.Load() == 1 })
	if h.mem.count() != 2 || h.bl.Stats().EquivalentSkipped.Load() != 1 {
		t.Fatalf("rows=%d equivalent_skipped=%d", h.mem.count(), h.bl.Stats().EquivalentSkipped.Load())
	}
}

// Review item 1 (shadow): per matched event, record whether the seqs agree.
func TestShadowSeqMatchCounters(t *testing.T) {
	h := newHarness(t, ModeShadow, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	var ws indira.WSOrderStatus
	_ = json.Unmarshal([]byte(frameTS("S4450", "ORD-S1", "EXECUTED", 600, 10, "101.5")), &ws)
	h.sink.handleWSEvent(context.Background(), &ws)
	_ = json.Unmarshal([]byte(frameTS("S4450", "ORD-S2", "EXECUTED", 700, 10, "101.5")), &ws)
	h.sink.handleWSEvent(context.Background(), &ws)

	h.fb.send(frameTS("S4450", "ORD-S1", "EXECUTED", 600, 10, "101.5")) // same seq
	h.fb.send(frameTS("S4450", "ORD-S2", "EXECUTED", 701, 10, "101.5")) // same event, other seq
	h.fb.send(frameTS("S4450", "ORD-S3", "EXECUTED", 800, 10, "101.5")) // broadcast only
	waitFor(t, "3 comparisons", func() bool {
		s := h.bl.Stats()
		return s.SeqMatchSeq.Load()+s.SeqMatchEquivalentOnly.Load()+s.SeqMatchNone.Load() == 3
	})
	s := h.bl.Stats()
	if s.SeqMatchSeq.Load() != 1 || s.SeqMatchEquivalentOnly.Load() != 1 || s.SeqMatchNone.Load() != 1 {
		t.Fatalf("seq=%d equivalent_only=%d none=%d", s.SeqMatchSeq.Load(), s.SeqMatchEquivalentOnly.Load(), s.SeqMatchNone.Load())
	}
	if s.ShadowSeen.Load() != 2 || s.ShadowMissed.Load() != 1 || h.mem.count() != 2 {
		t.Fatalf("seen=%d missed=%d rows=%d", s.ShadowSeen.Load(), s.ShadowMissed.Load(), h.mem.count())
	}
}

// Review item 2: a type mismatch on a non-routing field keeps the frame.
func TestPartialParseKeepsFrame(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.fb.send(`{"UCC":"S4450","Symbol":"KEI","Buy_Sell":"2","OrderStatus":"EXECUTED","TriggerPrice":"0.00","OrderOriginalQty":"25","TradeQty":"5","TradedPrice":"99.5","UniqueCode":"ORD-P","MessageSequenceNumber":"31"}`)
	waitFor(t, "row stored", func() bool { return h.mem.count() == 1 })
	s := h.bl.Stats()
	if s.Partial.Load() != 1 || s.Malformed.Load() != 0 {
		t.Fatalf("partial=%d malformed=%d", s.Partial.Load(), s.Malformed.Load())
	}
	r := h.mem.all()[0]
	if r.BrokerOrderID != "ORD-P" || r.EventSeq != 31 || r.FilledQty != 5 || r.TradedPrice != 99.5 || r.Status != "EXECUTED" {
		t.Fatalf("row = %+v", r)
	}
	// Routing key lost → still malformed.
	h.fb.send(`{"UCC":"S4450","OrderStatus":"EXECUTED","UniqueCode":12345,"MessageSequenceNumber":32}`)
	waitFor(t, "malformed counted", func() bool { return h.bl.Stats().Malformed.Load() == 1 })
}

// Review item 4: dead-anchor alert once per session; a TTL retry does not
// re-fire; a re-login re-arms; nothing fires outside market hours.
func TestDeadAnchorAlertSemantics(t *testing.T) {
	clk := newFakeClock()
	open := true
	now := clk.Now()
	users := &fakeUsers{users: []string{"A"}, creds: map[string]*indira.AuthContext{}}
	users.set("A", fakeJWT("A", now.Add(-time.Hour), now.Add(24*time.Hour)))
	logger := zap.NewNop()
	client := indira.NewClient(indira.Config{BaseURL: "http://127.0.0.1:1"})
	alerts := &fakeAlerter{}
	b := NewBroadcastListener(BroadcastConfig{Mode: ModeShadow, Clock: clk, DeadAnchorTTL: 30 * time.Minute,
		MarketOpen: func(time.Time) bool { return open }},
		client, NewListener(client, newMemWriter(), nil, logger), users, nil, nil, alerts, logger)
	ctx := context.Background()
	count := func() int {
		n := 0
		for _, k := range alerts.list() {
			if k == "BROADCAST_ANCHOR_LOST:A" {
				n++
			}
		}
		return n
	}

	b.markAnchorDead(ctx, "A", errors.New("401"))
	if count() != 1 || b.pickAnchor(ctx) != nil {
		t.Fatalf("first death: alerts=%d, anchor=%v", count(), b.pickAnchor(ctx))
	}
	// TTL passes: the session is retried, but rejected again → no new alert.
	clk.Advance(31 * time.Minute)
	if a := b.pickAnchor(ctx); a == nil || a.UserId != "A" {
		t.Fatal("TTL retry should offer A again")
	}
	b.markAnchorDead(ctx, "A", errors.New("401 again"))
	if count() != 1 {
		t.Fatalf("TTL retry re-fired the alert: %d", count())
	}
	// Re-login (newer iat) → eligible again, alert re-armed.
	users.set("A", fakeJWT("A", clk.Now().Add(time.Second), clk.Now().Add(24*time.Hour)))
	if a := b.pickAnchor(ctx); a == nil {
		t.Fatal("re-login should revive A")
	}
	b.markAnchorDead(ctx, "A", errors.New("401 after relogin"))
	if count() != 2 {
		t.Fatalf("re-login death should alert again: %d", count())
	}
	// Market closed: no alert, but the death is recorded and stays pending.
	open = false
	users.set("A", fakeJWT("A", clk.Now().Add(time.Minute), clk.Now().Add(24*time.Hour)))
	_ = b.pickAnchor(ctx)
	b.markAnchorDead(ctx, "A", errors.New("401 at night"))
	if count() != 2 {
		t.Fatalf("alert outside market hours: %d", count())
	}
	// ...and fires on the next rejection once the market is open (same token).
	open = true
	clk.Advance(31 * time.Minute)
	_ = b.pickAnchor(ctx)
	b.markAnchorDead(ctx, "A", errors.New("401 at open"))
	if count() != 3 {
		t.Fatalf("pending night death should alert at open: %d", count())
	}
}

// Review item 8: an order frame delivered before the handshake reply is data,
// not a rejection.
func TestEarlyOrderFrameBeforeHandshakeReply(t *testing.T) {
	h := newHarnessWith(t, ModeAll, nil, nil, func(fb *fakeBroker) {
		fb.earlyFrame = frame("S4450", "ORD-EARLY", "PENDING", 1)
	})
	waitFor(t, "connect", h.bl.Connected)
	waitFor(t, "early frame stored", func() bool { return h.mem.count() == 1 })
	if h.bl.Stats().AnchorSwitches.Load() != 0 || h.bl.CurrentAnchor() != userFIV99 {
		t.Fatalf("anchor retired on an early order frame: switches=%d anchor=%s", h.bl.Stats().AnchorSwitches.Load(), h.bl.CurrentAnchor())
	}
}

// Review item 9: an HTTP 401 on the websocket upgrade retires the anchor.
func TestDialUnauthorizedRetiresAnchor(t *testing.T) {
	h := newHarness(t, ModeAll, nil, nil)
	waitFor(t, "connect", h.bl.Connected)
	h.fb.mu.Lock()
	h.fb.dialStatus = http.StatusUnauthorized
	h.fb.mu.Unlock()
	h.fb.dropAll()
	waitFor(t, "anchor switch", func() bool { return h.bl.Stats().AnchorSwitches.Load() >= 1 })
	h.fb.mu.Lock()
	h.fb.dialStatus = 0
	h.fb.mu.Unlock()
	waitFor(t, "reconnected on the other session", func() bool { return h.bl.Connected() && h.bl.CurrentAnchor() == userS4450 })
}

// Review item 6: pongs keep a frameless connection alive past MaxSilence.
func TestPingPongKeepsQuietConnectionAlive(t *testing.T) {
	h := newHarness(t, ModeShadow, func(c *BroadcastConfig) {
		c.MaxSilence = 120 * time.Millisecond
		c.PingInterval = 30 * time.Millisecond
	}, nil)
	waitFor(t, "connect", h.bl.Connected)
	time.Sleep(500 * time.Millisecond) // > 4× MaxSilence with no frames
	if h.bl.Stats().Connects.Load() != 1 || !h.bl.Connected() || h.fb.tokensIssued() != 1 {
		t.Fatalf("quiet connection churned: connects=%d tokens=%d connected=%v", h.bl.Stats().Connects.Load(), h.fb.tokensIssued(), h.bl.Connected())
	}
}
