// Package orderws is the hft-engine's bridge to Indira's order-status
// WebSocket. Every broker order event (PLACED, EXECUTED, REJECTED,
// CANCELLED, partial fills) flows through here and gets routed to the
// owning strategy.Runner.
//
// Why this matters for HFT:
//   - IndiraBroker.PlaceLimit just gets an HTTP "order accepted" — the
//     ACTUAL fill (or partial fill, or rejection) arrives milliseconds
//     later via the order-status WSS.
//   - Without this bridge the strategy's ChunkState would stay in
//     CHUNK_OPEN forever, never transitioning to CHUNK_PARTIAL or
//     CHUNK_FILLED, and the next chunk would never be placed.
//
// Architecture:
//   - One shared *indira.WSClient per process. Same connection serves
//     all subscribed users. Established lazily on first SubscribeUser.
//   - A single goroutine (readLoop) drains the WSClient.Updates channel
//     and calls Router for every event.
//   - Router (provided by the manager) figures out which strategy owns
//     the broker_order_id and forwards a state.FillEvent to its Runner.
//
// This file is the WSS-side only. The order-id → strategy routing lives
// in manager.go because that's where the live Runner registry lives.
package orderws

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// Router is the callback the orderws.Service calls for every translated
// broker event. Returning quickly is critical — Router runs synchronously
// in the read goroutine. The manager's implementation just hands off to
// Runner.SendFill (non-blocking channel send).
type Router func(ev state.FillEvent)

// Service owns the shared *indira.WSClient. It is safe to call methods
// from many goroutines.
type Service struct {
	httpClient *indira.Client
	router     Router
	logger     *zap.Logger

	mu       sync.Mutex
	wsClient *indira.WSClient
	subs     map[string]*indira.AuthContext // userID → last good auth

	// readerRunning ensures the read loop is started exactly once.
	readerRunning bool

	// ctx is the lifetime context for the read loop. Stop cancels it.
	ctx    context.Context
	cancel context.CancelFunc

	// metrics
	eventsReceived uint64
	eventsRouted   uint64
	eventsDropped  uint64
}

// New builds a Service. Call SubscribeUser(auth) for each user the engine
// will run strategies for; the first such call establishes the connection
// and starts the read loop.
//
// httpClient must be the same *indira.Client the broker layer uses (we
// reuse the underlying HTTP connection pool for the WS token fetch).
func New(httpClient *indira.Client, router Router, logger *zap.Logger) *Service {
	return &Service{
		httpClient: httpClient,
		router:     router,
		logger:     logger.Named("orderws"),
		subs:       make(map[string]*indira.AuthContext),
	}
}

// Start prepares the service for use. It does NOT open the WS — that
// happens lazily on the first SubscribeUser. We accept a ctx so Stop
// can be called by simply cancelling it from main.go.
func (s *Service) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx, s.cancel = context.WithCancel(ctx)
}

// Stop closes the WS and stops the read loop. Idempotent.
func (s *Service) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	if s.wsClient != nil {
		s.wsClient.Close()
		s.wsClient = nil
	}
	s.readerRunning = false
}

// SubscribeUser ensures the given user's order events are streaming.
// Idempotent: calling twice for the same user is a no-op. First call
// dials + authenticates the shared WS; subsequent calls add the user
// to the existing connection.
//
// Auth refresh on AU004 happens automatically inside *indira.WSClient
// via its OnAuthRefresh hook (wired in main.go to repo.LoadCredentials).
func (s *Service) SubscribeUser(ctx context.Context, auth *indira.AuthContext) error {
	if auth == nil || auth.UserId == "" {
		return fmt.Errorf("orderws: auth + UserId required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Already subscribed — store the latest auth (JWT may have rotated)
	// and return.
	if _, exists := s.subs[auth.UserId]; exists {
		s.subs[auth.UserId] = auth
		return nil
	}

	// First user? Establish the connection.
	if s.wsClient == nil {
		c := indira.NewWSClient(s.httpClient, auth)
		if err := c.Connect(ctx); err != nil {
			return fmt.Errorf("ws connect: %w", err)
		}
		s.wsClient = c
		s.logger.Info("order-status WS connected (first user)",
			zap.String("user_id", auth.UserId))
	} else {
		// Subsequent users — subscribe on the live connection.
		if err := s.wsClient.Subscribe(ctx, auth); err != nil {
			return fmt.Errorf("ws subscribe user %s: %w", auth.UserId, err)
		}
		s.logger.Info("user subscribed on shared order-status WS",
			zap.String("user_id", auth.UserId))
	}

	s.subs[auth.UserId] = auth

	// Start the reader once.
	if !s.readerRunning {
		s.readerRunning = true
		go s.readLoop()
	}
	return nil
}

// UnsubscribeUser is currently a no-op at the wire level — Indira's order
// WSS doesn't have a per-user unsubscribe; we just drop the user from our
// routing map so events for them are skipped. The connection stays open
// for remaining users.
func (s *Service) UnsubscribeUser(userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, userID)
}

// readLoop drains the WSClient.Updates channel and dispatches events to
// the manager-provided Router. One goroutine per process.
func (s *Service) readLoop() {
	defer func() {
		s.mu.Lock()
		s.readerRunning = false
		s.mu.Unlock()
		s.logger.Info("order-status read loop stopped")
	}()

	for {
		s.mu.Lock()
		ws := s.wsClient
		ctx := s.ctx
		s.mu.Unlock()
		if ws == nil || ctx == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ws.Updates:
			if !ok {
				s.logger.Warn("order-status WS updates channel closed")
				return
			}
			s.eventsReceived++
			s.handleUpdate(msg)
		}
	}
}

// handleUpdate translates one *indira.WSOrderStatus into a state.FillEvent
// (or a CANCEL / REJECT event) and forwards via the Router.
func (s *Service) handleUpdate(msg *indira.WSOrderStatus) {
	// indiraID identifies the broker order. Same precedence as
	// trade-execution: UniqueCode preferred, OrderNumber as fallback.
	indiraID := msg.UniqueCode
	if indiraID == "" {
		indiraID = msg.OrderNumber
	}
	if indiraID == "" || indiraID == "0" {
		return
	}

	statusUpper := strings.ToUpper(strings.TrimSpace(msg.OrderStatus))
	ev := state.FillEvent{
		BrokerOrderID:  indiraID,
		At:             time.Now(),
		ExchangeTimeUS: parseBrokerTimeMicros(msg.OrderTimeStamp),
		Reason:         msg.Reason,
	}

	switch statusUpper {
	case "EXECUTED", "TRADED", "FILLED", "COMPLETE",
		"PARTIALLY TRADED", "PARTIALLY EXECUTED":
		ev.EventType = "FILL"
		ev.FillQty = atoi(string(msg.TradedQTY))
		ev.FillPrice = atof(msg.TradedPrice)
		// Fallback: some Indira EXECUTED frames carry traded_price=0
		// (race between exchange ack and price publication). Use the
		// order's own LimitPrice as a sane stand-in — the strategy will
		// reconcile via the trade book on next reconciler tick.
		if ev.FillPrice <= 0 {
			ev.FillPrice = atof(msg.OrderPrice)
		}

	case "CANCELLED":
		ev.EventType = "CANCEL"

	case "REJECTED", "A.REJECTED":
		ev.EventType = "REJECT"

	default:
		// PENDING, OPEN, TRIGGER PENDING, MODIFIED → not actionable for
		// chunk state. Drop silently. The strategy uses the place-time
		// orderID to track the chunk; it doesn't need to know about
		// intermediate broker-side state transitions.
		return
	}

	if ev.FillQty <= 0 && ev.EventType == "FILL" {
		// Partial fill update with no incremental qty — drop.
		return
	}

	s.router(ev)
	s.eventsRouted++
}

// Stats — exported for /readyz dashboards.
func (s *Service) Stats() (received, routed, dropped uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eventsReceived, s.eventsRouted, s.eventsDropped
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

func atoi(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

func atof(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

// parseBrokerTimeMicros best-effort parses Indira's OrderTimeStamp into
// microseconds since epoch. Format observed live: "13-May-2026 15:14:34".
// Returns 0 on parse failure — exchange_time_us is purely informational.
func parseBrokerTimeMicros(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse("02-Jan-2006 15:04:05", s)
	if err != nil {
		return 0
	}
	return t.UnixMicro()
}
