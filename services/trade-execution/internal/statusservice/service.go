// Package statusservice provides real-time order status updates
// using the Indira WebSocket stream. It subscribes per-user and
// updates the order database and publishes Kafka notifications.
package statusservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	inexec "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/timezone"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// OCOHandler is called on every broker WS status update to check if the order
// belongs to an OCO group and act accordingly (place legs, cancel counterpart).
type OCOHandler interface {
	HandleBrokerUpdate(ctx context.Context, order *models.Order, brokerStatus string)
	// CancelGroupsBySymbol cancels all active OCO groups for a user+symbol.
	// Used when a manual position exit is detected (order not in our DB).
	CancelGroupsBySymbol(ctx context.Context, userID string, symbol string)
	// IsKnownBrokerID returns true if the broker order ID belongs to an OCO group.
	// Used to avoid false manual-exit detection when the DB write is still in-flight.
	IsKnownBrokerID(brokerID string) bool
}

// MLHandler is called on every broker WS status update to check if the order
// belongs to a multi-level exit group (TP limit orders or single SL order).
// Implemented by *multilevel.Manager.
type MLHandler interface {
	HandleBrokerUpdate(ctx context.Context, order *models.Order, brokerStatus string)
	CancelGroupsBySymbol(ctx context.Context, userID string, symbol string)
	IsKnownBrokerID(brokerID string) bool
}

// OrderStatusService maintains one dedicated WebSocket connection per user to
// the Indira broker. The broker binds a connection to a single authenticated
// user, so order statuses for multiple users cannot share a connection — each
// subscribed user gets its own WSClient (own token, heartbeat, reconnect loop)
// and its own processUpdates goroutine draining that client's Updates channel.
type OrderStatusService struct {
	execClient    *inexec.ExecutionClient
	repo          repository.OrderRepository
	credsRepo     repository.CredentialsRepository
	publisher     *publisher.KafkaPublisher
	logger        *zap.Logger
	wsBroadcaster func(userID string, order *models.Order)
	ocoHandler    OCOHandler // OCO order management hook
	mlHandler     MLHandler  // Multi-level exit order management hook

	// tokenExpiredNotifier is called when a user's token is confirmed expired (30s retry also failed).
	// Wired in main.go to push a token_expired event via /ws/live-orders.
	tokenExpiredNotifier func(userID string)

	// onOrderFilled is called when an order status becomes EXECUTED/TRADED.
	// Wired in main.go to fetch and push updated positions via /ws/live-orders.
	onOrderFilled func(userID string, auth *indiraClient.AuthContext)

	// userMu: userID → *sync.Mutex. Serializes StartSubscription/StopSubscription
	// for the SAME user (so concurrent calls can't create duplicate clients).
	// Per-user — NOT a single global lock — because StartSubscription holds it
	// across a blocking broker connect; a global lock would let one user's slow
	// connect freeze every other user's subscription.
	userMu sync.Map

	// wsClients: userID → *indiraClient.WSClient. One dedicated broker
	// connection per subscribed user.
	wsClients sync.Map

	// processors: userID → *processorHandle. Cancels that user's processUpdates
	// goroutine on StopSubscription and tracks whether it is still alive so
	// StartSubscription can restart a dead processor on a lingering client.
	processors sync.Map

	// subscriberAuths: userID → *indiraClient.AuthContext
	// Tracks who is subscribed and supplies the latest auth for position
	// fetches after a fill and for re-login (ResumeUserSubscription).
	subscriberAuths sync.Map

	// orderMutexes: brokerOrderID → *sync.Mutex
	// Serializes handleStatusUpdate for the same broker order so concurrent
	// goroutines (one per WS event) can't read-modify-write the same row out of
	// order and overwrite a newer fill with stale TradedQTY/TradedPrice.
	// Why: the broker pushes one WS event per partial fill, and each event is
	// dispatched on a fresh goroutine (see processUpdates). Without this mutex,
	// event B (cumulative qty=80) can land in the DB after event C (cumulative
	// qty=100), leaving filled_quantity/filled_price stuck at the older snapshot
	// and surfacing as wrong values on /ws/live-orders.
	orderMutexes sync.Map

	// activeStrategyUsers: userID → struct{}. Users with at least one active LIVE
	// strategy are excluded from the idle-sweep for the full trading day — their
	// broker WS must stay alive until market close regardless of position state.
	// Populated on startup from trading_db and kept current via Kafka strategy
	// events (CONFIG_CREATED/UPDATED → mark; CONFIG_DELETED/PAUSED → unmark).
	// Unmark does NOT close the WS — the connection stays open; only CloseAll
	// (service shutdown) or UnmarkAllActiveStrategyUsers (market close) leads to
	// a sweep closing it.
	activeStrategyUsers sync.Map
}

// lockOrder returns the mutex guarding all WS updates for one broker order ID.
// Callers MUST defer Unlock immediately after acquiring.
func (s *OrderStatusService) lockOrder(brokerOrderID string) *sync.Mutex {
	muVal, _ := s.orderMutexes.LoadOrStore(brokerOrderID, &sync.Mutex{})
	return muVal.(*sync.Mutex)
}

// lockUser returns the mutex serializing StartSubscription/StopSubscription for
// one user. Each user has its own mutex, so a slow broker connect for one user
// never blocks subscription work for another. Entries are never deleted: a
// concurrent caller may already hold a pointer to the mutex.
func (s *OrderStatusService) lockUser(userID string) *sync.Mutex {
	muVal, _ := s.userMu.LoadOrStore(userID, &sync.Mutex{})
	return muVal.(*sync.Mutex)
}

// SetWSBroadcaster wires a callback so live order status changes are pushed
// to the frontend immediately via /ws/live-orders.
func (s *OrderStatusService) SetWSBroadcaster(fn func(userID string, order *models.Order)) {
	s.wsBroadcaster = fn
}

// SetTokenExpiredNotifier wires a callback invoked when a user's broker token is
// confirmed expired (the 30s first retry also got 401). Used to push token_expired
// events to the frontend via /ws/live-orders so the user can be prompted to re-login.
func (s *OrderStatusService) SetTokenExpiredNotifier(fn func(userID string)) {
	s.tokenExpiredNotifier = fn
}

// SetOnOrderFilled wires a callback invoked after each EXECUTED/TRADED broker update.
// The callback receives the userID and current auth so the caller can fetch and push
// fresh positions to the frontend via /ws/live-orders.
func (s *OrderStatusService) SetOnOrderFilled(fn func(userID string, auth *indiraClient.AuthContext)) {
	s.onOrderFilled = fn
}

// ResumeUserSubscription supplies a fresh AuthContext after the user re-logins.
// It updates the stored auth and tells that user's WS client to reconnect
// immediately with the new credentials.
func (s *OrderStatusService) ResumeUserSubscription(userID string, auth *indiraClient.AuthContext) {
	authCopy := *auth
	s.subscriberAuths.Store(userID, &authCopy)
	if v, ok := s.wsClients.Load(userID); ok {
		v.(*indiraClient.WSClient).ResumeWithNewAuth(&authCopy)
		s.logger.Info("Auth resume sent to WS client", zap.String("user_id", userID))
	}
}

// SetOCOHandler wires the OCO manager so every broker WS status update is
// checked for OCO group membership. This is the hook that enables:
//   - Entry fill → place SL+TP legs
//   - SL leg fill → cancel TP leg (and vice versa)
func (s *OrderStatusService) SetOCOHandler(handler OCOHandler) {
	s.ocoHandler = handler
}

// SetMLHandler wires the multi-level exit manager so broker WS events for
// live TP limit orders and single SL orders are routed correctly.
func (s *OrderStatusService) SetMLHandler(handler MLHandler) {
	s.mlHandler = handler
}

// MarkUserActiveStrategy marks userID as having an active LIVE strategy, protecting
// their broker WS from the idle sweep for the remainder of the trading day.
// Called on CONFIG_CREATED / CONFIG_UPDATED Kafka events.
func (s *OrderStatusService) MarkUserActiveStrategy(userID string) {
	s.activeStrategyUsers.Store(userID, struct{}{})
}

// UnmarkUserActiveStrategy removes the idle-sweep protection for userID.
// Does NOT close the broker WS — the connection stays open so any in-flight
// orders / SL/TP legs can still receive fill events. Only the next idle sweep
// (if the user has no live exposure) or service shutdown will close it.
// Called on CONFIG_DELETED / CONFIG_PAUSED Kafka events.
func (s *OrderStatusService) UnmarkUserActiveStrategy(userID string) {
	s.activeStrategyUsers.Delete(userID)
}

// UnmarkAllActiveStrategyUsers clears the full protection set at market close
// so the next idle-sweep pass can clean up connections for users with no
// remaining exposure. Called by AutoSquareOffScheduler after the live
// square-off fires.
func (s *OrderStatusService) UnmarkAllActiveStrategyUsers() {
	s.activeStrategyUsers.Range(func(key, _ any) bool {
		s.activeStrategyUsers.Delete(key)
		return true
	})
}

// NewOrderStatusService creates a new order status service.
// credsRepo may be nil — if set, enables auto-refresh of expired broker
// credentials on the shared WebSocket connection (avoids persistent 401 loops).
func NewOrderStatusService(execClient *inexec.ExecutionClient, repo repository.OrderRepository, credsRepo repository.CredentialsRepository, pub *publisher.KafkaPublisher, logger *zap.Logger) *OrderStatusService {
	return &OrderStatusService{
		execClient: execClient,
		repo:       repo,
		credsRepo:  credsRepo,
		publisher:  pub,
		logger:     logger,
	}
}

// StartSubscription opens a dedicated broker WebSocket connection for userID.
// Idempotent: if the user already has a connection, it only refreshes the
// stored auth — the client's own monitorReconnect loop self-heals any drop,
// so no further action is needed.
func (s *OrderStatusService) StartSubscription(ctx context.Context, userID string, auth *indiraClient.AuthContext) error {
	// Always refresh stored auth (bearer token may have rotated).
	authCopy := *auth
	s.subscriberAuths.Store(userID, &authCopy)

	// Per-user lock: concurrent Start/Stop for the SAME user serialize, but
	// different users never block each other — one user's slow broker connect
	// must not freeze anyone else's subscription.
	mu := s.lockUser(userID)
	mu.Lock()
	defer mu.Unlock()

	// Already connected for this user — push fresh credentials into the existing
	// client so the token refresh loop uses the new bearer token. This handles
	// CONFIG_UPDATED: the REST order path re-reads from DB after cache invalidation,
	// but the WSClient has its own internal w.auth that never gets updated otherwise.
	// ResumeWithNewAuth resets the retry backoff and retries createWsToken immediately.
	if v, ok := s.wsClients.Load(userID); ok {
		client := v.(*indiraClient.WSClient)
		client.ResumeWithNewAuth(&authCopy)
		// Self-heal: a prior StopSubscription (or a teardown/re-subscribe race) can
		// leave the client in the map while its processUpdates goroutine has already
		// exited. With no live processor nothing drains client.Updates, so broker
		// fills — including the EXECUTED that places OCO SL/TP legs — are silently
		// dropped. If the processor is dead, restart it on the existing client.
		if h, ok := s.processors.Load(userID); ok && h.(*processorHandle).alive.Load() {
			return nil
		}
		s.logger.Warn("WS client present but processor not running — restarting processor",
			zap.String("user_id", userID))
		procCtx, cancel := context.WithCancel(context.Background())
		handle := &processorHandle{cancel: cancel}
		handle.alive.Store(true)
		s.processors.Store(userID, handle)
		go s.processUpdates(procCtx, userID, client, handle)
		return nil
	}

	client := s.execClient.NewUserWSClient(&authCopy)

	// Wire per-user callbacks. userID is captured from the closure (our system
	// user ID) rather than taken from the WSClient's broker auth, so DB and
	// notifier lookups always use the correct key.
	client.OnReconnected = func() {
		metrics.BrokerWSReconnects.Inc()
		s.logger.Info("Broker WS reconnected", zap.String("user_id", userID))
	}
	// On 401, reload credentials from DB so the WS can recover without waiting
	// for the user to place a new order.
	client.OnAuthRefresh = func(_ string) (*indiraClient.AuthContext, error) {
		return s.refreshAuthFromDB(userID)
	}
	// On confirmed token expiry (30s retry also failed), notify only THIS user —
	// each user has an independent connection, so one expired token no longer
	// affects anyone else.
	client.OnTokenExpired = func(_ string) {
		if s.tokenExpiredNotifier != nil {
			go s.tokenExpiredNotifier(userID)
		}
	}

	// Bound the broker connect (token fetch + WS dial). Callers pass
	// context.Background(), so without this a hung broker token API would
	// block this goroutine — and this user's lock — indefinitely.
	connectCtx, connectCancel := context.WithTimeout(ctx, 20*time.Second)
	err := client.Connect(connectCtx)
	connectCancel()
	if err != nil {
		return fmt.Errorf("connect broker WS for user %s: %w", userID, err)
	}

	// Dedicated processor goroutine for this user's Updates channel.
	// Background context for the loop's own lifetime — cancelled by StopSubscription.
	procCtx, cancel := context.WithCancel(context.Background())
	handle := &processorHandle{cancel: cancel}
	handle.alive.Store(true)
	s.processors.Store(userID, handle)
	s.wsClients.Store(userID, client)
	metrics.BrokerWSActiveConnections.Inc()

	go s.processUpdates(procCtx, userID, client, handle)
	s.logger.Info("Per-user broker WS established", zap.String("user_id", userID))
	return nil
}

// StopSubscription closes a user's broker WebSocket connection and stops its
// processor goroutine. Safe to call for an unknown user (no-op). Used both for
// explicit unsubscribe and by the idle-connection sweep.
func (s *OrderStatusService) StopSubscription(userID string) {
	mu := s.lockUser(userID)
	mu.Lock()
	defer mu.Unlock()

	if h, ok := s.processors.LoadAndDelete(userID); ok {
		h.(*processorHandle).cancel()
	}
	if v, ok := s.wsClients.LoadAndDelete(userID); ok {
		v.(*indiraClient.WSClient).Close()
		metrics.BrokerWSActiveConnections.Dec()
		s.logger.Info("Closed broker WS connection", zap.String("user_id", userID))
	}
	s.subscriberAuths.Delete(userID)
}

// CloseAll shuts down every per-user broker WebSocket connection. Called on
// service shutdown.
func (s *OrderStatusService) CloseAll() {
	s.wsClients.Range(func(key, _ any) bool {
		s.StopSubscription(key.(string))
		return true
	})
}

// StartIdleSweep launches a background loop that periodically closes broker WS
// connections for users that no longer have live exposure (no in-flight orders
// and no open positions). Without this, per-user connections only accumulate
// over a trading day. The loop stops when ctx is cancelled.
func (s *OrderStatusService) StartIdleSweep(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweepIdleConnections(ctx)
			}
		}
	}()
}

// sweepIdleConnections closes connections for users absent from the live-exposure set.
func (s *OrderStatusService) sweepIdleConnections(ctx context.Context) {
	sweepCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	active, err := s.repo.GetUsersWithLiveExposure(sweepCtx)
	if err != nil {
		s.logger.Warn("Idle WS sweep: failed to list users with live exposure", zap.Error(err))
		return
	}
	keep := make(map[string]struct{}, len(active))
	for _, u := range active {
		keep[u] = struct{}{}
	}

	var idle []string
	s.wsClients.Range(func(key, _ any) bool {
		userID := key.(string)
		if _, ok := keep[userID]; !ok {
			// Never sweep a user who has an active LIVE strategy: their broker WS
			// must stay alive for the full trading day so entry fills, SL/TP events,
			// and any order placed after a quiet period are always received.
			if _, protected := s.activeStrategyUsers.Load(userID); protected {
				return true
			}
			idle = append(idle, userID)
		}
		return true
	})
	for _, userID := range idle {
		s.StopSubscription(userID)
	}
	if len(idle) > 0 {
		s.logger.Info("Idle WS sweep closed connections", zap.Int("count", len(idle)))
	}
}

// refreshAuthFromDB reloads credentials from the DB for the given user.
// Called by WSClient.OnAuthRefresh when a 401 is detected during token fetch.
func (s *OrderStatusService) refreshAuthFromDB(userID string) (*indiraClient.AuthContext, error) {
	if s.credsRepo == nil {
		return nil, fmt.Errorf("no credentials repository configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	userId, appId, source, bearerToken, err := s.credsRepo.GetIndiraCredentials(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to load credentials for user %s: %w", userID, err)
	}
	newAuth := &indiraClient.AuthContext{
		UserId:      userId,
		AppId:       appId,
		Source:      source,
		BearerToken: bearerToken,
	}
	// Also update subscriberAuths so position fetches use the fresh token.
	s.subscriberAuths.Store(userID, newAuth)
	s.logger.Info("Refreshed auth from DB for WS reconnect", zap.String("user_id", userID))
	return newAuth, nil
}

// processorHandle tracks one user's processUpdates goroutine: cancel stops it,
// alive reports whether it is still draining the broker WS Updates channel. Used
// by StartSubscription to detect (and restart) a client whose processor died.
type processorHandle struct {
	cancel context.CancelFunc
	alive  atomic.Bool
}

// StartFillReconcile launches the WS-independent fill safety net. On each tick it
// asks the broker trade-book API whether any still-open order has executed and,
// if so, drives an EXECUTED snapshot through the same handleStatusUpdate → OCO
// path the live WS uses — so protective SL/TP legs are placed even when the
// per-user order WebSocket is dead (idle-swept, dropped, or its processor exited).
// interval <= 0 disables the loop. Stops when ctx is cancelled.
func (s *OrderStatusService) StartFillReconcile(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.reconcileFillsViaTradeBook(ctx)
			}
		}
	}()
}

// reconcileFillsViaTradeBook is one pass of the trade-book safety net: for every
// non-terminal live order it aggregates the broker trade book and, when the order
// has (partially) filled, applies an EXECUTED / PARTIALLY-EXECUTED snapshot via
// ReconcileOrderFromBrokerBook (which reuses handleStatusUpdate, so the OCO/ML
// hooks fire and place SL/TP). Idempotent: ReconcileOrderFromBrokerBook no-ops
// when the broker status already matches the DB, and the OCO state machine ignores
// a fill it has already acted on — so repeated passes never double-place legs.
func (s *OrderStatusService) reconcileFillsViaTradeBook(ctx context.Context) {
	rcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// minAge=10s gives the normal placement → broker-WS path a head start so this
	// safety net does not race freshly placed orders.
	candidates, err := s.repo.GetReconciliationCandidates(rcCtx, 24, 10)
	if err != nil {
		s.logger.Warn("fill-reconcile: list candidates failed", zap.Error(err))
		return
	}
	if len(candidates) == 0 {
		return
	}

	byUser := make(map[string][]*models.Order, 8)
	for _, o := range candidates {
		// Only ENTRY orders need fill-reconciliation: the reconciler's job is to
		// catch a missed entry fill so OCO/ML can PLACE the SL/TP legs. Resting
		// SL/TP legs (and reverse square-off orders) are already placed at the
		// broker and only need the live WS to report when they trigger — polling
		// the trade book for them returns empty until they fill and would hammer
		// the broker for every protective order on every tick. Skip them.
		if o.IsSquareOffOrder || o.ParentOrderID != nil {
			continue
		}
		if o.OCORole != nil && *o.OCORole != "ENTRY" {
			continue
		}
		byUser[o.UserID] = append(byUser[o.UserID], o)
	}
	if len(byUser) == 0 {
		return
	}

	for userID, orders := range byUser {
		auth := s.ensureAuthLoaded(userID)
		if auth == nil {
			continue
		}
		for _, o := range orders {
			if o.IndiraOrderID == nil || *o.IndiraOrderID == "" {
				continue
			}
			// Single attempt: no EXECUTED event to race here, so an empty result
			// just means "not filled yet" — the next tick re-checks.
			qty, price, _, ok := s.fillFromTradeBookN(rcCtx, userID, *o.IndiraOrderID, 1)
			if !ok || qty <= 0 {
				continue // not filled at the broker yet — leave for the next pass
			}
			status := "EXECUTED"
			if qty < int(o.Quantity) {
				status = "PARTIALLY EXECUTED"
			}
			snapshot := &indiraClient.WSOrderStatus{
				UniqueCode:  *o.IndiraOrderID,
				OrderStatus: status,
				Symbol:      o.Symbol,
				UCC:         userID,
				BuySell:     string(o.OrderSide),
				TradedQTY:   indiraClient.FlexInt(strconv.Itoa(qty)),
				TradedPrice: strconv.FormatFloat(price, 'f', -1, 64),
			}
			changed, rerr := s.ReconcileOrderFromBrokerBook(rcCtx, o, snapshot)
			if rerr != nil {
				s.logger.Warn("fill-reconcile: apply snapshot failed",
					zap.String("user_id", userID),
					zap.String("indira_order_id", *o.IndiraOrderID),
					zap.Error(rerr))
				continue
			}
			if changed {
				s.logger.Info("fill-reconcile: applied trade-book fill (WS fallback) — OCO/ML will place SL/TP",
					zap.String("user_id", userID),
					zap.String("symbol", o.Symbol),
					zap.String("indira_order_id", *o.IndiraOrderID),
					zap.String("status", status),
					zap.Int("filled_qty", qty),
					zap.Float64("fill_price", price))
			}
		}
	}
}

// ensureAuthLoaded returns the cached auth for a user, loading it from the DB
// (and caching it) when absent. The idle sweep deletes subscriberAuths on
// teardown, but the trade-book reconciler still needs credentials to call the
// broker. Returns nil when no credentials are available.
func (s *OrderStatusService) ensureAuthLoaded(userID string) *indiraClient.AuthContext {
	if v, ok := s.subscriberAuths.Load(userID); ok {
		return v.(*indiraClient.AuthContext)
	}
	auth, err := s.refreshAuthFromDB(userID)
	if err != nil {
		s.logger.Warn("fill-reconcile: load credentials failed",
			zap.String("user_id", userID), zap.Error(err))
		return nil
	}
	return auth
}

// processUpdates drains one user's WSClient.Updates channel and dispatches
// each update to its own goroutine so DB operations never block the read loop.
// One processor runs per subscribed user; it exits when ctx is cancelled by
// StopSubscription. Each update is handled with a background context so a
// StopSubscription mid-update never aborts an in-flight DB write.
func (s *OrderStatusService) processUpdates(ctx context.Context, userID string, client *indiraClient.WSClient, handle *processorHandle) {
	s.logger.Info("Broker WS update processor started", zap.String("user_id", userID))
	defer func() {
		if handle != nil {
			handle.alive.Store(false)
		}
		s.logger.Info("Broker WS update processor stopped", zap.String("user_id", userID))
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case wsStatus, ok := <-client.Updates:
			if !ok {
				// Updates channel is never closed by WSClient; safety net only.
				s.logger.Warn("Broker WS updates channel closed unexpectedly",
					zap.String("user_id", userID))
				return
			}
			go s.handleStatusUpdate(context.Background(), wsStatus)
		}
	}
}

func (s *OrderStatusService) handleStatusUpdate(ctx context.Context, wsStatus *indiraClient.WSOrderStatus) {
	updateStart := time.Now()
	defer func() {
		metrics.StatusUpdateProcessDuration.Observe(time.Since(updateStart).Seconds())
	}()

	// UniqueCode is normally the System-generated order ID ("NZVND00001J2" style)
	// OrderNumber = Exchange order number (0 when not yet matched)
	indiraID := wsStatus.UniqueCode
	if indiraID == "" {
		indiraID = wsStatus.OrderNumber
	}
	if indiraID == "" || indiraID == "0" {
		return
	}

	// Serialize all WS events for the same broker order. processUpdates spawns
	// one goroutine per event; without this, two events that arrive close together
	// for the same order can read the same DB row, mutate it independently, and
	// both write back — last writer wins, which is frequently the older event and
	// surfaces as wrong filled_quantity / filled_price to /ws/live-orders.
	mu := s.lockOrder(indiraID)
	mu.Lock()
	defer mu.Unlock()

	order, err := s.repo.GetByIndiraOrderID(ctx, indiraID)
	if err != nil {
		// Order not in DB yet — race between background DB persist and WS callback.
		// Retry after a short delay: our system's exit/OCO/ML orders may still be writing.
		isKnownOCO := s.ocoHandler != nil && s.ocoHandler.IsKnownBrokerID(indiraID)
		isKnownML := s.mlHandler != nil && s.mlHandler.IsKnownBrokerID(indiraID)
		isKnownInternal := isKnownOCO || isKnownML
		brokerStatusUpper := strings.ToUpper(strings.TrimSpace(wsStatus.OrderStatus))
		isFill := brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED"

		// Always retry once after 500ms — handles force-exit orders, OCO/ML legs,
		// and any other orders where the DB write races the broker WS callback.
		time.Sleep(500 * time.Millisecond)
		order, err = s.repo.GetByIndiraOrderID(ctx, indiraID)
		if err != nil {
			if isKnownInternal {
				s.logger.Warn("Internal SL/TP broker ID recognised in memory but DB lookup still failed after retry",
					zap.String("broker_id", indiraID),
					zap.Bool("oco", isKnownOCO),
					zap.Bool("ml", isKnownML),
					zap.Error(err))
			} else if isFill && wsStatus.Symbol != "" {
				// Genuinely not our order — position closed outside our system
				// (manual broker-terminal exit or broker EOD MIS square-off).
				s.handleExternalExit(ctx, wsStatus)
			}
			return
		}
		// Fall through — process the order normally below.
	}

	// ── Store raw broker data exactly as received ──────────────────────────
	// Serialize full WS payload to JSON for audit/debugging
	rawJSON, _ := json.Marshal(wsStatus)
	rawJSONStr := string(rawJSON)
	order.BrokerWSData = &rawJSONStr

	// Store raw broker status string without any mapping
	brokerStatus := wsStatus.OrderStatus
	order.BrokerStatus = &brokerStatus

	// Store exchange order number
	if wsStatus.OrderNumber != "" && wsStatus.OrderNumber != "0" {
		order.ExchangeOrderNumber = &wsStatus.OrderNumber
	}

	// ── Use broker status directly — no mapping ───────────────────────────
	// The broker sends statuses like PENDING, EXECUTED, CANCELLED etc.
	// We store and show exactly what the broker sends.
	brokerStatusUpper := strings.ToUpper(strings.TrimSpace(wsStatus.OrderStatus))
	newStatus := models.OrderStatus(brokerStatusUpper)
	previousStatus := order.Status

	// Record every WS event in execution_events for full audit trail
	s.repo.RecordExecutionEvent(ctx, order.OrderID, "BROKER_WS_UPDATE", map[string]interface{}{
		"broker_status":         wsStatus.OrderStatus,
		"previous_status":       string(previousStatus),
		"symbol":                wsStatus.Symbol,
		"order_number":          wsStatus.OrderNumber,
		"unique_code":           wsStatus.UniqueCode,
		"traded_qty":            wsStatus.TradedQTY,
		"traded_price":          wsStatus.TradedPrice,
		"order_price":           wsStatus.OrderPrice,
		"trigger_price":         wsStatus.TriggerPrice,
		"pending_qty":           wsStatus.PendingQty,
		"order_original_qty":    wsStatus.OrderOriginalQty,
		"product":               wsStatus.Product,
		"order_type":            wsStatus.OrderType,
		"buy_sell":              wsStatus.BuySell,
		"reason":                wsStatus.Reason,
		"order_entry_time":      wsStatus.OrderEntryTime,
		"last_modified":         wsStatus.LastModifiedTimeStamp,
		"order_seq":             wsStatus.OrderSequenceNumber,
		"message_seq":           wsStatus.MessageSequenceNumber,
		"decimal_locator":       wsStatus.DecimalLocator,
		"exchange":              wsStatus.Exchange,
		"misc":                  wsStatus.Misc,
		"initiated_by":          wsStatus.InitiatedBy,
		"modified_by":           wsStatus.ModifiedBy,
		"exchange_algo_id":      wsStatus.ExchangeAlgoID,
		"exchange_account_code": wsStatus.ExchangeAccountCode,
		"timestamp":             time.Now(),
	})

	// Metrics
	metrics.StatusUpdatesReceived.WithLabelValues(brokerStatusUpper).Inc()
	switch brokerStatusUpper {
	case "EXECUTED", "TRADED":
		metrics.FillsTotal.WithLabelValues("live").Inc()
	case "REJECTED", "A.REJECTED":
		metrics.RejectionsTotal.WithLabelValues("rejected").Inc()
	case "CANCELLED":
		metrics.RejectionsTotal.WithLabelValues("cancelled").Inc()
	}

	// Set status to exactly what broker sent
	order.Status = newStatus

	// Extract fill details using DecimalLocator for correct price
	dl := decimalLocator(string(wsStatus.DecimalLocator))

	// Update filled qty and price using a running volume-weighted average.
	// Indira sends TradedQTY as CUMULATIVE across all partial fills, but
	// TradedPrice as the price of THIS individual trade only — so the naive
	// "FilledPrice = TradedPrice" shows only the last partial's price.
	// VWAP formula: (prev_avg * prev_qty + this_price * delta_qty) / new_cum_qty.
	// Stale events (newCumQty < prevQty) are dropped: the per-order mutex
	// serializes execution but goroutine acquire order is non-deterministic,
	// so an older event can still run after a newer one.
	// Cumulative filled qty. Prefers the broker's TradedQTY, but falls back to
	// OrderOriginalQty/PendingQty when the broker reports EXECUTED/TRADED with an
	// empty TradedQTY — otherwise a real fill is recorded as filled_quantity=0.
	// See resolveFilledQty for why that 0 cascades into USER-DIRECT misattribution.
	newCumQty := resolveFilledQty(wsStatus, int(order.Quantity))
	thisTradePrice := 0.0
	if p, err := strconv.ParseFloat(wsStatus.TradedPrice, 64); err == nil && p > 0 {
		thisTradePrice = p
	}
	if newCumQty >= 0 {
		prevQty := int(order.FilledQuantity)
		switch {
		case newCumQty > prevQty:
			// Forward progress — compute VWAP using the delta from this event.
			deltaQty := newCumQty - prevQty
			var newAvg float64
			switch {
			case thisTradePrice <= 0:
				// Status-only event with no usable price — preserve prev avg.
				if order.FilledPrice != nil {
					newAvg = *order.FilledPrice
				}
			case prevQty <= 0:
				// First fill — this event's price IS the average.
				newAvg = thisTradePrice
			default:
				prevAvg := 0.0
				if order.FilledPrice != nil {
					prevAvg = *order.FilledPrice
				}
				newAvg = (prevAvg*float64(prevQty) + thisTradePrice*float64(deltaQty)) / float64(newCumQty)
			}
			order.FilledQuantity = int32(newCumQty)
			if newAvg > 0 {
				order.FilledPrice = &newAvg
			}
		case newCumQty == prevQty && thisTradePrice > 0 && order.FilledPrice == nil:
			// No qty progress but we now have a price and didn't before — backfill.
			order.FilledPrice = &thisTradePrice
		}
		// newCumQty < prevQty → stale event, drop the numeric update.
	}

	// Update order price from broker
	if wsStatus.OrderPrice != "" {
		if oprice, err := strconv.ParseFloat(wsStatus.OrderPrice, 64); err == nil && oprice > 0 {
			order.Price = &oprice
			// Fallback: if traded_price was 0 but order is filled, use order_price as fill price.
			// Indira broker sometimes sends traded_price=0.00 even on EXECUTED status.
			if order.FilledPrice == nil && (brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED") {
				order.FilledPrice = &oprice
			}
		}
	}

	// Mark execution time: OrderEntryTime → OrderTimeStamp → leave nil.
	if brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED" {
		if t := parseBrokerTime(wsStatus.OrderEntryTime); !t.IsZero() {
			order.ExecutedAt = &t
		} else if t := parseBrokerTime(wsStatus.OrderTimeStamp); !t.IsZero() {
			order.ExecutedAt = &t
		}
	}

	// Authoritative fill from the REST trade book. The broker WS omits TradedQTY
	// and ALL timestamps on EXECUTED/TRADED events (the qty above is derived and
	// ExecutedAt is usually still nil here), so pull the real fill qty, VWAP price
	// and exact trade TIME from the trade book. This is what makes exit time
	// populate: the OCO/ML/square-off exit recorders below all read the leg's
	// ExecutedAt / FilledPrice. Guarded to the fill-status transition so it runs
	// once per fill, not on every duplicate WS event. On any failure (error, race
	// before the trade is booked) we keep the WS-derived values as a fallback.
	if (brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED") &&
		string(previousStatus) != brokerStatusUpper {
		if qty, price, ts, ok := s.fillFromTradeBook(ctx, order.UserID, indiraID); ok {
			if qty > 0 {
				order.FilledQuantity = int32(qty)
			}
			if price > 0 {
				order.FilledPrice = &price
			}
			if !ts.IsZero() {
				order.ExecutedAt = &ts
			}
		}
	}

	// Store trigger price from broker (apply DecimalLocator)
	if wsStatus.TriggerPrice > 0 {
		tp := wsStatus.TriggerPrice / dl
		order.StopLoss = &tp
	}

	// Store rejection/cancellation reason
	if wsStatus.Reason != "" {
		reason := wsStatus.Reason
		order.RejectionReason = &reason
	}

	if err := s.repo.Update(ctx, order); err != nil {
		s.logger.Error("Failed to update order from WS",
			zap.Error(err),
			zap.String("order_id", order.OrderID.String()))
		return
	}

	s.logger.Info("Order updated from broker WS",
		zap.String("order_id", order.OrderID.String()),
		zap.String("prev", string(previousStatus)),
		zap.String("broker_status", brokerStatusUpper),
		zap.String("traded_qty", string(wsStatus.TradedQTY)),
		zap.String("traded_price", wsStatus.TradedPrice),
		zap.String("order_number", wsStatus.OrderNumber))

	// Push to all connected /ws/live-orders clients immediately
	if s.wsBroadcaster != nil {
		orderCopy := *order
		s.wsBroadcaster(orderCopy.UserID, &orderCopy)
	}

	// On fill of an auto-square-off reverse order, record the exit on the parent
	// entry position using the reverse order's actual broker fill price — so the
	// closed-orders view and the position_exit event show the true exit price.
	// Scoped to IsSquareOffOrder + ParentOrderID: OCO legs (IsSquareOffOrder=false)
	// and ML legs (ParentOrderID=nil) are excluded; they record their own exits.
	if (brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED") &&
		order.IsSquareOffOrder && order.ParentOrderID != nil {
		sqCopy := *order
		go s.recordSquareOffExit(context.Background(), &sqCopy)
	}

	// On fill: push updated positions to frontend via /ws/live-orders so the
	// frontend doesn't need to poll the positions REST API.
	if (brokerStatusUpper == "EXECUTED" || brokerStatusUpper == "TRADED") && s.onOrderFilled != nil {
		if authVal, ok := s.subscriberAuths.Load(order.UserID); ok {
			auth := authVal.(*indiraClient.AuthContext)
			go s.onOrderFilled(order.UserID, auth)
		}
	}

	// ── OCO hook: check if this order is part of an OCO group ────────────
	// On entry fill → place SL+TP legs. On leg fill → cancel counterpart.
	// Runs in its own goroutine so it never blocks the WS read loop.
	if s.ocoHandler != nil {
		orderCopy := *order
		go s.ocoHandler.HandleBrokerUpdate(context.Background(), &orderCopy, brokerStatusUpper)
	}

	// ── Multi-level hook: check if order is a TP/SL leg of a multi-level group ──
	// Handles: TP limit order fills, fixed/trailing SL fills for multi-level groups.
	if s.mlHandler != nil {
		orderCopy := *order
		go s.mlHandler.HandleBrokerUpdate(context.Background(), &orderCopy, brokerStatusUpper)
	}

	s.publishNotification(ctx, order, wsStatus)
}

// ReconcileOrderFromBrokerBook applies a broker-book snapshot to a single
// order, but only if the broker's reported status differs from the DB.
//
// This is the surgical entry point for the startup reconciliation pass: it
// reuses the same handleStatusUpdate path that broker WS events take (so OCO,
// ML, publisher, and execution_event hooks all fire identically), but it never
// invokes the path when the DB already matches the broker — preventing spurious
// duplicate notifications for orders that were already up-to-date.
//
// Caller responsibility: build a WSOrderStatus from the broker order-book row
// (UniqueCode = indira_order_id, OrderStatus = broker status, plus any fields
// the caller has). This method does not call the broker.
func (s *OrderStatusService) ReconcileOrderFromBrokerBook(ctx context.Context, dbOrder *models.Order, snapshot *indiraClient.WSOrderStatus) (changed bool, err error) {
	if dbOrder == nil || snapshot == nil {
		return false, fmt.Errorf("nil order or snapshot")
	}
	brokerStatusUpper := strings.ToUpper(strings.TrimSpace(snapshot.OrderStatus))
	dbStatusUpper := strings.ToUpper(strings.TrimSpace(string(dbOrder.Status)))
	if brokerStatusUpper == "" || brokerStatusUpper == dbStatusUpper {
		// Nothing to do — broker agrees with DB or has no opinion.
		return false, nil
	}
	s.logger.Info("startup reconciliation: applying broker snapshot",
		zap.String("order_id", dbOrder.OrderID.String()),
		zap.String("indira_id", snapshot.UniqueCode),
		zap.String("db_status", dbStatusUpper),
		zap.String("broker_status", brokerStatusUpper))
	s.handleStatusUpdate(ctx, snapshot)
	return true, nil
}

// normalizeBuySell maps the broker WS Buy_Sell field to "BUY"/"SELL". The broker
// sends it inconsistently as "1"/"2", "B"/"S", or the full word.
func normalizeBuySell(v string) string {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "1", "B", "BUY":
		return "BUY"
	case "2", "S", "SELL":
		return "SELL"
	}
	return ""
}

// normalizeExitSymbol uppercases and strips exchange series suffixes so the WS
// Symbol matches the plain symbol stored on our orders.
func normalizeExitSymbol(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	for _, sfx := range []string{"-EQ", "-BE", "-BZ", "-BL", "-SM", "-ST", "_EQ", "_BE"} {
		if strings.HasSuffix(s, sfx) && len(s) > len(sfx) {
			return s[:len(s)-len(sfx)]
		}
	}
	return s
}

// handleExternalExit runs when an EXECUTED order arrives whose broker ID is not
// in our DB — the position was closed outside our system (a manual sell from the
// broker terminal, or the broker's own EOD MIS auto-square-off).
//
// It does two things:
//  1. Cancels any active OCO / multi-level SL/TP groups for that user+symbol so
//     orphaned legs don't fire into a now-flat position.
//  2. Records the exit on the matching open algo entry order(s) — using the
//     order WebSocket event's actual fill price and exchange timestamp — so the
//     position shows a complete round trip (entry + exit price/time/P&L) instead
//     of being stuck with no exit data.
func (s *OrderStatusService) handleExternalExit(ctx context.Context, wsStatus *indiraClient.WSOrderStatus) {
	userID := wsStatus.UCC // UCC is the client code (user ID)
	if userID == "" {
		return
	}
	symbol := wsStatus.Symbol

	s.logger.Info("External exit detected on order WS",
		zap.String("user_id", userID),
		zap.String("symbol", symbol),
		zap.String("unique_code", wsStatus.UniqueCode))

	// (1) Tear down protective SL/TP groups for the now-closed position.
	if s.ocoHandler != nil {
		go s.ocoHandler.CancelGroupsBySymbol(context.Background(), userID, symbol)
	}
	if s.mlHandler != nil {
		go s.mlHandler.CancelGroupsBySymbol(context.Background(), userID, symbol)
	}

	// (2) Record the exit on the matching open entry order(s).
	fillSide := normalizeBuySell(wsStatus.BuySell)
	if fillSide == "" {
		return // can't tell direction — skip exit recording
	}

	entries, err := s.repo.GetOpenLiveEntriesByUserSymbol(ctx, userID, normalizeExitSymbol(symbol))
	if err != nil {
		s.logger.Warn("handleExternalExit: failed to load open entries", zap.Error(err))
		return
	}
	if len(entries) == 0 {
		return // nothing of ours to close
	}

	// Exit price from the WS event (TradedPrice, scaled by DecimalLocator),
	// falling back to OrderPrice.
	dl := decimalLocator(string(wsStatus.DecimalLocator))
	exitPrice := 0.0
	if p, e := strconv.ParseFloat(wsStatus.TradedPrice, 64); e == nil && p > 0 {
		exitPrice = p / dl
	}
	if exitPrice <= 0 {
		if p, e := strconv.ParseFloat(wsStatus.OrderPrice, 64); e == nil && p > 0 {
			exitPrice = p
		}
	}
	if exitPrice <= 0 {
		s.logger.Warn("handleExternalExit: no usable exit price — skipping", zap.String("symbol", symbol))
		return
	}

	// Authoritative exit qty/price/time from the REST trade book. The broker WS
	// closing event (a broker EOD MIS square-off or manual terminal sell) omits
	// TradedQTY and every timestamp, so without this the exit time would just be
	// time.Now() and the qty would have to be derived. UCC is the user id.
	tbQty, tbPrice, tbTime, tbOK := s.fillFromTradeBook(ctx, userID, wsStatus.UniqueCode)

	// Exit timestamp: trade book is authoritative; fall back to WS fields, then now.
	exitTime := tbTime
	if exitTime.IsZero() {
		exitTime = parseBrokerTime(wsStatus.OrderEntryTime)
	}
	if exitTime.IsZero() {
		exitTime = parseBrokerTime(wsStatus.OrderTimeStamp)
	}
	if exitTime.IsZero() {
		exitTime = parseBrokerTime(wsStatus.LastModifiedTimeStamp)
	}
	if exitTime.IsZero() {
		exitTime = time.Now()
	}

	// Prefer the trade book's authoritative fill price when the WS gave none.
	if exitPrice <= 0 && tbOK && tbPrice > 0 {
		exitPrice = tbPrice
	}

	// Quantity filled on the closing order. Trade book is authoritative; otherwise
	// fall back to the WS fields (Indira reports EXECUTED/TRADED with an empty
	// TradedQTY, the fill riding in OrderOriginalQty/PendingQty). Without a usable
	// qty the exit is silently dropped and the position keeps blank exit data.
	// placedQty=0: this order isn't ours, so there's no DB quantity to fall back to.
	remainingExitQty := resolveFilledQty(wsStatus, 0)
	if tbOK && tbQty > 0 {
		remainingExitQty = tbQty
	}
	if remainingExitQty <= 0 {
		return
	}

	// Allocate the exit quantity FIFO across our open entries. Only an
	// opposite-side fill closes an entry (a SELL closes a BUY, and vice versa).
	for _, entry := range entries {
		if remainingExitQty <= 0 {
			break
		}
		entrySide := strings.ToUpper(string(entry.OrderSide))
		if (entrySide == "BUY" && fillSide != "SELL") || (entrySide == "SELL" && fillSide != "BUY") {
			continue // same-side fill — a new position, not an exit
		}
		qty := int(entry.FilledQuantity)
		if qty > remainingExitQty {
			// External fill covers only part of this entry — don't mark a
			// position fully exited on a partial close. Stop here (FIFO).
			break
		}
		remainingExitQty -= qty

		entryPrice := 0.0
		if entry.FilledPrice != nil {
			entryPrice = *entry.FilledPrice
		}
		var pnl float64
		if entrySide == "BUY" {
			pnl = (exitPrice - entryPrice) * float64(qty)
		} else {
			pnl = (entryPrice - exitPrice) * float64(qty)
		}

		if err := s.repo.UpdateLiveTradeExit(ctx, entry.OrderID, exitPrice, pnl, exitTime, "MANUAL"); err != nil {
			s.logger.Warn("handleExternalExit: failed to record exit",
				zap.String("entry_order", entry.OrderID.String()), zap.Error(err))
			continue
		}
		// Audit trail — the external order isn't in our DB, so record the event
		// against the entry order it closed.
		s.repo.RecordExecutionEvent(ctx, entry.OrderID, "EXTERNAL_EXIT", map[string]interface{}{
			"exit_price":  exitPrice,
			"exit_qty":    qty,
			"pnl":         pnl,
			"exit_time":   exitTime,
			"fill_side":   fillSide,
			"unique_code": wsStatus.UniqueCode,
			"symbol":      symbol,
		})
		s.logger.Info("External exit recorded on entry order",
			zap.String("entry_order", entry.OrderID.String()),
			zap.String("symbol", symbol),
			zap.Float64("exit_price", exitPrice),
			zap.Float64("pnl", pnl))
	}
}

// recordSquareOffExit marks the parent entry position closed when its
// auto-square-off reverse order fills. The reverse order's actual broker fill
// price is the true exit price, so the recorded exit price / P&L match exactly
// what executed at the exchange. Idempotent — UpdateLiveTradeExit guards on
// live_exit_price IS NULL, so a race with an OCO/ML exit resolves cleanly.
func (s *OrderStatusService) recordSquareOffExit(ctx context.Context, sqOrder *models.Order) {
	if sqOrder.ParentOrderID == nil || sqOrder.FilledPrice == nil {
		return
	}
	entry, err := s.repo.GetByID(ctx, *sqOrder.ParentOrderID)
	if err != nil || entry == nil || entry.FilledPrice == nil {
		return
	}
	exitPrice := *sqOrder.FilledPrice
	qty := float64(sqOrder.FilledQuantity)
	if qty <= 0 {
		qty = float64(entry.FilledQuantity)
	}
	var pnl float64
	if entry.OrderSide == models.OrderSideBuy {
		pnl = (exitPrice - *entry.FilledPrice) * qty
	} else {
		pnl = (*entry.FilledPrice - exitPrice) * qty
	}
	exitTime := time.Now()
	if sqOrder.ExecutedAt != nil && !sqOrder.ExecutedAt.IsZero() {
		exitTime = *sqOrder.ExecutedAt
	}
	if err := s.repo.UpdateLiveTradeExit(ctx, entry.OrderID, exitPrice, pnl, exitTime, "SQUARE_OFF"); err != nil {
		s.logger.Warn("recordSquareOffExit: failed to record entry exit",
			zap.String("entry_order", entry.OrderID.String()), zap.Error(err))
		return
	}
	s.logger.Info("Square-off exit recorded on entry order",
		zap.String("entry_order", entry.OrderID.String()),
		zap.String("square_off_order", sqOrder.OrderID.String()),
		zap.Float64("exit_price", exitPrice),
		zap.Float64("pnl", pnl))
}

// flexToInt converts a FlexInt to int, returning 0 on failure.
func flexToInt(f indiraClient.FlexInt) int {
	v, _ := strconv.Atoi(string(f))
	return v
}

// resolveFilledQty returns the cumulative filled quantity reported by a broker WS
// event, or -1 when none can be derived (the caller then leaves filled_quantity
// unchanged).
//
// Indira normally sends TradedQTY as the cumulative fill, but on some EXECUTED/
// TRADED events it arrives empty — the fill only appears in OrderOriginalQty/
// PendingQty. Without this fallback, filled_quantity stays 0 on a genuine fill,
// which cascades into real bugs: the deactivation bulk-cancel guard
// (filled_quantity > 0) fails to protect the position so it gets CANCELLED, the
// square-off path never closes it so no exit is recorded, and enrichPositions
// treats it as broker-direct ("USER DIRECT") instead of attributing it to its
// strategy. An explicit TradedQTY (including "0") is always respected.
func resolveFilledQty(ws *indiraClient.WSOrderStatus, placedQty int) int {
	if q, err := strconv.Atoi(string(ws.TradedQTY)); err == nil {
		return q
	}
	status := strings.ToUpper(strings.TrimSpace(ws.OrderStatus))
	if status != "EXECUTED" && status != "TRADED" {
		return -1
	}
	// Fully/partially filled cumulative qty = original − still-pending.
	if d := ws.OrderOriginalQty - flexToInt(ws.PendingQty); d > 0 {
		return d
	}
	if ws.OrderOriginalQty > 0 {
		return ws.OrderOriginalQty
	}
	if placedQty > 0 {
		return placedQty
	}
	return -1
}

// fillFromTradeBook fetches the broker trade book for one order and aggregates its
// trades into the authoritative cumulative fill: total qty, VWAP price, and the
// latest trade time. The order WebSocket omits TradedQTY and every timestamp on
// EXECUTED messages, so this REST trade book is the only authoritative source for
// fill quantity and — critically — fill/exit TIME. Returns ok=false on any error,
// missing auth, or no matching trade, so the caller keeps its WS-derived fallback.
func (s *OrderStatusService) fillFromTradeBook(ctx context.Context, userID, indiraOrderID string) (qty int, price float64, ts time.Time, ok bool) {
	// Two attempts: the socket EXECUTED event can momentarily lead the broker's
	// trade book, so an empty first result is retried once after a short delay
	// before giving up (caller then keeps the WS-derived fallback).
	return s.fillFromTradeBookN(ctx, userID, indiraOrderID, 2)
}

// fillFromTradeBookN is fillFromTradeBook with a configurable attempt count.
// The real-time WS path uses 2 (the trade book can briefly lag the EXECUTED
// push). The periodic fill reconciler uses 1: there is no event to race, so an
// empty result simply means "not filled yet" and the next tick re-checks —
// retrying with a 500ms sleep there would only double broker API load.
func (s *OrderStatusService) fillFromTradeBookN(ctx context.Context, userID, indiraOrderID string, attempts int) (qty int, price float64, ts time.Time, ok bool) {
	if s.execClient == nil || indiraOrderID == "" {
		return 0, 0, time.Time{}, false
	}
	if attempts < 1 {
		attempts = 1
	}
	authVal, has := s.subscriberAuths.Load(userID)
	if !has {
		return 0, 0, time.Time{}, false
	}
	auth := authVal.(*indiraClient.AuthContext)

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		trades, err := s.execClient.GetTradeBook(cctx, auth, indiraOrderID)
		cancel()
		if err != nil {
			s.logger.Warn("trade book fetch failed — keeping WS-derived fill",
				zap.String("indira_order_id", indiraOrderID), zap.Error(err))
			return 0, 0, time.Time{}, false
		}
		if q, p, t, ok := aggregateTrades(trades, indiraOrderID); ok {
			return q, p, t, true
		}
	}
	return 0, 0, time.Time{}, false
}

// aggregateTrades reduces a trade-book response for one order into the cumulative
// fill: total qty, volume-weighted average price, and the latest trade time.
// Pure (no I/O) so it is unit-tested directly. ok=false when there is no usable
// quantity, signalling the caller to keep its WS-derived fallback values.
func aggregateTrades(trades []indiraClient.TradeBook, indiraOrderID string) (qty int, price float64, ts time.Time, ok bool) {
	var totalQty int
	var notional float64
	var latest time.Time
	for _, t := range trades {
		// Defensive: the endpoint is queried per order, but skip any stray row.
		if t.OrdId != "" && indiraOrderID != "" && !strings.EqualFold(t.OrdId, indiraOrderID) {
			continue
		}
		totalQty += t.TradedQty
		notional += float64(t.TradedQty) * t.TradedPrice
		if tt := parseTradeBookTime(t.TradeTime); tt.After(latest) {
			latest = tt
		}
	}
	if totalQty <= 0 {
		return 0, 0, time.Time{}, false
	}
	return totalQty, notional / float64(totalQty), latest, true
}

// decimalLocator returns the divisor encoded in the WS DecimalLocator field.
// E.g. "100" means raw prices must be divided by 100 to get the actual value.
// Returns 1 when the field is empty or unparseable (no-op divisor).
func decimalLocator(dl string) float64 {
	if dl == "" {
		return 1
	}
	if v, err := strconv.ParseFloat(dl, 64); err == nil && v > 0 {
		return v
	}
	return 1
}

// parseBrokerTime parses an Indira broker time string into time.Time (IST).
// Handles both colon-separated (OrderTimeStamp "15-May-2026 12:31:29")
// and dot-separated (OrderEntryTime "15-May-2026 12.31.30") formats.
func parseBrokerTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{"02-Jan-2006 15:04:05", "02-Jan-2006 15.04.05"} {
		if t, err := time.ParseInLocation(layout, s, timezone.IST); err == nil {
			return t
		}
	}
	return time.Time{}
}

// parseTradeBookTime parses the REST trade-book TradeTime format ("2026-06-01
// 14:38:41"), which differs from the WS order-event format parseBrokerTime handles.
func parseTradeBookTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", s, timezone.IST); err == nil {
		return t
	}
	return time.Time{}
}

func (s *OrderStatusService) publishNotification(ctx context.Context, order *models.Order, wsStatus *indiraClient.WSOrderStatus) {
	if s.publisher == nil {
		return
	}

	dl := decimalLocator(string(wsStatus.DecimalLocator))

	// Parse WS numeric fields
	tradedQty := 0
	if qty, err := strconv.Atoi(string(wsStatus.TradedQTY)); err == nil {
		tradedQty = qty
	}
	tradedPriceStr := ""
	if wsStatus.TradedPrice != "" && wsStatus.TradedPrice != "0" {
		if p, err := strconv.ParseFloat(wsStatus.TradedPrice, 64); err == nil && p > 0 {
			tradedPriceStr = fmt.Sprintf("₹%.2f", p)
		}
	}
	triggerPriceStr := ""
	if wsStatus.TriggerPrice > 0 {
		triggerPriceStr = fmt.Sprintf("₹%.2f", wsStatus.TriggerPrice/dl)
	}

	// Broker ref: prefer UniqueCode (system order ID), fall back to OrderNumber
	brokerRef := wsStatus.UniqueCode
	if brokerRef == "" {
		brokerRef = wsStatus.OrderNumber
	}

	// Build enriched ExecutionDetails from WS payload
	execDetails := &models.ExecutionDetails{
		ExecutedAt:      time.Now(),
		ExecutedPrice:   tradedPriceStr,
		BrokerRef:       brokerRef,
		TradedQty:       tradedQty,
		PendingQty:      flexToInt(wsStatus.PendingQty),
		OriginalQty:     wsStatus.OrderOriginalQty,
		ExchangeOrderNo: wsStatus.OrderNumber,
		OrderEntryTime:  wsStatus.OrderEntryTime,
		LastModifiedAt:  wsStatus.LastModifiedTimeStamp,
		Product:         wsStatus.Product,
		OrderValidity:   wsStatus.OrderValidity,
		Reason:          wsStatus.Reason,
		TriggerPrice:    triggerPriceStr,
	}

	update := &models.OrderUpdate{
		UpdateID:  uuid.New().String(),
		OrderID:   order.OrderID.String(),
		UserID:    order.UserID,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(1 * time.Hour),
		Status:    string(order.Status),
		OrderSummary: models.OrderSummary{
			Stock:     order.Symbol,
			Action:    string(order.OrderSide),
			Quantity:  order.Quantity,
			Exchange:  string(order.Exchange),
			OrderType: string(order.OrderType),
		},
		ExecutionDetails: execDetails,
	}
	if order.Price != nil {
		update.OrderSummary.Price = fmt.Sprintf("₹%.2f", *order.Price)
	} else {
		update.OrderSummary.Price = "MARKET"
	}

	// Use raw broker status for notification type
	brokerStatusUpper := strings.ToUpper(strings.TrimSpace(wsStatus.OrderStatus))
	switch brokerStatusUpper {
	case "EXECUTED", "TRADED":
		update.UpdateType = "EXECUTION_SUCCESS"
		update.Priority = "HIGH"
		update.Title = "Order Executed ✓"
		update.Message = fmt.Sprintf("Your %s order was executed", order.Symbol)
		if tradedPriceStr != "" {
			update.DetailedMessage = fmt.Sprintf(
				"Filled %d/%d share(s) of %s at %s on %s | Broker ref: %s | Entry: %s",
				tradedQty, wsStatus.OrderOriginalQty, order.Symbol, tradedPriceStr,
				order.Exchange, brokerRef, wsStatus.OrderEntryTime,
			)
			if t := parseBrokerTime(wsStatus.OrderEntryTime); !t.IsZero() {
				execDetails.ExecutedAt = t
			} else {
				execDetails.ExecutedAt = time.Now()
			}
		}
		update.StatusEmoji = "✅"
		update.StatusColor = "#00C851"

	case "PARTIALLY TRADED", "PARTIALLY EXECUTED":
		update.UpdateType = "PARTIAL_FILL"
		update.Priority = "MEDIUM"
		update.Title = "Order Partially Filled"
		update.Message = fmt.Sprintf("%d of %d shares filled for %s", tradedQty, wsStatus.OrderOriginalQty, order.Symbol)
		update.DetailedMessage = fmt.Sprintf(
			"Partially filled %d/%d at %s | Pending qty: %d | Broker ref: %s",
			tradedQty, wsStatus.OrderOriginalQty, tradedPriceStr, flexToInt(wsStatus.PendingQty), brokerRef,
		)
		update.StatusEmoji = "🔶"
		update.StatusColor = "#FFBB33"

	case "REJECTED", "A.REJECTED":
		update.UpdateType = "ORDER_REJECTED"
		update.Priority = "HIGH"
		update.Title = "Order Rejected ✗"
		update.Message = fmt.Sprintf("Your %s order was rejected", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Rejected by broker: %s | Ref: %s", wsStatus.Reason, brokerRef)
		update.StatusEmoji = "❌"
		update.StatusColor = "#FF4444"
		execDetails.Reason = wsStatus.Reason

	case "CANCELLED":
		update.UpdateType = "ORDER_CANCELLED"
		update.Priority = "MEDIUM"
		update.Title = "Order Cancelled"
		update.Message = fmt.Sprintf("Your %s order was cancelled", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Cancelled: %s | Ref: %s", wsStatus.Reason, brokerRef)
		update.StatusEmoji = "⚠️"
		update.StatusColor = "#FFBB33"
		execDetails.Reason = wsStatus.Reason

	case "PENDING":
		update.UpdateType = "ORDER_PENDING"
		update.Priority = "LOW"
		update.Title = "Order Pending"
		update.Message = fmt.Sprintf("Your %s order is pending", order.Symbol)
		update.DetailedMessage = fmt.Sprintf("Pending | Ref: %s | Entry: %s", brokerRef, wsStatus.OrderEntryTime)
		update.StatusEmoji = "⏳"
		update.StatusColor = "#33B5E5"

	default:
		// For any other broker status, still publish a generic notification
		update.UpdateType = "ORDER_STATUS_UPDATE"
		update.Priority = "LOW"
		update.Title = fmt.Sprintf("Order %s", brokerStatusUpper)
		update.Message = fmt.Sprintf("Your %s order status: %s", order.Symbol, brokerStatusUpper)
		update.StatusEmoji = "ℹ️"
		update.StatusColor = "#33B5E5"
	}

	if err := s.publisher.PublishOrderUpdate(ctx, update); err != nil {
		s.logger.Error("Failed to publish WS order update", zap.Error(err))
	} else {
		s.logger.Info("WS order-update published to Kafka",
			zap.String("update_type", update.UpdateType),
			zap.String("order_id", order.OrderID.String()),
			zap.String("broker_ref", brokerRef),
			zap.String("traded_price", tradedPriceStr),
			zap.Int("traded_qty", tradedQty),
			zap.Int("pending_qty", flexToInt(wsStatus.PendingQty)),
			zap.String("reason", wsStatus.Reason),
		)
	}
}
