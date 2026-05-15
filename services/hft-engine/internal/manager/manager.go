// Package manager owns the set of currently-running strategies.
//
// Phase 2 — REAL. Start spawns a strategy.Runner goroutine that runs
// the tick loop, applies fills, and transitions through the chunk-aware
// state machine. Stop cancels the Runner and waits for it to flush
// audit + cancel any resting orders.
//
// Manager is the boundary between gRPC handlers (Entry / Exit / GetState)
// and the per-strategy Runner goroutines. Everything past this boundary
// is single-goroutine-per-strategy.
package manager

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/audit"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/broker"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/marketws"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/orderws"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/repo"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/strategy"
)

// Manager keeps a map of strategy_id → *strategy.Runner.
//
// The map mutex (mu) guards add/remove only. The Runner itself owns its
// own goroutine and publishes thread-safe snapshots — readers never need
// the map lock.
type Manager struct {
	mu      sync.Mutex
	runners map[string]*strategy.Runner

	repo     *repo.Repo
	audit    *audit.Writer
	br       broker.Broker
	mws      *marketws.Manager  // may be nil in tests
	ows      *orderws.Service   // may be nil in PAPER (no real fills to route)
	logger   *zap.Logger

	// engineMode records what kind of broker we were wired with: "PAPER"
	// or "LIVE". Used in Start to refuse Entry for a strategy whose
	// cfg.Mode disagrees — we NEVER silently demote LIVE→PAPER.
	engineMode state.Mode

	// rootCtx is the parent ctx every Runner derives from. Cancelling it
	// (in main.go on shutdown) cleanly stops every active strategy.
	rootCtx context.Context

	// pendingFills buffers FillEvents that arrived before any Runner
	// claimed their broker_order_id. The order-status WS can deliver a
	// FILL before PlaceLimit's HTTP response hands the id back to the
	// Runner — the fill literally beats the ack. reapPendingFills retries
	// routing these for a short window before giving up.
	//
	// pendingMu is independent of mu — never hold both at once. RouteFill
	// takes mu, releases it, then takes pendingMu; retryPendingFills takes
	// pendingMu, releases it, then takes mu.
	pendingMu    sync.Mutex
	pendingFills map[string][]pendingFill

	// completedSnapshots keeps the FINAL snapshot of strategies that have
	// auto-exited (max_reached, window_closed, price_band, no_data) or
	// been Stop'd. Frontends polling /state right up to the completion
	// event see the final {active:false, halt_reason, position, history}
	// for a grace window instead of a 404 the instant the strategy ends.
	// Entries TTL-evict on read (no background goroutine needed).
	//
	// completedMu is independent of mu and pendingMu — never held with either.
	completedMu        sync.Mutex
	completedSnapshots map[string]completedSnapshot
}

// pendingFill is a FillEvent buffered because no Runner owned its
// broker_order_id at arrival time. received ages it out.
type pendingFill struct {
	ev       state.FillEvent
	received time.Time
}

// completedSnapshot holds one finished strategy's last snapshot + the
// time it finished. Get serves it during the post-mortem window.
type completedSnapshot struct {
	snap       *state.Strategy
	finishedAt time.Time
}

// completedSnapshotTTL bounds how long a finished strategy's snapshot is
// kept in memory after the runner exits. Long enough for the frontend to
// poll a couple of times and render the completion; short enough that
// the map doesn't grow without bound.
const completedSnapshotTTL = 10 * time.Minute

// New wires dependencies and starts the pending-fill reaper goroutine
// (process-lifetime; exits when rootCtx is cancelled).
//
// rootCtx is typically context.Background() in tests and the
// process-lifetime ctx in main.go.
//
// mws may be nil — when nil, manager.Start refuses Entry with a clear
// error rather than starting a strategy that can never receive ticks.
// In tests we pass a stub or skip the marketws layer entirely.
func New(rootCtx context.Context, r *repo.Repo, a *audit.Writer, br broker.Broker, mws *marketws.Manager, ows *orderws.Service, engineMode state.Mode, logger *zap.Logger) *Manager {
	if engineMode == "" {
		engineMode = state.ModePaper
	}
	m := &Manager{
		runners:            make(map[string]*strategy.Runner),
		repo:               r,
		audit:              a,
		br:                 br,
		mws:                mws,
		ows:                ows,
		engineMode:         engineMode,
		logger:             logger.Named("manager"),
		rootCtx:            rootCtx,
		pendingFills:       make(map[string][]pendingFill),
		completedSnapshots: make(map[string]completedSnapshot),
	}
	go m.reapPendingFills()
	return m
}

// RouteFill is the orderws.Router callback. Called from the orderws read
// goroutine for every broker order event. We scan running strategies for
// one that owns this broker_order_id and dispatch via Runner.SendFill.
//
// O(N) over running strategies. N is small (< 100 in practice) — a map
// would shave microseconds but add bookkeeping; revisit only if profiling
// shows this as a hotspot.
func (m *Manager) RouteFill(ev state.FillEvent) {
	m.mu.Lock()
	var target *strategy.Runner
	for _, r := range m.runners {
		if r.OwnsBrokerOrderID(ev.BrokerOrderID) {
			target = r
			break
		}
	}
	runnerCount := len(m.runners)
	m.mu.Unlock()

	if target != nil {
		target.SendFill(ev)
		return
	}

	// Indira's order WSS streams EVERY order on the user's account —
	// including orders placed by other services (Manthan, trade-execution,
	// the replayer, ...). With no HFT runner active, nothing here could
	// ever own this event, so there is nothing to buffer for: drop it.
	if runnerCount == 0 {
		m.logger.Debug("fill event ignored — no strategies running",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("event_type", ev.EventType))
		return
	}

	// A runner IS active but none owns this id yet. Usual cause is a
	// race: the order-status WS delivered the FILL before PlaceLimit's
	// HTTP response handed the id back to the Runner (the fill beat the
	// ack). Buffer it — reapPendingFills retries routing every 10ms.
	// Foreign-account events that never get claimed age out after
	// pendingFillTTL and are dropped.
	m.pendingMu.Lock()
	m.pendingFills[ev.BrokerOrderID] = append(m.pendingFills[ev.BrokerOrderID],
		pendingFill{ev: ev, received: time.Now()})
	m.pendingMu.Unlock()
	m.logger.Debug("fill event buffered — no matching runner yet",
		zap.String("broker_order_id", ev.BrokerOrderID),
		zap.String("event_type", ev.EventType))
}

// pendingFillTTL bounds how long a buffered fill is retried before we
// give up on it. The place-vs-fill race resolves in well under a second
// (place-order HTTP responses return in ~300ms); 2s covers it with
// margin while getting foreign-account events out of the buffer fast.
const pendingFillTTL = 2 * time.Second

// reapPendingFills runs for the life of the process. Every 10ms it
// retries routing buffered FillEvents whose Runner has since registered
// its broker_order_id. Exits when rootCtx is cancelled.
//
// When the buffer is empty (the common case) a tick is just one mutex
// lock + length check — cheap enough to run at 10ms.
func (m *Manager) reapPendingFills() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.rootCtx.Done():
			return
		case <-ticker.C:
			m.retryPendingFills()
		}
	}
}

// retryPendingFills re-attempts routing for every buffered fill. A fill
// whose Runner now owns the id is delivered; one older than pendingFillTTL
// with still no owner is dropped with a WARN.
//
// Lock discipline: detach the buffer under pendingMu, release it, THEN
// scan runners under mu — never hold both locks at once (RouteFill
// acquires them in the reverse-free order).
func (m *Manager) retryPendingFills() {
	m.pendingMu.Lock()
	if len(m.pendingFills) == 0 {
		m.pendingMu.Unlock()
		return
	}
	batch := m.pendingFills
	m.pendingFills = make(map[string][]pendingFill)
	m.pendingMu.Unlock()

	now := time.Now()
	leftover := make(map[string][]pendingFill)

	for id, events := range batch {
		m.mu.Lock()
		var target *strategy.Runner
		for _, r := range m.runners {
			if r.OwnsBrokerOrderID(id) {
				target = r
				break
			}
		}
		m.mu.Unlock()

		if target != nil {
			for _, pf := range events {
				target.SendFill(pf.ev)
			}
			continue
		}

		// Still unclaimed — keep the ones inside the TTL, drop the rest.
		// A drop is normally just a foreign-account order event (another
		// service placed it), so this is Debug, not Warn.
		for _, pf := range events {
			if now.Sub(pf.received) < pendingFillTTL {
				leftover[id] = append(leftover[id], pf)
			} else {
				m.logger.Debug("dropping orphan fill event — no runner claimed it (likely foreign-account order)",
					zap.String("broker_order_id", id),
					zap.String("event_type", pf.ev.EventType),
					zap.Duration("age", now.Sub(pf.received)))
			}
		}
	}

	if len(leftover) > 0 {
		m.pendingMu.Lock()
		for id, evs := range leftover {
			// leftover entries are older than anything RouteFill appended
			// while we were scanning — keep them first to preserve order.
			m.pendingFills[id] = append(evs, m.pendingFills[id]...)
		}
		m.pendingMu.Unlock()
	}
}

// Start begins running a strategy. Loads config + credentials, builds a
// Runner, spawns its goroutine, registers in the map.
//
// Returns ErrAlreadyRunning if the strategy is already active.
// Returns the underlying error if config load, creds load, or symbol
// resolution failed.
func (m *Manager) Start(ctx context.Context, strategyID, sideOverride string, lotsOverride int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.runners[strategyID]; exists {
		return ErrAlreadyRunning
	}

	cfg, err := m.repo.LoadConfig(ctx, strategyID)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyOverrides(cfg, sideOverride, lotsOverride)

	// Mode safety gate. The strategy's cfg.Mode and the engine-wide
	// engineMode must agree. We NEVER silently demote LIVE→PAPER (would
	// be a violation of user intent), and never silently promote
	// PAPER→LIVE (would risk real money on a strategy meant to be paper).
	if cfg.Mode != m.engineMode {
		return fmt.Errorf("mode mismatch: strategy.Mode=%q, engine.Mode=%q — refusing Entry",
			cfg.Mode, m.engineMode)
	}

	creds, err := m.repo.LoadCredentials(ctx, cfg.UserID)
	if err != nil {
		// Paper mode can technically run without broker creds — but
		// we still need them in case the user flips mode mid-day, and
		// for the symbol-token resolution in Phase 3+. Fail fast.
		return fmt.Errorf("load credentials: %w", err)
	}

	auth := &broker.AuthContext{
		UserID:      creds.UserID,
		AppID:       creds.AppID,
		Source:      creds.Source,
		BearerToken: creds.BearerToken,
	}

	// Subscribe this user to the order-status WSS so we receive fill
	// events for orders we'll place. Only in LIVE mode — PaperBroker
	// doesn't touch Indira so there are no real fills to route. The
	// subscribe is idempotent (other strategies for the same user reuse
	// the existing subscription).
	if m.engineMode == state.ModeLive && m.ows != nil {
		indiraAuth := &indira.AuthContext{
			UserId:      creds.UserID,
			AppId:       creds.AppID,
			Source:      creds.Source,
			BearerToken: creds.BearerToken,
		}
		if oerr := m.ows.SubscribeUser(ctx, indiraAuth); oerr != nil {
			return fmt.Errorf("order-status WS subscribe: %w", oerr)
		}
	}
	// Subscribe to the ODIN broadcast feed for this symbol BEFORE we
	// spawn the runner — if subscribe fails (symbol not in external
	// Redis, ODIN feed down), Entry fails fast with a clear error.
	// Doing this before runner creation keeps the failure path simple:
	// no orphan goroutine to tear down.
	if m.mws == nil {
		return fmt.Errorf("market-data feed not configured — refusing Entry")
	}
	if !m.mws.FeedAlive() {
		return fmt.Errorf("market-data feed disconnected — refusing Entry until reconnect")
	}
	sub, err := m.mws.Subscribe(ctx, strategyID, cfg.ISIN, cfg.Symbol)
	if err != nil {
		return fmt.Errorf("marketws subscribe: %w", err)
	}

	// SymbolSpec — built AFTER Subscribe so ExchangeToken can be
	// populated from the resolved security code. IndiraBroker composes
	// "STK_{Symbol}_EQ_{Exchange}_{ExchangeToken}" for every order; an
	// empty token would produce a malformed symbol the broker rejects.
	sym := broker.SymbolSpec{
		Symbol:        cfg.Symbol,
		ISIN:          cfg.ISIN,
		Exchange:      cfg.Exchange,
		ExchangeToken: strconv.FormatInt(sub.SecurityCode, 10),
		ProductType:   cfg.ProductType,
		TickSize:      cfg.TickSize,
	}

	runner := strategy.NewRunner(*cfg, auth, sym, m.br, m.audit, m.logger)
	m.runners[strategyID] = runner

	// Forwarder goroutine: pipes ticks from the marketws subscription
	// into the runner's tickCh. Exits when sub.Ticks is closed (i.e.
	// Unsubscribe was called).
	go func() {
		for md := range sub.Ticks {
			runner.SendTick(md)
		}
	}()

	// Spawn the runner goroutine. When Run exits (Stop, both-sides-done,
	// or ctx done), we capture the final snapshot for the post-mortem
	// grace window, unsubscribe, and prune from the live runner map.
	go func() {
		runner.Run(m.rootCtx)
		sub.Unsubscribe() // closes sub.Ticks → ends forwarder goroutine

		// Stash the final snapshot so /state can serve it for
		// completedSnapshotTTL after the strategy auto-exits — without
		// this the frontend gets a 404 the instant max_reached fires
		// and can't render the completion.
		if final := runner.Snapshot(); final != nil {
			final.Active = false
			m.completedMu.Lock()
			m.completedSnapshots[strategyID] = completedSnapshot{
				snap:       final,
				finishedAt: time.Now(),
			}
			m.completedMu.Unlock()
		}

		m.mu.Lock()
		if m.runners[strategyID] == runner {
			delete(m.runners, strategyID)
		}
		m.mu.Unlock()
	}()

	m.logger.Info("strategy STARTED",
		zap.String("strategy_id", strategyID),
		zap.String("user_id", cfg.UserID),
		zap.String("symbol", cfg.Symbol),
		zap.String("mode", string(cfg.Mode)),
		zap.String("side", string(cfg.Side)),
		zap.Int("max_buy_qty", cfg.MaxBuyQty),
		zap.Int("max_sell_qty", cfg.MaxSellQty),
		zap.Int64("security_code", sub.SecurityCode))
	return nil
}

// Stop signals the Runner to halt and waits up to 5 seconds for it to
// finish (cancel resting orders + flush audit). The Runner removes
// itself from the map on exit; we also remove it here defensively.
func (m *Manager) Stop(ctx context.Context, strategyID string) error {
	m.mu.Lock()
	runner, exists := m.runners[strategyID]
	if !exists {
		m.mu.Unlock()
		return ErrNotRunning
	}
	// Note: we don't delete from the map here — the Runner's exit goroutine
	// (spawned in Start) does that. Releasing the lock first so Stop can
	// wait without blocking other RPCs.
	m.mu.Unlock()

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	runner.Stop(stopCtx)

	m.logger.Info("strategy STOPPED", zap.String("strategy_id", strategyID))
	return nil
}

// Get returns the current snapshot of one strategy. Nil if neither
// running nor in the recently-completed cache.
//
// For a still-running strategy the snapshot has Active=true and live
// position/current-chunk data. For one that has just finished, Get falls
// through to completedSnapshots and returns the final {Active:false,
// halt_reason, position, history} for completedSnapshotTTL after exit —
// so the frontend's polling loop sees the completion, not a 404.
//
// Snapshot is a deep-copied *state.Strategy — caller can read freely
// without holding any lock.
func (m *Manager) Get(strategyID string) *state.Strategy {
	m.mu.Lock()
	runner, ok := m.runners[strategyID]
	m.mu.Unlock()
	if ok {
		return runner.Snapshot()
	}

	// Not running — try the post-mortem cache, evicting on read if expired.
	m.completedMu.Lock()
	cs, found := m.completedSnapshots[strategyID]
	if found && time.Since(cs.finishedAt) > completedSnapshotTTL {
		delete(m.completedSnapshots, strategyID)
		found = false
	}
	m.completedMu.Unlock()
	if found {
		return cs.snap
	}
	return nil
}

// GetRunner exposes the live Runner for producers (market WS in Phase 4,
// SubmitFills bridge in Phase 5) that need to call SendTick / SendFill.
// Returns nil if not running.
func (m *Manager) GetRunner(strategyID string) *strategy.Runner {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runners[strategyID]
}

// Count returns the number of running strategies.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.runners)
}

// ── Errors ────────────────────────────────────────────────────────────────

type StartErr string

func (e StartErr) Error() string { return string(e) }

const (
	ErrAlreadyRunning = StartErr("strategy already running")
	ErrNotRunning     = StartErr("strategy not running")
)

// ── Helpers ───────────────────────────────────────────────────────────────

// applyOverrides mirrors the C++ template's handle_entry side/lots logic.
func applyOverrides(cfg *state.Config, side string, lots int) {
	if side == "" {
		return
	}
	cfg.Side = state.Side(side)
	if lots <= 0 {
		return
	}
	switch side {
	case "BUY":
		cfg.MaxBuyQty = lots
		cfg.MaxSellQty = 0
	case "SELL":
		cfg.MaxSellQty = lots
		cfg.MaxBuyQty = 0
	case "BOTH":
		cfg.MaxBuyQty = lots
		cfg.MaxSellQty = lots
	}
}

