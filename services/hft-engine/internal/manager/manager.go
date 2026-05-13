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
}

// New wires dependencies. Cheap; no goroutines started.
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
	return &Manager{
		runners:    make(map[string]*strategy.Runner),
		repo:       r,
		audit:      a,
		br:         br,
		mws:        mws,
		ows:        ows,
		engineMode: engineMode,
		logger:     logger.Named("manager"),
		rootCtx:    rootCtx,
	}
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
	m.mu.Unlock()

	if target == nil {
		// Common: events arrive for orders we cancelled milliseconds ago
		// (broker reports CANCELLED after we've already moved the chunk
		// to History). Debug-only; not an error.
		m.logger.Debug("fill event with no matching runner — ignored",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("event_type", ev.EventType))
		return
	}
	target.SendFill(ev)
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
	sym := broker.SymbolSpec{
		Symbol:      cfg.Symbol,
		ISIN:        cfg.ISIN,
		Exchange:    cfg.Exchange,
		ProductType: cfg.ProductType,
		TickSize:    cfg.TickSize,
		// ExchangeToken is empty in Phase 2 — IndiraBroker (Phase 3)
		// resolves it from external Redis at construction.
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
	// or ctx done), we unsubscribe and prune from the map.
	go func() {
		runner.Run(m.rootCtx)
		sub.Unsubscribe() // closes sub.Ticks → ends forwarder goroutine
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

// Get returns the current snapshot of one strategy. Nil if not running.
// Snapshot is a deep-copied *state.Strategy — caller can read freely
// without holding any lock.
func (m *Manager) Get(strategyID string) *state.Strategy {
	m.mu.Lock()
	runner, ok := m.runners[strategyID]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return runner.Snapshot()
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

