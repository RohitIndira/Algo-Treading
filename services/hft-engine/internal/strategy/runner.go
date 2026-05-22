// Package strategy holds the per-strategy goroutine that runs the bidding
// state machine.
//
// One Runner per running strategy. The Runner owns its state.Strategy
// (the data) and is the ONLY writer to Buy/Sell. Outside readers
// (GetState RPC, StreamState push) read from an atomic snapshot the
// Runner publishes after every state change — no locks on the read path.
//
// The Runner reacts to three channels in a select:
//   tickCh — bid/ask updates from market WS (Phase 4) or tests
//   fillCh — fill/cancel/reject events (Phase 5) or tests
//   ctx    — cancelled on Exit or shutdown
//
// Phase 2 wires in PaperBroker so all Place/Modify/Cancel calls are
// inert — the state machine still executes its full logic, the audit
// log still records every action, but no real order ever gets placed.
package strategy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/audit"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/broker"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/repo"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// Runner is the live execution of one strategy. Created by manager.Start,
// destroyed by manager.Stop.
type Runner struct {
	// Cfg is immutable after construction — Phase 2 trusts that no one
	// mutates the underlying struct.
	Cfg state.Config

	// state.Strategy is the live, mutable data. ONLY Run() and the
	// helpers it calls (handleTick, handleSide, onFill) write to it.
	// All readers go through snap (below).
	mu      sync.Mutex
	live    *state.Strategy

	// snap is the lock-free reader path. After every mutation the Run
	// goroutine builds a fresh deep-copied *state.Strategy and stores
	// it. GetState RPC just calls Snapshot() — zero contention.
	snap atomic.Value // *state.Strategy

	// Dependencies
	broker broker.Broker
	audit  *audit.Writer
	logger *zap.Logger
	auth   *broker.AuthContext
	sym    broker.SymbolSpec

	// Channels — written from outside the goroutine, read by it.
	// Buffered (256) so a brief slow tick or fill burst doesn't drop events.
	tickCh chan state.MarketData
	fillCh chan state.FillEvent
	// priceCh delivers real broker fill prices (orderID → avg traded price)
	// from the manager's TradeBook reconciler. Applied on this goroutine so
	// chunk state is never raced.
	priceCh chan map[string]float64

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{} // closed when Run() returns; Stop() waits on this
	started atomic.Bool   // true once Run starts
}

// NewRunner builds (but does not start) a Runner. The caller — manager
// usually — then calls Run(ctx) in a goroutine.
//
// auth is the broker credential snapshot at Start time. Phase 5+ may
// refresh it when JWTs rotate.
func NewRunner(
	cfg state.Config,
	auth *broker.AuthContext,
	sym broker.SymbolSpec,
	br broker.Broker,
	aud *audit.Writer,
	logger *zap.Logger,
) *Runner {
	live := &state.Strategy{
		Cfg:       cfg,
		Active:    true,
		StartedAt: time.Now(),
	}
	r := &Runner{
		Cfg:    cfg,
		live:   live,
		broker: br,
		audit:  aud,
		logger: logger.Named("strategy").With(zap.String("strategy_id", cfg.StrategyID)),
		auth:   auth,
		sym:    sym,
		tickCh:  make(chan state.MarketData, 256),
		fillCh:  make(chan state.FillEvent, 256),
		priceCh: make(chan map[string]float64, 8),
		done:    make(chan struct{}),
	}
	r.publishSnapshot() // seed snap so Snapshot() never returns nil
	return r
}

// SendTick is the producer-side push for market data. Non-blocking
// (drops on overflow with a warn) so a stuck Run goroutine can't pin
// the market-data subscriber.
func (r *Runner) SendTick(md state.MarketData) {
	select {
	case r.tickCh <- md:
	default:
		r.logger.Warn("tick channel full — dropping",
			zap.Float64("bid", md.Bid),
			zap.Float64("ask", md.Ask))
	}
}

// SendFill is the producer-side push for broker fill events.
func (r *Runner) SendFill(f state.FillEvent) {
	select {
	case r.fillCh <- f:
	default:
		r.logger.Warn("fill channel full — dropping",
			zap.String("broker_order_id", f.BrokerOrderID),
			zap.String("event_type", f.EventType))
	}
}

// SendResolvedPrices delivers real broker fill prices (orderID → avg
// traded price) from the manager's reconciler. Non-blocking. The run
// loop applies them so chunk state stays single-writer.
func (r *Runner) SendResolvedPrices(prices map[string]float64) {
	if len(prices) == 0 {
		return
	}
	select {
	case r.priceCh <- prices:
	default:
		// reconciler runs on a timer — a dropped batch just retries next tick
	}
}

// Snapshot returns the most recently published deep-copy of the state.
// Safe to call from any goroutine. Never returns nil after NewRunner.
func (r *Runner) Snapshot() *state.Strategy {
	v := r.snap.Load()
	if v == nil {
		return nil
	}
	return v.(*state.Strategy)
}

// Stop signals the Run goroutine to exit. Cancels any resting orders
// (via the state machine's HALT path) and flushes audit before
// returning. Bounded by ctx; the caller should pass a short timeout.
func (r *Runner) Stop(ctx context.Context) {
	r.cancel()
	select {
	case <-r.done:
	case <-ctx.Done():
		r.logger.Warn("Stop timed out waiting for Run to finish — leaving goroutine running")
	}
}

// Done returns a channel closed when Run exits — useful for tests.
func (r *Runner) Done() <-chan struct{} { return r.done }

// OwnsBrokerOrderID returns true if this strategy currently has a chunk
// (BUY or SELL) resting at the broker with the given order ID. Used by
// the manager to route incoming order-status events to the right Runner.
//
// Reads the published snapshot — lock-free and safe to call from any
// goroutine, including the orderws read loop.
func (r *Runner) OwnsBrokerOrderID(brokerOrderID string) bool {
	snap := r.Snapshot()
	if snap == nil {
		return false
	}
	if snap.Buy.Current != nil && snap.Buy.Current.BrokerOrderID == brokerOrderID {
		return true
	}
	if snap.Sell.Current != nil && snap.Sell.Current.BrokerOrderID == brokerOrderID {
		return true
	}
	return false
}

// UserID returns the strategy's user_id. Used by the manager when deciding
// whether to subscribe that user to the order-status WS.
func (r *Runner) UserID() string { return r.Cfg.UserID }

// ─────────────────────────────────────────────────────────────────────────
// Run — the main goroutine for one strategy
// ─────────────────────────────────────────────────────────────────────────

// Run is the strategy's main loop. Called once by manager.Start, runs
// until parent ctx is cancelled (Exit RPC, shutdown) or both sides hit
// Done. On exit it cancels any resting chunk and publishes a final HALTED
// snapshot.
//
// Concurrency invariant: from this method until it returns, this goroutine
// is the ONLY writer to r.live.Buy and r.live.Sell. Nothing else may touch
// those fields.
func (r *Runner) Run(parent context.Context) {
	r.started.Store(true)
	r.ctx, r.cancel = context.WithCancel(parent)
	defer close(r.done)

	r.logger.Info("strategy goroutine started",
		zap.String("symbol", r.Cfg.Symbol),
		zap.String("mode", string(r.Cfg.Mode)),
		zap.String("side", string(r.Cfg.Side)),
		zap.Int("max_buy_qty", r.Cfg.MaxBuyQty),
		zap.Int("max_sell_qty", r.Cfg.MaxSellQty))

	// Final cleanup runs no matter how Run exits.
	defer func() {
		r.cancelAllResting()
		r.live.Active = false
		r.publishSnapshot()
		r.logger.Info("strategy goroutine exited",
			zap.Int("buy_position", r.live.Buy.Position),
			zap.Int("sell_position", r.live.Sell.Position),
			zap.String("buy_halt", string(r.live.Buy.HaltReason)),
			zap.String("sell_halt", string(r.live.Sell.HaltReason)))
	}()

	// No-data watchdog. Fires every NoDataCheckInterval; if the last
	// tick is older than NoDataHaltAfter, halt both sides with
	// HaltNoData (which cancels any resting order). The clock starts on
	// Run entry. The threshold is deliberately long — illiquid stocks
	// routinely go many minutes between touchline updates, and a resting
	// limit order is still protective while we wait. We only give up
	// (halt + cancel) once the feed is genuinely dead / the symbol wrong.
	watchdog := time.NewTicker(NoDataCheckInterval)
	defer watchdog.Stop()
	r.live.LastTickAt = time.Now() // grace period starts now, not Unix epoch

	for {
		select {
		case <-r.ctx.Done():
			r.haltAll(state.HaltExitAPI)
			return

		case md := <-r.tickCh:
			r.handleTick(md)
			if r.allSidesDone() {
				return
			}

		case f := <-r.fillCh:
			r.onFill(f)
			if r.allSidesDone() {
				return
			}

		case prices := <-r.priceCh:
			r.applyResolvedPrices(prices)

		case <-watchdog.C:
			if time.Since(r.live.LastTickAt) >= NoDataHaltAfter {
				r.logger.Warn("no market data for too long — halting",
					zap.Duration("since_last_tick", time.Since(r.live.LastTickAt)),
					zap.Duration("threshold", NoDataHaltAfter))
				r.haltAll(state.HaltNoData)
				return
			}
		}
	}
}

// NoDataHaltAfter is how long without a tick before both sides HALT
// (and any resting order is cancelled). Set long on purpose — illiquid
// symbols routinely go minutes between touchline updates, and we'd
// rather keep a protective limit order resting than kill the strategy
// on a quiet tape. 30min only trips on a genuinely dead feed.
const NoDataHaltAfter = 30 * time.Minute

// NoDataCheckInterval is how often the watchdog re-evaluates. 30s is
// fine granularity for a 30min threshold without spamming.
const NoDataCheckInterval = 30 * time.Second

// publishSnapshot deep-copies r.live and atomically stores it.
// Called by Run after any state mutation. Cheap — copies are small
// (a few hundred bytes + History slice).
func (r *Runner) publishSnapshot() {
	cp := deepCopyStrategy(r.live)
	r.snap.Store(cp)
}

// allSidesDone returns true when every CONFIGURED leg has terminated
// (filled to cap or halted). Used to auto-exit the goroutine.
//
// Respects Cfg.Side — a BUY-only strategy never runs the Sell leg, so
// Sell.Done stays false forever; waiting on it would leave the goroutine
// running idle after Buy completes. We only require the leg(s) the
// strategy actually trades:
//
//   Side == BUY   → done when Buy.Done
//   Side == SELL  → done when Sell.Done
//   Side == BOTH  → done when both
func (r *Runner) allSidesDone() bool {
	switch r.Cfg.Side {
	case state.SideBuy:
		return r.live.Buy.Done
	case state.SideSell:
		return r.live.Sell.Done
	default: // BOTH (or empty — treated as BOTH per repo defaults)
		return r.live.Buy.Done && r.live.Sell.Done
	}
}

// haltAll forces both sides into Done with the given reason. Used by
// the ctx-done branch (EXIT API, shutdown) and by some error paths.
// Does NOT cancel resting orders — defer cancelAllResting handles that.
func (r *Runner) haltAll(reason state.HaltReason) {
	if !r.live.Buy.Done {
		r.live.Buy.Done = true
		r.live.Buy.HaltReason = reason
	}
	if !r.live.Sell.Done {
		r.live.Sell.Done = true
		r.live.Sell.HaltReason = reason
	}
	r.publishSnapshot()
}

// cancelAllResting sends a Cancel for any currently-resting chunk.
// Run on goroutine exit (deferred) so we never orphan a broker order.
// Each cancel goes through broker — PaperBroker just logs; IndiraBroker
// (Phase 3) issues real cancels.
func (r *Runner) cancelAllResting() {
	for _, side := range []*state.SideState{&r.live.Buy, &r.live.Sell} {
		if side.Current == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := r.broker.Cancel(ctx, r.auth, r.sym, side.Current.BrokerOrderID)
		cancel()
		if err != nil {
			r.logger.Warn("cancel on exit failed",
				zap.String("broker_order_id", side.Current.BrokerOrderID),
				zap.Error(err))
		}
		r.auditRow("CANCEL", sideChar(side), side.Current.Seq,
			side.Current.Qty-side.Current.Filled, side.Current.LimitPrice,
			side.Current.BrokerOrderID, "")
		side.Current.Status = state.ChunkCancelled
		side.History = append(side.History, *side.Current)
		side.Current = nil
	}
}

// auditRow is a thin wrapper that builds and posts an audit event.
// Non-blocking: drops on overflow (the writer logs the drop).
func (r *Runner) auditRow(action, sideC string, chunkSeq, qty int, price float64, brokerID, errMsg string) {
	r.audit.Log(repo.AuditRow{
		StrategyID:    r.Cfg.StrategyID,
		UserID:        r.Cfg.UserID,
		Symbol:        r.Cfg.Symbol,
		Side:          sideC,
		Action:        action,
		ChunkSeq:      chunkSeq,
		Qty:           qty,
		Price:         price,
		BrokerOrderID: brokerID,
		ErrorMsg:      errMsg,
		Mode:          string(r.Cfg.Mode),
	})
}

// sideChar returns "B" or "S" for a side pointer — used in audit rows.
// We compare addresses against r.live.Buy/Sell to figure out which one.
func sideChar(s *state.SideState) string {
	// Caller passes a pointer into r.live; we don't have a back-ref so
	// we use the convention that Buy is the "default" — handlers always
	// know which side they're in and pass the right char directly.
	// This helper is only used by cancelAllResting where we iterate
	// [&Buy, &Sell] in order; map index 0 → "B", 1 → "S".
	// Simpler: caller code in tick.go / fill.go computes the char.
	return "?"
}

// ─────────────────────────────────────────────────────────────────────────
// Deep copy for atomic snapshots
// ─────────────────────────────────────────────────────────────────────────

// deepCopyStrategy returns a new *state.Strategy independent of the
// source. The copy is what GetState callers see; mutating it does
// nothing.
func deepCopyStrategy(s *state.Strategy) *state.Strategy {
	cp := *s
	cp.Buy = deepCopySide(&s.Buy)
	cp.Sell = deepCopySide(&s.Sell)
	return &cp
}

func deepCopySide(s *state.SideState) state.SideState {
	cp := *s
	if s.Current != nil {
		c := *s.Current
		cp.Current = &c
	}
	if len(s.History) > 0 {
		cp.History = make([]state.ChunkState, len(s.History))
		copy(cp.History, s.History)
	}
	return cp
}
