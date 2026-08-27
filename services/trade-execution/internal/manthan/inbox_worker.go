package manthan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// InboxWorker drains signal_inbox in a worker pool. The Kafka consumer's
// only job is to durably persist messages here; this worker is where the
// actual broker calls happen. See migration 012 for the design rationale.
//
// Concurrency model: N goroutines call ClaimDueInboxRows in a loop. The
// claim query uses FOR UPDATE SKIP LOCKED so workers never race on the
// same row. After processing, each worker marks the row DONE / FAILED /
// DLQ exactly once.
//
// Backoff policy (per attempt):
//
//	AUTH_EXPIRED   30 s     — gate-driven; the auth-restored hook also
//	                           wakes us early via the workCh.
//	BROKER_REJECT  exp      — 1 s, 2 s, 4 s … capped at 5 min, max 50.
//	TRANSIENT      exp      — same as BROKER_REJECT.
//	POISON         DLQ      — never retry; operator pages.
//	UPPER_CIRCUIT  retry    — flat 3min, attempts NOT counted; entry is
//	                          placeable the moment the band releases. Ends
//	                          via the 15:20 market-close pre-check (POISON).
type InboxWorker struct {
	repo      *Repository
	entry     *EntryHandler
	sl        *SLHandler
	authNotif AuthExpiryNotifier
	logger    *zap.Logger

	workers      int
	pollInterval time.Duration
	maxAttempts  int

	// workCh is a low-cardinality wake signal. Anyone who knows that work
	// just became processable (auth refresh, freshly-inserted row) can
	// non-blocking-send into it; the next idle worker tick wakes early.
	workCh chan struct{}

	stopOnce sync.Once
	stopped  chan struct{}
}

// InboxWorkerConfig — tunables; sensible defaults applied for zero values.
type InboxWorkerConfig struct {
	Workers      int           // default 4
	PollInterval time.Duration // default 2s — how often each worker re-checks for due rows
	MaxAttempts  int           // default 50 — beyond this → DLQ
}

func NewInboxWorker(
	repo *Repository,
	entry *EntryHandler,
	sl *SLHandler,
	authNotif AuthExpiryNotifier,
	logger *zap.Logger,
	cfg InboxWorkerConfig,
) *InboxWorker {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 50
	}
	return &InboxWorker{
		repo:         repo,
		entry:        entry,
		sl:           sl,
		authNotif:    authNotif,
		logger:       logger,
		workers:      cfg.Workers,
		pollInterval: cfg.PollInterval,
		maxAttempts:  cfg.MaxAttempts,
		workCh:       make(chan struct{}, 1),
		stopped:      make(chan struct{}),
	}
}

// SetAuthExpiryNotifier wires the gate so the worker can pre-flight check
// before each row and skip wasted broker calls during a session-expired
// window. Optional — nil-safe via authGated().
func (w *InboxWorker) SetAuthExpiryNotifier(n AuthExpiryNotifier) {
	if w == nil {
		return
	}
	w.authNotif = n
}

// Notify sends a non-blocking wake signal so the next worker tick fires
// immediately. Called when:
//
//   - The Kafka consumer just inserted a new row (skip the poll wait)
//   - The credentials cache invalidator cleared a user's auth gate (the
//     pending AUTH_EXPIRED rows are now eligible)
func (w *InboxWorker) Notify() {
	if w == nil {
		return
	}
	select {
	case w.workCh <- struct{}{}:
	default:
		// Channel buffered to 1 — a wake is already pending, drop this one.
	}
}

// Start spins up the worker pool. Blocks until ctx is cancelled.
func (w *InboxWorker) Start(ctx context.Context) {
	if w == nil {
		return
	}
	w.logger.Info("Inbox worker starting",
		zap.Int("workers", w.workers),
		zap.Duration("poll", w.pollInterval),
		zap.Int("max_attempts", w.maxAttempts))

	// Reaper: any RUNNING row left over from a previous (crashed) process
	// belongs to nobody; sweep it back to FAILED so a fresh worker picks
	// it up. 60s is comfortably longer than any single broker call.
	if n, err := w.repo.ResetStuckInboxRows(ctx, 60*time.Second); err != nil {
		w.logger.Warn("Inbox reaper: reset failed", zap.Error(err))
	} else if n > 0 {
		w.logger.Info("Inbox reaper: requeued orphaned RUNNING rows",
			zap.Int64("count", n))
	}

	// Same-day expiry: entry signals never survive their trading date —
	// yesterday's signal must not place today (prices, allocation and
	// eligibility were computed for yesterday's session). Swept at startup
	// and every 5 minutes so held/auth-parked rows die at the day boundary.
	expireStale := func() {
		now := time.Now().In(istLocation())
		todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, istLocation())
		if n, err := w.repo.ExpireStaleEntrySignals(ctx, todayStart); err != nil {
			w.logger.Warn("Inbox expiry sweep failed", zap.Error(err))
		} else if n > 0 {
			w.logger.Info("Inbox expiry sweep: dead-lettered stale entry signals (same-day-only rule)",
				zap.Int64("count", n))
		}
	}
	expireStale()
	go func() {
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				expireStale()
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < w.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.runOne(ctx, id)
		}(i)
	}

	// Periodic reaper — catches rows stuck in RUNNING due to a process
	// kill mid-handler (the worker can't update status if it died).
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(2 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := w.repo.ResetStuckInboxRows(ctx, 60*time.Second); err != nil {
					w.logger.Warn("Inbox reaper tick: reset failed", zap.Error(err))
				} else if n > 0 {
					w.logger.Info("Inbox reaper: requeued orphaned RUNNING rows",
						zap.Int64("count", n))
					w.Notify()
				}
			}
		}
	}()

	wg.Wait()
	close(w.stopped)
	w.logger.Info("Inbox worker stopped")
}

// Stop is idempotent — used by the lifecycle shutdown to wait for the pool.
func (w *InboxWorker) Stop() error {
	if w == nil {
		return nil
	}
	w.stopOnce.Do(func() {
		// Worker goroutines exit on ctx.Done; this is just a wait-for-clean.
		// Caller is expected to cancel the ctx separately.
	})
	<-w.stopped
	return nil
}

// runOne is a single worker loop: claim → dispatch → mark.
func (w *InboxWorker) runOne(ctx context.Context, id int) {
	for {
		// Claim a small batch — workers compete fairly via SKIP LOCKED.
		// Batch size 4 keeps end-to-end latency low while amortising the
		// claim query cost.
		rows, err := w.repo.ClaimDueInboxRows(ctx, 4)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("Inbox claim failed", zap.Int("worker", id), zap.Error(err))
		}
		for _, row := range rows {
			w.processRow(ctx, row)
		}

		// Nothing claimed → wait for poll tick or wake signal.
		if len(rows) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-w.workCh:
				// Drained; loop again.
			case <-time.After(w.pollInterval):
			}
		}
	}
}

// processRow runs the handler for a single inbox row and marks the row's
// terminal status. Never panics — handler errors are classified, never
// leaked to the goroutine.
func (w *InboxWorker) processRow(ctx context.Context, row *InboxRow) {
	// Pre-flight gate: if the user is mid-AU004 window, fail fast with
	// AUTH_EXPIRED so we don't waste a broker call we know will return AU0xx.
	if authGated(w.authNotif, row.UserID) {
		// A row that was HOLDING for upper-circuit relief keeps its hold
		// class through an auth outage: re-marking it AUTH_EXPIRED would
		// burn an attempt every 30s (review finding: a >25-min AU004 spell
		// would DLQ the hold mid-session). Keeping UPPER_CIRCUIT preserves
		// the no-burn policy and the 3-min cadence; the on-login wake plus
		// that cadence resumes the hold within ~3 min of re-login.
		if isHoldClass(row.LastErrorClass) {
			w.markFailed(ctx, row, row.LastErrorClass,
				"auth gate while on hold ("+row.LastErrorClass+") — session expired")
			return
		}
		w.markFailed(ctx, row, InboxErrAuthExpired,
			"auth gate set — broker session expired")
		return
	}

	w.logger.Info("Inbox row claimed",
		zap.Int64("id", row.ID),
		zap.String("signal_id", row.SignalID),
		zap.String("user", row.UserID),
		zap.String("type", row.OrderType),
		zap.Int("attempts", row.Attempts))

	handlerErr := w.dispatch(ctx, row)

	if handlerErr == nil {
		if err := w.repo.MarkInboxDone(ctx, row.ID); err != nil {
			w.logger.Warn("Inbox mark DONE failed",
				zap.Int64("id", row.ID), zap.Error(err))
		} else {
			w.logger.Info("Inbox row done",
				zap.Int64("id", row.ID),
				zap.String("signal_id", row.SignalID),
				zap.String("type", row.OrderType))
		}
		return
	}

	// DB-level race defense (migration 016): a sibling worker already has
	// an active entry order for this (strategy, symbol, order_type). Their
	// order IS being processed; our attempt correctly failed at the
	// partial UNIQUE index. Mark DONE (not retry, not DLQ) — this row is
	// resolved by having lost the race. Retrying would deterministically
	// fail again with the same violation until the sibling's order
	// reaches a terminal state.
	if errors.Is(handlerErr, ErrDuplicateActiveEntry) {
		w.logger.Info("Inbox row duplicate — sibling worker holds the active entry, marking DONE",
			zap.Int64("id", row.ID),
			zap.String("signal_id", row.SignalID),
			zap.String("type", row.OrderType))
		if err := w.repo.MarkInboxDone(ctx, row.ID); err != nil {
			w.logger.Warn("Inbox mark DONE (duplicate) failed",
				zap.Int64("id", row.ID), zap.Error(err))
		}
		return
	}

	// Classify and apply the right status / backoff.
	class := classifyHandlerErr(handlerErr)
	if class == InboxErrPoison {
		w.markDLQ(ctx, row, class, handlerErr.Error())
		return
	}
	w.markFailed(ctx, row, class, handlerErr.Error())
}

// dispatch routes an inbox row to the matching handler. Any json.Unmarshal
// failure is reported as a POISON error → DLQ. All other errors propagate
// up so processRow can classify them.
func (w *InboxWorker) dispatch(ctx context.Context, row *InboxRow) error {
	switch row.OrderType {
	case "MANTHAN_ENTRY", "MANTHAN_TOPUP":
		var sig ManthanSignal
		if err := json.Unmarshal(row.Payload, &sig); err != nil {
			return poisonErr("unmarshal MANTHAN_ENTRY: %v", err)
		}
		// Idempotency: the entry handler already dedups by signal_id via
		// repo.GetOrderBySignalID. A redelivered MANTHAN_ENTRY hits that
		// branch and silently returns success. We mirror that here so a
		// successful re-run of an already-placed entry marks the inbox row
		// DONE rather than churning the broker.
		if w.repo != nil && sig.OrderID != "" {
			if existing, err := w.repo.GetOrderBySignalID(ctx, sig.OrderID); err == nil && existing != nil {
				if entryAttemptIsTerminalFailure(existing.Status) {
					// 2026-08-27 SHREEPUSHK: an AU004-rejected entry row was
					// treated as "already placed" here, the inbox row went
					// DONE, and the day's signal died while the user simply
					// hadn't logged in yet. A REJECTED/CANCELLED row is a
					// failed ATTEMPT, not a placement — fall through and
					// re-attempt. Same-day only: the expiry sweep dead-letters
					// yesterday's entry rows before they can reach this point.
					w.logger.Info("Inbox MANTHAN_ENTRY prior attempt failed terminally — re-attempting",
						zap.String("signal_id", sig.OrderID),
						zap.Int64("prior_order_id", existing.ID),
						zap.String("prior_status", string(existing.Status)))
				} else {
					w.logger.Info("Inbox MANTHAN_ENTRY already placed — dedup OK",
						zap.String("signal_id", sig.OrderID),
						zap.Int64("existing_order_id", existing.ID))
					return nil
				}
			}
		}
		_, err := w.entry.ExecuteEntry(ctx, sig)
		return err
	case "MANTHAN_SL_MODIFY":
		var sig SLModifySignal
		if err := json.Unmarshal(row.Payload, &sig); err != nil {
			return poisonErr("unmarshal MANTHAN_SL_MODIFY: %v", err)
		}
		// FIX 1: serialize per-position SL modifications so two concurrent
		// SL_MODIFYs for the same (strategy,symbol) can't both read the stale
		// broker trigger and race the ratchet guard → the SL can never regress
		// down. The second goroutine blocks until the first commits its new
		// trigger, then the guard refuses the lower modify.
		return w.repo.WithPositionLock(ctx, sig.StrategyID, sig.Symbol, func(ctx context.Context) error {
			return w.sl.ModifyTrail(ctx, sig)
		})
	case "MANTHAN_SL_EXIT":
		var sig SLExitSignal
		if err := json.Unmarshal(row.Payload, &sig); err != nil {
			return poisonErr("unmarshal MANTHAN_SL_EXIT: %v", err)
		}
		return w.sl.EmergencySell(ctx, sig)
	default:
		// Unknown order types are inbox poison — they were filtered to
		// MANTHAN_* by the consumer, so reaching here means a code path
		// added a new MANTHAN_X without updating the worker.
		return poisonErr("unknown order_type: %s", row.OrderType)
	}
}

func (w *InboxWorker) markFailed(ctx context.Context, row *InboxRow, class, msg string) {
	attempts := row.Attempts + 1
	// UPPER_CIRCUIT holds do not burn attempts: a stock can sit at its band
	// for hours, and 50 x 3min would DLQ a perfectly good signal mid-session.
	// The hold is bounded by the market-hours pre-check instead — after
	// 15:20 IST the same row fails "too close to market close" (POISON) and
	// dead-letters cleanly at end of session. The computed value is passed
	// to MarkInboxFailed verbatim (the SQL no longer increments on its own).
	if isHoldClass(class) {
		attempts = row.Attempts
	}
	if attempts >= w.maxAttempts {
		w.markDLQ(ctx, row, class,
			fmt.Sprintf("attempts %d >= max %d: %s", attempts, w.maxAttempts, msg))
		return
	}
	delay := backoffFor(class, attempts)
	next := time.Now().Add(delay)
	if err := w.repo.MarkInboxFailed(ctx, row.ID, class, msg, next, attempts); err != nil {
		w.logger.Warn("Inbox mark FAILED failed (will be reaped)",
			zap.Int64("id", row.ID), zap.Error(err))
		return
	}
	w.logger.Info("Inbox row scheduled for retry",
		zap.Int64("id", row.ID),
		zap.String("signal_id", row.SignalID),
		zap.String("class", class),
		zap.Int("attempts", attempts),
		zap.Duration("retry_in", delay))
}

func (w *InboxWorker) markDLQ(ctx context.Context, row *InboxRow, class, msg string) {
	if err := w.repo.MarkInboxDLQ(ctx, row.ID, class, msg); err != nil {
		w.logger.Warn("Inbox mark DLQ failed (will be reaped)",
			zap.Int64("id", row.ID), zap.Error(err))
		return
	}
	w.logger.Error("Inbox row → DLQ — operator attention required",
		zap.Int64("id", row.ID),
		zap.String("signal_id", row.SignalID),
		zap.String("user", row.UserID),
		zap.String("type", row.OrderType),
		zap.String("class", class),
		zap.String("error", msg))
}

// ─── helpers ──────────────────────────────────────────────────────────────

// classifyHandlerErr maps handler errors to the inbox error class taxonomy.
// AUTH_EXPIRED takes precedence — every loop in the codebase agrees that
// errors.Is(err, indiraClient.ErrAuthExpired) means "back off, gate is set".
func classifyHandlerErr(err error) string {
	if errors.Is(err, indiraClient.ErrAuthExpired) {
		return InboxErrAuthExpired
	}
	// Upper-circuit holds are checked BEFORE the generic "pre-check:" POISON
	// rule — the reason text ("stock at upper circuit — skip entry") is a
	// pre-check reason, but unlike the others it self-resolves the moment
	// the band releases, so it must retry, not DLQ.
	if errors.Is(err, ErrUpperCircuitHold) {
		return InboxErrUpperCircuit
	}
	if errors.Is(err, ErrPreOpenHold) {
		return InboxErrPreOpen
	}
	var pe poisonError
	if errors.As(err, &pe) {
		return InboxErrPoison
	}
	msg := err.Error()
	// Pre-check rejections are permanent for the current evaluation cycle —
	// "too close to market close", "market not open yet", "market closed —
	// Saturday", "stock at lower circuit", "margin pre-check: insufficient",
	// "SL modify pre-check: ..." all key off wall-clock / data-snapshot
	// conditions that the next retry cannot fix. Send straight to DLQ so we
	// don't burn 50 retries on a deterministic-fail. Operator re-issues the
	// signal once the underlying condition (funding, market hours, etc.) is
	// resolved.
	if strings.Contains(msg, "pre-check:") {
		return InboxErrPoison
	}
	// "no active SL order found", "resolve symbol", "broker rejected" all
	// land here. Most are operationally transient (state will catch up
	// from a fill event, or the operator will fix the data); a small number
	// are permanent. We rely on max-attempts → DLQ to bound the retry.
	if strings.Contains(msg, "rejected") ||
		strings.Contains(msg, "DPR") ||
		strings.Contains(msg, "tick") {
		return InboxErrBrokerReject
	}
	return InboxErrTransient
}

// backoffFor returns the wait before the next attempt. AUTH_EXPIRED uses a
// flat 30 s — short enough that a fast re-login picks up immediately, long
// enough that we don't burn the gate's dedup window. Everything else gets
// exponential backoff capped at 5 min.
func backoffFor(class string, attempts int) time.Duration {
	if class == InboxErrAuthExpired {
		return 30 * time.Second
	}
	// Upper-circuit holds: flat 3-minute cadence. Exponential backoff is
	// wrong here — the band can release at any moment and we want to catch
	// it promptly for the whole session, not probe frantically for the
	// first minute and then nap. Each re-check reads FRESH LTP/DPR from
	// the market feed (CheckCircuit does a live Redis GET per call).
	if class == InboxErrUpperCircuit {
		return 3 * time.Minute
	}
	// Pre-open holds: wake precisely at 09:15:10 IST (a few seconds after
	// the bell so the LTP feed has a live tick), never sooner than 30 s.
	if class == InboxErrPreOpen {
		return untilMarketOpen(time.Now())
	}
	const cap = 5 * time.Minute
	exp := attempts
	if exp > 8 {
		exp = 8 // 2^8 = 256 s ≈ 4.27 min, just under cap
	}
	d := time.Duration(1<<uint(exp)) * time.Second
	if d > cap {
		return cap
	}
	return d
}

// isHoldClass reports whether an inbox error class is a WAIT state (the
// condition self-resolves with time) rather than a failure. Hold classes do
// not burn attempts and keep their class through an auth outage.
func isHoldClass(class string) bool {
	// AUTH_EXPIRED joined the hold classes 2026-08-27: a dead broker session
	// can last hours (user hasn't logged in), and 50 × 30 s burned the
	// attempt budget in 25 minutes — the day's signals DLQ'd long before the
	// user could ever act. Like the other holds it is bounded externally:
	// the same-day expiry sweep dead-letters entry rows at the next IST day,
	// and the market-hours pre-check poisons them after the entry cutoff.
	return class == InboxErrUpperCircuit || class == InboxErrPreOpen ||
		class == InboxErrAuthExpired
}

// entryAttemptIsTerminalFailure reports whether an existing manthan_orders
// row for a signal represents a FAILED placement attempt (safe to retry)
// rather than a live or completed one. Everything not listed here — PENDING,
// PLACED, PARTIAL, FILLED, SL_* — means the entry is in flight or done, and
// the inbox must dedup.
func entryAttemptIsTerminalFailure(s OrderStatus) bool {
	return s == StatusRejected || s == StatusCancelled || s == StatusExpired
}

// untilMarketOpen returns how long to sleep so the next attempt lands at
// 09:15:10 IST today (10 s past the bell so the LTP feed has a live tick).
// If that instant has already passed — the hold was set at 09:14:59 and we
// are now at 09:15:30, or the clock is odd — fall back to 30 s so the row
// simply re-runs the pre-check, which will now pass.
// istLocation returns Asia/Kolkata, falling back to a fixed +05:30 zone on
// hosts without tzdata.
func istLocation() *time.Location {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	if loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	return loc
}

func untilMarketOpen(now time.Time) time.Duration {
	n := now.In(istLocation())
	open := time.Date(n.Year(), n.Month(), n.Day(), 9, 15, 10, 0, istLocation())
	d := open.Sub(n)
	if d < 30*time.Second {
		return 30 * time.Second
	}
	return d
}

// poisonError is a sentinel for messages that can never be processed. We
// can't construct it from the indira package the way ErrAuthExpired works
// because it's local-only — the worker creates it for unmarshal failures
// and unknown order types.
type poisonError struct{ msg string }

func (p poisonError) Error() string { return p.msg }

func poisonErr(format string, a ...any) error {
	return poisonError{msg: fmt.Sprintf(format, a...)}
}
