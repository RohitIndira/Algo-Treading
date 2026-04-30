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
//   AUTH_EXPIRED   30 s     — gate-driven; the auth-restored hook also
//                              wakes us early via the workCh.
//   BROKER_REJECT  exp      — 1 s, 2 s, 4 s … capped at 5 min, max 50.
//   TRANSIENT      exp      — same as BROKER_REJECT.
//   POISON         DLQ      — never retry; operator pages.
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
			return poisonErr("unmarshal MANTHAN_ENTRY: %w", err)
		}
		// Idempotency: the entry handler already dedups by signal_id via
		// repo.GetOrderBySignalID. A redelivered MANTHAN_ENTRY hits that
		// branch and silently returns success. We mirror that here so a
		// successful re-run of an already-placed entry marks the inbox row
		// DONE rather than churning the broker.
		if w.repo != nil && sig.OrderID != "" {
			if existing, err := w.repo.GetOrderBySignalID(ctx, sig.OrderID); err == nil && existing != nil {
				w.logger.Info("Inbox MANTHAN_ENTRY already placed — dedup OK",
					zap.String("signal_id", sig.OrderID),
					zap.Int64("existing_order_id", existing.ID))
				return nil
			}
		}
		_, err := w.entry.ExecuteEntry(ctx, sig)
		return err
	case "MANTHAN_SL_MODIFY":
		var sig SLModifySignal
		if err := json.Unmarshal(row.Payload, &sig); err != nil {
			return poisonErr("unmarshal MANTHAN_SL_MODIFY: %w", err)
		}
		return w.sl.ModifyTrail(ctx, sig)
	case "MANTHAN_SL_EXIT":
		var sig SLExitSignal
		if err := json.Unmarshal(row.Payload, &sig); err != nil {
			return poisonErr("unmarshal MANTHAN_SL_EXIT: %w", err)
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
	if attempts >= w.maxAttempts {
		w.markDLQ(ctx, row, class,
			fmt.Sprintf("attempts %d >= max %d: %s", attempts, w.maxAttempts, msg))
		return
	}
	delay := backoffFor(class, attempts)
	next := time.Now().Add(delay)
	if err := w.repo.MarkInboxFailed(ctx, row.ID, class, msg, next); err != nil {
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
	var pe poisonError
	if errors.As(err, &pe) {
		return InboxErrPoison
	}
	// "no active SL order found", "resolve symbol", "broker rejected" all
	// land here. Most are operationally transient (state will catch up
	// from a fill event, or the operator will fix the data); a small number
	// are permanent. We rely on max-attempts → DLQ to bound the retry.
	msg := err.Error()
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

// poisonError is a sentinel for messages that can never be processed. We
// can't construct it from the indira package the way ErrAuthExpired works
// because it's local-only — the worker creates it for unmarshal failures
// and unknown order types.
type poisonError struct{ msg string }

func (p poisonError) Error() string { return p.msg }

func poisonErr(format string, a ...any) error {
	return poisonError{msg: fmt.Sprintf(format, a...)}
}
