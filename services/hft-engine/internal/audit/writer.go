// Package audit is the asynchronous, batched writer for the
// hft_audit_orders table. The tick loop never blocks on DB writes —
// it drops events into a channel and the writer drains the channel
// in the background.
//
// Design rules:
//   1. Log() MUST be non-blocking. If the channel is full, drop the
//      event and emit a warn log. Better to lose an audit row than
//      miss a market tick.
//   2. The writer flushes every FlushInterval (default 1s) or when
//      the batch buffer hits BatchSize, whichever comes first.
//   3. On Stop(), drain whatever's in the channel before exiting so
//      Ctrl+C doesn't lose the last few seconds of audit.
package audit

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/repo"
)

// Config tunes the writer. Sensible defaults for ~100 events/sec workloads.
type Config struct {
	ChannelSize   int           // buffered channel depth. Default 1000.
	BatchSize     int           // flush when this many events queued. Default 200.
	FlushInterval time.Duration // flush at least this often. Default 1s.
}

// Sink is the minimal surface Writer needs from its persistence layer.
// repo.Repo satisfies it in prod; tests can pass a stub.
type Sink interface {
	InsertAuditOrder(ctx context.Context, e repo.AuditRow) error
}

// Writer drains a channel of repo.AuditRow into the configured Sink.
type Writer struct {
	cfg     Config
	sink    Sink
	logger  *zap.Logger
	ch      chan repo.AuditRow
	stopCh  chan struct{}
	stopped chan struct{} // closed when the worker goroutine has exited
	dropped uint64        // total events dropped due to full channel — exported via metrics later
	mu      sync.Mutex    // protects dropped (cheap atomic alternative; keeps things simple)
}

// New builds a Writer but does NOT start the worker. Call Start(ctx).
// `sink` may be nil — the worker will still run but every flush will
// log + drop. Useful for tests that don't care about persistence.
func New(sink Sink, logger *zap.Logger, cfg Config) *Writer {
	if cfg.ChannelSize <= 0 {
		cfg.ChannelSize = 1000
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 1 * time.Second
	}
	return &Writer{
		cfg:     cfg,
		sink:    sink,
		logger:  logger.Named("audit"),
		ch:      make(chan repo.AuditRow, cfg.ChannelSize),
		stopCh:  make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// Start runs the worker goroutine. Returns immediately; the goroutine
// runs until Stop() is called or ctx is cancelled.
func (w *Writer) Start(ctx context.Context) {
	go w.run(ctx)
}

// Log is the hot-path API. NEVER blocks. Returns true if the event
// was queued, false if dropped because the channel was full.
//
// Callers should not check the bool — losing an audit row is preferable
// to back-pressuring the strategy. The dropped counter goes to metrics.
func (w *Writer) Log(e repo.AuditRow) bool {
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	select {
	case w.ch <- e:
		return true
	default:
		w.mu.Lock()
		w.dropped++
		dropped := w.dropped
		w.mu.Unlock()
		// Log every 100 drops so we know it's happening without spamming.
		if dropped%100 == 1 {
			w.logger.Warn("audit channel full — event dropped",
				zap.Uint64("total_dropped", dropped))
		}
		return false
	}
}

// Stop drains the channel and waits for the worker to finish.
// Safe to call once; subsequent calls are no-ops.
func (w *Writer) Stop() {
	select {
	case <-w.stopCh:
		return // already stopped
	default:
		close(w.stopCh)
	}
	<-w.stopped // wait until run() has drained and exited
}

// run is the worker goroutine. Three exit paths: stopCh closed,
// ctx cancelled, or panic (which Go will surface).
func (w *Writer) run(ctx context.Context) {
	defer close(w.stopped)

	batch := make([]repo.AuditRow, 0, w.cfg.BatchSize)
	flushTimer := time.NewTimer(w.cfg.FlushInterval)
	defer flushTimer.Stop()

	flush := func(reason string) {
		if len(batch) == 0 {
			return
		}
		// nil sink path: tests pass nil to avoid wiring a DB. Drop the
		// batch and move on. Prod always wires a real Sink, so this
		// branch never fires there.
		if w.sink == nil {
			w.logger.Debug("audit batch dropped (no sink)",
				zap.String("reason", reason),
				zap.Int("rows", len(batch)))
			batch = batch[:0]
			return
		}
		// Phase 1: one row at a time. Phase 2+ optimisation: build a
		// multi-VALUES INSERT to cut wire round-trips.
		ok, fail := 0, 0
		var firstErr error
		for _, e := range batch {
			if err := w.sink.InsertAuditOrder(ctx, e); err != nil {
				fail++
				if firstErr == nil {
					firstErr = err
				}
				// Log per-row at warn so we can grep for individual failures.
				w.logger.Warn("audit row insert failed",
					zap.String("strategy_id", e.StrategyID),
					zap.String("action", e.Action),
					zap.Error(err))
				continue
			}
			ok++
		}
		fields := []zap.Field{
			zap.String("reason", reason),
			zap.Int("ok", ok),
			zap.Int("fail", fail),
		}
		if firstErr != nil {
			fields = append(fields, zap.Error(firstErr))
		}
		w.logger.Debug("audit batch flushed", fields...)
		batch = batch[:0]
	}

	for {
		select {
		case <-w.stopCh:
			// Drain remaining channel content before exit.
			for {
				select {
				case e := <-w.ch:
					batch = append(batch, e)
				default:
					flush("stop")
					return
				}
			}
		case <-ctx.Done():
			flush("ctx-done")
			return
		case e := <-w.ch:
			batch = append(batch, e)
			if len(batch) >= w.cfg.BatchSize {
				flush("size")
				// Reset the timer because we just flushed.
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flushTimer.Reset(w.cfg.FlushInterval)
			}
		case <-flushTimer.C:
			flush("interval")
			flushTimer.Reset(w.cfg.FlushInterval)
		}
	}
}
