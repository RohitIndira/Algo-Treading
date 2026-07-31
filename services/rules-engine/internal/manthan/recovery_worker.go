package manthan

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Recovery worker for the manthan_signal_decisions outbox (FIX B).
//
// The publish path is: INSERT PROPOSED → Kafka trade-signals → mark DISPATCHED.
// If the inline Kafka publish fails, the row is left PROPOSED and nothing else
// retries it — a Kafka blip would silently lose an ENTRY_BUY / SL_MODIFY /
// EXIT_TSL signal. This worker is the backstop: it periodically re-publishes
// any PROPOSED row that has been stuck past a grace period, using the exact
// bytes captured in kafka_payload (migration 012), then marks it DISPATCHED.
//
// Safety:
//   - at-least-once + idempotent. Re-publish sends the same value with the same
//     signal_id; trade-execution's UNIQUE(signal_id) on manthan_orders drops any
//     duplicate, so a re-fire never creates a second broker order.
//   - multi-replica safe via SELECT ... FOR UPDATE SKIP LOCKED: each PROPOSED
//     row is claimed by exactly one worker per pass.
//   - inline-vs-worker race avoided by the grace period: the inline publisher
//     exclusively owns a row for recoveryGracePeriod after INSERT; the worker
//     only touches rows older than that (i.e. the inline path genuinely failed).
//   - verbatim re-publish: kafka_payload is the exact json.Marshal(order) that
//     the inline path published, so trading_mode (PAPER/LIVE) and every other
//     strategy-derived field are byte-identical — no reconstruction risk.
const (
	// recoveryGracePeriod: inline path owns a freshly-inserted row for this long.
	// Only rows unpublished past this window are treated as inline failures.
	recoveryGracePeriod = 5 * time.Second
	// recoveryBatchSize bounds work per pass; a large backlog drains over
	// several passes rather than holding row locks for an unbounded batch.
	recoveryBatchSize = 100
	// recoveryInterval between scan passes.
	recoveryInterval = 2 * time.Second
	// recoveryPublishTimeout caps a single re-publish so a Kafka stall cannot
	// hold the batch transaction's row locks indefinitely.
	recoveryPublishTimeout = 5 * time.Second
)

// RunRecoveryWorker blocks until ctx is cancelled, re-publishing stuck PROPOSED
// signal_decisions every interval. No-op (logs + returns) if DB or the
// trade-signals writer isn't wired.
func (p *ManthanPublisher) RunRecoveryWorker(ctx context.Context, interval time.Duration) {
	if p.db == nil || p.tradeWriter == nil {
		p.logger.Info("manthan recovery worker not started (db or trade writer nil)")
		return
	}
	if interval <= 0 {
		interval = recoveryInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	p.logger.Info("manthan recovery worker started",
		zap.Duration("interval", interval),
		zap.Duration("grace", recoveryGracePeriod))
	defer p.logger.Info("manthan recovery worker stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.recoverOnce(ctx)
		}
	}
}

// recoverOnce claims a batch of stuck PROPOSED rows (SKIP LOCKED), re-publishes
// each verbatim, and marks the successes DISPATCHED — all in one transaction so
// the claim, publish, and status flip commit atomically. A row whose re-publish
// fails is left PROPOSED (not marked) and retried on a later pass.
func (p *ManthanPublisher) recoverOnce(ctx context.Context) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		p.logger.Warn("recovery: begin tx failed", zap.Error(err))
		return
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
		SELECT signal_id, signal_type, user_id, strategy_id, symbol, kafka_payload
		FROM manthan_signal_decisions
		WHERE status = 'PROPOSED'
		  AND kafka_payload IS NOT NULL
		  AND decided_at < now() - make_interval(secs => $1)
		ORDER BY decided_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`,
		recoveryGracePeriod.Seconds(), recoveryBatchSize)
	if err != nil {
		p.logger.Warn("recovery: scan failed", zap.Error(err))
		return
	}

	type rec struct {
		sigID, sigType, userID, stratID, symbol string
		payload                                 []byte
	}
	var batch []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.sigID, &r.sigType, &r.userID, &r.stratID, &r.symbol, &r.payload); err != nil {
			p.logger.Warn("recovery: scan row failed", zap.Error(err))
			continue
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		p.logger.Warn("recovery: rows iteration failed", zap.Error(err))
	}
	_ = rows.Close()
	if len(batch) == 0 {
		return
	}

	recovered := 0
	for _, r := range batch {
		pubCtx, cancel := context.WithTimeout(ctx, recoveryPublishTimeout)
		perr := p.tradeWriter.WriteMessages(pubCtx, kafka.Message{
			// FIX A (lockstep with the inline publish path): key by position
			// identity so a recovered signal lands on the same partition as its
			// siblings and preserves per-position order.
			Key:   []byte(r.stratID + ":" + r.symbol),
			Value: r.payload, // verbatim: the exact value first published
			Headers: []kafka.Header{
				{Key: "order_type", Value: []byte(orderTypeHeader(r.sigType))},
				{Key: "user_id", Value: []byte(r.userID)},
				{Key: "strategy_id", Value: []byte(r.stratID)},
				{Key: "symbol", Value: []byte(r.symbol)},
				{Key: "signal_id", Value: []byte(r.sigID)},
				{Key: "signal_type", Value: []byte(r.sigType)},
			},
		})
		cancel()
		if perr != nil {
			// Leave PROPOSED — the row's lock releases when this tx ends and it
			// is retried on a later pass. Downstream UNIQUE(signal_id) makes any
			// eventual double-publish harmless.
			p.logger.Warn("recovery: re-publish failed — will retry next pass",
				zap.String("signal_id", r.sigID),
				zap.String("signal_type", r.sigType),
				zap.Error(perr))
			continue
		}
		if _, uerr := tx.ExecContext(ctx, `
			UPDATE manthan_signal_decisions
			SET status = 'DISPATCHED', dispatched_at = now()
			WHERE signal_id = $1 AND status = 'PROPOSED'`, r.sigID); uerr != nil {
			p.logger.Warn("recovery: mark dispatched failed",
				zap.String("signal_id", r.sigID), zap.Error(uerr))
			continue
		}
		recovered++
	}

	if err := tx.Commit(); err != nil {
		p.logger.Warn("recovery: commit failed — batch will be retried", zap.Error(err))
		return
	}
	if recovered > 0 {
		p.logger.Info("manthan recovery worker re-published stuck signals",
			zap.Int("recovered", recovered),
			zap.Int("scanned", len(batch)))
	}
}

// orderTypeHeader maps a stored signal_type to the trade-signals order_type
// header, matching what the inline publish path sets.
func orderTypeHeader(signalType string) string {
	switch signalType {
	case "ENTRY_BUY":
		return "MANTHAN_ENTRY"
	case "SL_MODIFY":
		return "MANTHAN_SL_MODIFY"
	case "EXIT_TSL":
		return "MANTHAN_SL_EXIT"
	default:
		return "MANTHAN_UNKNOWN"
	}
}
