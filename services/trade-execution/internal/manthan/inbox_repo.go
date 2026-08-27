package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Inbox row status values — must stay in sync with the CHECK constraint
// in migrations/012_signal_inbox.sql.
const (
	InboxStatusPending = "PENDING"
	InboxStatusRunning = "RUNNING"
	InboxStatusDone    = "DONE"
	InboxStatusFailed  = "FAILED"
	InboxStatusDLQ     = "DLQ"
)

// Inbox error classes — the worker uses these to pick a backoff policy
// and to decide DLQ-vs-retry. See migration 012 for the contract.
const (
	InboxErrAuthExpired  = "AUTH_EXPIRED"
	InboxErrBrokerReject = "BROKER_REJECT"
	InboxErrPoison       = "POISON"
	InboxErrTransient    = "TRANSIENT"
	// UPPER_CIRCUIT — entry blocked only because the stock is pinned at its
	// upper price band. Not a failure: no sellers exist at the band, so the
	// order CANNOT fill right now but becomes placeable the moment the price
	// comes off the band. The worker re-checks on a flat cadence with fresh
	// LTP/DPR and does NOT burn attempts while holding; the 15:20 IST
	// market-close pre-check is the natural terminator (→ POISON → DLQ at
	// end of session). 2026-08-18: MODISONLTD spent the morning at UC and
	// the old "pre-check:"→POISON rule DLQ'd it with zero retries.
	InboxErrUpperCircuit = "UPPER_CIRCUIT"
	// PRE_OPEN — entry arrived before the 09:15 IST open (the daily 09:00
	// publish does this EVERY morning). Held with a precise wake at
	// 09:15:10 IST, no attempt burn. 2026-08-17: 26 FIV99 entries were
	// DLQ'd on attempt 1 by the generic "pre-check:"→POISON rule.
	InboxErrPreOpen = "PRE_OPEN"
)

// InboxRow mirrors a row of signal_inbox. Payload is kept as []byte so
// handlers can json.Unmarshal into their existing signal types (the inbox
// itself stays signal-shape-agnostic).
type InboxRow struct {
	ID             int64
	SignalID       string
	UserID         string
	OrderType      string
	Payload        []byte
	KafkaTopic     string
	KafkaPartition int
	KafkaOffset    int64
	Status         string
	Attempts       int
	LastError      string
	LastErrorClass string
}

// InsertInbox writes a Kafka signal to the inbox. ON CONFLICT (signal_id)
// DO NOTHING means Kafka redeliveries (consumer rebalance, restart, etc.)
// are silent no-ops. Returns true if the row was inserted (new), false if
// it was a duplicate. The caller commits Kafka regardless.
func (r *Repository) InsertInbox(
	ctx context.Context,
	signalID, userID, orderType string,
	payload json.RawMessage,
	topic string, partition int, offset int64,
) (bool, error) {
	const q = `
INSERT INTO signal_inbox
    (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset)
VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
ON CONFLICT (signal_id) DO NOTHING
RETURNING id`
	var id int64
	err := r.db.QueryRowContext(ctx, q,
		signalID, userID, orderType, []byte(payload), topic, partition, offset,
	).Scan(&id)
	if err == sql.ErrNoRows {
		// Duplicate — already in the inbox. Caller should still commit Kafka.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert signal_inbox: %w", err)
	}
	return true, nil
}

// ClaimDueInboxRows picks up to `limit` rows whose next_attempt_at has
// passed, marks them RUNNING (with started_at=now), and returns them. Uses
// FOR UPDATE SKIP LOCKED so multiple worker goroutines never fight over
// the same row. The caller MUST eventually call MarkInboxDone, MarkInboxFailed
// or MarkInboxDLQ — a worker crash leaves the row in RUNNING, but the
// reaper (ResetStuckInboxRows) sweeps those back to FAILED.
// ExpireStaleEntrySignals dead-letters every pending/failed MANTHAN_ENTRY
// inbox row created before todayStart (IST midnight). Entry signals are
// same-day only by policy: yesterday's signal must never place today —
// prices, allocation, and eligibility were all computed for yesterday's
// session. SL/exit rows are untouched: protective actions must survive
// day boundaries. Returns the number of rows expired.
func (r *Repository) ExpireStaleEntrySignals(ctx context.Context, todayStart time.Time) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
        UPDATE signal_inbox
           SET status='DLQ', completed_at=now(),
               last_error='expired: entry signals are same-day only (generated before ' || $1::date || ')',
               last_error_class='EXPIRED'
         WHERE order_type='MANTHAN_ENTRY'
           AND status IN ('PENDING','FAILED')
           AND created_at < $1`, todayStart)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (r *Repository) ClaimDueInboxRows(ctx context.Context, limit int) ([]*InboxRow, error) {
	if limit <= 0 {
		limit = 16
	}
	const q = `
WITH due AS (
    SELECT id
    FROM signal_inbox
    WHERE status IN ('PENDING','FAILED')
      AND next_attempt_at <= now()
    ORDER BY next_attempt_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE signal_inbox s
SET status     = 'RUNNING',
    started_at = now()
FROM due
WHERE s.id = due.id
RETURNING s.id, s.signal_id, s.user_id, s.order_type, s.payload,
          s.kafka_topic, s.kafka_partition, s.kafka_offset,
          s.status, s.attempts,
          COALESCE(s.last_error, ''), COALESCE(s.last_error_class, '')`
	rows, err := r.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("claim inbox rows: %w", err)
	}
	defer rows.Close()
	out := make([]*InboxRow, 0, limit)
	for rows.Next() {
		var row InboxRow
		if err := rows.Scan(
			&row.ID, &row.SignalID, &row.UserID, &row.OrderType, &row.Payload,
			&row.KafkaTopic, &row.KafkaPartition, &row.KafkaOffset,
			&row.Status, &row.Attempts, &row.LastError, &row.LastErrorClass,
		); err != nil {
			return nil, fmt.Errorf("scan inbox row: %w", err)
		}
		out = append(out, &row)
	}
	return out, rows.Err()
}

// MarkInboxDone closes a row out as successfully processed.
func (r *Repository) MarkInboxDone(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE signal_inbox
         SET status='DONE', completed_at=now(), last_error=NULL, last_error_class=NULL
         WHERE id=$1`, id)
	return err
}

// MarkInboxFailed records the failure, stores the WORKER-COMPUTED attempts
// value, and schedules the next attempt. The attempts count is passed in
// explicitly — NOT incremented in SQL — because the worker owns the policy:
// UPPER_CIRCUIT holds (and auth gates on a holding row) deliberately do not
// burn attempts. The previous `attempts=attempts+1` here silently overrode
// that policy (2026-08-18 review blocker: a stock pinned at its band all
// morning would have DLQ'd after ~2.5h despite the worker "not counting"
// the holds — the DB counted them anyway).
func (r *Repository) MarkInboxFailed(
	ctx context.Context, id int64, errClass, errMsg string, nextAttempt time.Time, attempts int,
) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE signal_inbox
         SET status='FAILED',
             attempts=$5,
             last_error=$2,
             last_error_class=$3,
             next_attempt_at=$4
         WHERE id=$1`, id, errMsg, errClass, nextAttempt, attempts)
	return err
}

// MarkInboxDLQ moves a row to the dead-letter state — either a poison
// message (parse failure with no signal_id should never have reached the
// inbox, but defensive) or attempts exceeded. Operators replay via
// UPDATE … SET status='PENDING', next_attempt_at=now() WHERE id=…
func (r *Repository) MarkInboxDLQ(
	ctx context.Context, id int64, errClass, errMsg string,
) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE signal_inbox
         SET status='DLQ',
             attempts=attempts+1,
             last_error=$2,
             last_error_class=$3,
             completed_at=now()
         WHERE id=$1`, id, errMsg, errClass)
	return err
}

// ResetStuckInboxRows is the reaper: a row stuck in RUNNING longer than
// `maxRunning` is assumed to belong to a crashed worker and gets bumped
// back to FAILED so another worker re-processes it. Called once on startup
// + periodically. Returns the number of rows reset.
func (r *Repository) ResetStuckInboxRows(ctx context.Context, maxRunning time.Duration) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE signal_inbox
         SET status='FAILED',
             last_error='RUNNING for longer than threshold — assumed orphaned',
             last_error_class=$1,
             next_attempt_at=now()
         WHERE status='RUNNING'
           AND started_at < now() - ($2::interval)`,
		InboxErrTransient, fmt.Sprintf("%d seconds", int(maxRunning.Seconds())))
	if err != nil {
		return 0, fmt.Errorf("reset stuck inbox rows: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// CountPendingInbox returns a quick gauge for monitoring (Grafana panel).
// status=FAILED with attempts > 0 means "currently retrying"; status=DLQ
// means "operator attention required".
func (r *Repository) CountPendingInbox(ctx context.Context) (pending, retrying, dlq int, err error) {
	const q = `
SELECT
  COUNT(*) FILTER (WHERE status='PENDING'),
  COUNT(*) FILTER (WHERE status='FAILED'),
  COUNT(*) FILTER (WHERE status='DLQ')
FROM signal_inbox`
	err = r.db.QueryRowContext(ctx, q).Scan(&pending, &retrying, &dlq)
	return
}
