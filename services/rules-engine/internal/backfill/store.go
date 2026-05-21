package backfill

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Job status values, mirrored from the backfill_jobs.status CHECK constraint.
const (
	StatusPending   = "PENDING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

// Job is one row of the backfill_jobs table — the durable execution state of a
// single strategy's after-market-news backfill. It survives rules-engine
// restarts so a deferred dispatch (held until the next 09:15 IST) is recovered
// on boot rather than lost with the process.
type Job struct {
	StrategyID    string
	UserID        string
	Status        string
	WindowStart   time.Time
	WindowEnd     time.Time
	DispatchAfter time.Time
}

// JobStore persists backfill execution state in the shared PostgreSQL
// `backfill_jobs` table (same database the user-config service and the
// trade-signal repository use).
type JobStore struct {
	db *sql.DB
}

// NewJobStore wraps an existing *sql.DB pool owned by the caller.
func NewJobStore(db *sql.DB) *JobStore {
	return &JobStore{db: db}
}

// Claim attempts to durably claim the backfill for job.StrategyID by inserting
// a PENDING row. The PRIMARY KEY on strategy_id makes this atomic across
// concurrent rules-engine instances and across CONFIG_CREATED replays:
//
//	claimed == true  → this caller owns the backfill, proceed.
//	claimed == false → a row already exists (another worker, or an earlier run,
//	                   or startup recovery owns it) → caller must not proceed.
func (s *JobStore) Claim(ctx context.Context, job Job) (claimed bool, err error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("backfill job store: not initialized")
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO backfill_jobs
		     (strategy_id, user_id, status, window_start, window_end, dispatch_after)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (strategy_id) DO NOTHING`,
		job.StrategyID, job.UserID, StatusPending,
		job.WindowStart.UTC(), job.WindowEnd.UTC(), job.DispatchAfter.UTC(),
	)
	if err != nil {
		return false, fmt.Errorf("backfill job store: claim insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("backfill job store: rows affected: %w", err)
	}
	return n == 1, nil
}

// ListPending returns every job still in PENDING status. Called once on
// startup so backfills whose dispatch was deferred (or interrupted mid-run by
// a restart) are picked up and completed.
func (s *JobStore) ListPending(ctx context.Context) ([]Job, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("backfill job store: not initialized")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT strategy_id, user_id, status, window_start, window_end, dispatch_after
		   FROM backfill_jobs
		  WHERE status = $1`,
		StatusPending,
	)
	if err != nil {
		return nil, fmt.Errorf("backfill job store: list pending: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.StrategyID, &j.UserID, &j.Status,
			&j.WindowStart, &j.WindowEnd, &j.DispatchAfter,
		); err != nil {
			return nil, fmt.Errorf("backfill job store: scan: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("backfill job store: rows: %w", err)
	}
	return jobs, nil
}

// MarkCompleted transitions a job to COMPLETED and records the result counts.
func (s *JobStore) MarkCompleted(ctx context.Context, strategyID string, matchesFound, ordersDispatched int) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("backfill job store: not initialized")
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE backfill_jobs
		    SET status = $2, matches_found = $3, orders_dispatched = $4,
		        error = NULL, updated_at = NOW()
		  WHERE strategy_id = $1`,
		strategyID, StatusCompleted, matchesFound, ordersDispatched,
	)
	if err != nil {
		return fmt.Errorf("backfill job store: mark completed: %w", err)
	}
	return nil
}

// MarkFailed transitions a job to FAILED with a diagnostic message. A FAILED
// job is NOT retried automatically — an operator can delete the row (or flip
// process_after_market_news again) to re-trigger.
func (s *JobStore) MarkFailed(ctx context.Context, strategyID, reason string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("backfill job store: not initialized")
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE backfill_jobs
		    SET status = $2, error = $3, updated_at = NOW()
		  WHERE strategy_id = $1`,
		strategyID, StatusFailed, reason,
	)
	if err != nil {
		return fmt.Errorf("backfill job store: mark failed: %w", err)
	}
	return nil
}
