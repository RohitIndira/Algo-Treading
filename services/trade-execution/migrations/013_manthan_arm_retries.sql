-- Migration 013: Layer 4 — EOD Phase A retry queue
--
-- When the 15:35 IST EOD Phase A cron walks a user's positions and finds the
-- user's broker JWT expired or absent, the cycle skips that user — those
-- positions sit unprotected overnight unless the user re-logs in before the
-- next morning's 09:14 IST hot-SL fallback. To close that gap, every skip
-- enqueues a row here; a background worker scans the queue every 5 minutes
-- AND immediately on USER_CREDENTIALS_UPDATED Kafka events (i.e. the moment
-- the user re-logs in via SSO), re-attempting EOD Phase A so the AMO is
-- placed as soon as auth becomes available.
--
-- Status lifecycle:
--   PENDING  → DONE      (re-attempt placed the AMO successfully)
--   PENDING  → GIVEN_UP  (still pending past tomorrow's 09:30 IST — too late)
--
-- See services/trade-execution/internal/manthan/eod_phase_a_retry.go for the
-- worker; services/trade-execution/internal/manthan/eod_phase_a.go for the
-- skip-block that enqueues on no-auth / JWT-expired errors.

BEGIN;

CREATE TABLE IF NOT EXISTS manthan_arm_retries (
    id              BIGSERIAL PRIMARY KEY,
    user_id         TEXT NOT NULL,
    entry_order_id  BIGINT NOT NULL,
    trade_date      DATE NOT NULL,
    reason          TEXT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    last_error      TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partial UNIQUE: one PENDING row per (entry_order_id, trade_date). Keeps
-- enqueue idempotent — re-running EOD Phase A in the 15:35-16:30 startup
-- window cannot create duplicate retries for the same position.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_arm_retry_pending_per_entry
    ON manthan_arm_retries(entry_order_id, trade_date)
    WHERE status = 'PENDING';

-- Hot path for the worker's on-login scan: WHERE user_id = $1 AND status = 'PENDING'.
CREATE INDEX IF NOT EXISTS idx_arm_retries_pending_user
    ON manthan_arm_retries(user_id, trade_date)
    WHERE status = 'PENDING';

COMMENT ON TABLE manthan_arm_retries IS
'Layer 4 EOD Phase A retry queue. PENDING rows are inserted when 15:35 IST cycle skips a position for a recoverable reason (no broker auth / JWT expired). Background worker retries every 5 minutes and on USER_CREDENTIALS_UPDATED Kafka events.';

COMMIT;
