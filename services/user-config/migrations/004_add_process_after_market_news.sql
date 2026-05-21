-- ============================================================================
-- User Config Service — Migration 004
-- After-Market News backfill feature.
--
-- 1. strategies.process_after_market_news
--      Opt-in flag. When true, on strategy creation rules-engine scans news
--      from the previous trading day's 15:31 IST close up to creation time
--      (or, if created outside market hours / on a non-trading day, up to the
--      next trading day's 09:15 IST open) and places orders for any matches.
--      Orders are tagged signal_source = 'BACKFILL_AMN'.
--
-- 2. backfill_jobs
--      Execution-state table written by rules-engine. One row per strategy
--      that opted in. Survives rules-engine restarts so a backfill whose
--      dispatch was deferred to the next 09:15 IST is recovered on boot.
--
--      status: PENDING   → claimed, scan/dispatch not yet finished
--              COMPLETED → all matches dispatched
--              FAILED    → unrecoverable error (see error column)
-- ============================================================================

ALTER TABLE strategies
    ADD COLUMN IF NOT EXISTS process_after_market_news BOOLEAN DEFAULT false NOT NULL;

-- Backfill execution state lives in the backfill_jobs table (below), NOT on the
-- strategies row. Drop backfill_completed_at if an earlier iteration of this
-- migration added it — otherwise `SELECT *` on strategies fails to map the
-- orphaned column to the Strategy struct.
ALTER TABLE strategies
    DROP COLUMN IF EXISTS backfill_completed_at;

COMMENT ON COLUMN strategies.process_after_market_news IS
    'Opt-in: rules-engine backfills after-market news on creation. See backfill_jobs.';

CREATE TABLE IF NOT EXISTS backfill_jobs (
    strategy_id       UUID         PRIMARY KEY REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    user_id           VARCHAR(255) NOT NULL,
    status            VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                          CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),
    -- News-time window the backfill scans (IST stored as UTC timestamptz).
    window_start      TIMESTAMPTZ  NOT NULL,
    window_end        TIMESTAMPTZ  NOT NULL,
    -- Wall-clock time after which matched orders may be dispatched. Equal to
    -- created_at for an immediate (in-market-hours) backfill; equal to the
    -- next 09:15 IST when the strategy was created outside market hours.
    dispatch_after    TIMESTAMPTZ  NOT NULL,
    matches_found     INTEGER      NOT NULL DEFAULT 0,
    orders_dispatched INTEGER      NOT NULL DEFAULT 0,
    error             TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Startup recovery query: "give me every job still awaiting dispatch".
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_pending
    ON backfill_jobs(status)
    WHERE status = 'PENDING';

COMMENT ON TABLE  backfill_jobs                IS 'After-Market News backfill execution state (rules-engine owned).';
COMMENT ON COLUMN backfill_jobs.dispatch_after IS 'Orders held until this wall-clock time (next 09:15 IST for off-hours creation).';
