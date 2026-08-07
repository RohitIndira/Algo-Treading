-- 018_strategy_lifecycle_events.sql
--
-- Timeline feed for the mobile "Algo Timeline" screen. One row per lifecycle
-- transition, written SYNCHRONOUSLY by user-config (the strategies owner) in
-- the same request that performs the transition:
--   DEPLOYED            strategy created (details: capital)
--   PAUSED / RESUMED    deactivate / activate
--   CAPITAL_INCREASED   total_capital raised  (details: from, to)
--   CAPITAL_DECREASED   total_capital lowered (details: from, to)
--   DELETED             strategy deleted      (details: positions_exited)
--
-- Order-level activity (entries, SLs, exits) is NOT duplicated here — the
-- timeline API merges it live from execution_db.manthan_orders.
--
-- Writes are best-effort: a failed insert logs and never fails the user's
-- action (the timeline is observability, not source of truth).

CREATE TABLE IF NOT EXISTS strategy_lifecycle_events (
    id          BIGSERIAL PRIMARY KEY,
    strategy_id UUID        NOT NULL,
    user_id     VARCHAR(64) NOT NULL,
    event_type  VARCHAR(32) NOT NULL,
    details     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sle_strategy_time
    ON strategy_lifecycle_events (strategy_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sle_user_time
    ON strategy_lifecycle_events (user_id, created_at DESC);
