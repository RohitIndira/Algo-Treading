-- Drop the 7 dead fields from risk_limits.
--
-- Context: MANTHAN is the only live strategy type as of 2026-07-20. None of the
-- risk_limits fields are used by it:
--   * rules-engine dropped the RiskLimits mapping on 2026-06-25 (Cat B trim) and
--     never reads these values — Manthan enforces portfolio caps via the
--     rebalancer/allocator instead.
--   * The user-config Kafka payload (events/mapper.go) already stopped publishing
--     RiskLimits.
--   * The only consumer of enable_auto_square_off / auto_square_off_time was
--     user-config's own EOD scheduler, which was dead code (its query targeted the
--     removed 'NEWS' strategy_type) and is deleted in the same change. The live
--     auto-square-off is a separate trade-execution scheduler with its own config.
--
-- Table itself is KEPT (one row per strategy, holds risk_limit_id + strategy_id +
-- created_at) — same placeholder pattern as migration 016 for strategy_conditions.
-- The models.RiskLimits struct is trimmed to those three columns.
--
-- ROLLBACK: to restore, run
--   ALTER TABLE risk_limits
--       ADD COLUMN max_daily_trades INTEGER,
--       ADD COLUMN max_per_trade_risk DOUBLE PRECISION,
--       ...
-- The rows carried only Manthan's fixed defaults (enable_risk_checks=true,
-- enable_auto_square_off=false), so nothing meaningful is lost.

ALTER TABLE risk_limits
    DROP COLUMN IF EXISTS max_daily_trades,
    DROP COLUMN IF EXISTS max_per_trade_risk,
    DROP COLUMN IF EXISTS max_portfolio_exposure_pct,
    DROP COLUMN IF EXISTS max_loss_per_day,
    DROP COLUMN IF EXISTS enable_risk_checks,
    DROP COLUMN IF EXISTS enable_auto_square_off,
    DROP COLUMN IF EXISTS auto_square_off_time;
