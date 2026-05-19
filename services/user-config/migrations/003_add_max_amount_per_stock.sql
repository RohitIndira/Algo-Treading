-- ============================================================================
-- User Config Service — Migration 003
-- Adds max_amount_per_stock and max_trades_per_strategy to risk_limits.
-- Re-applies migration 002 idempotently for environments where 002 was
-- recorded as applied but the ALTER did not actually run (e.g. dirty schema
-- state from an earlier failed deploy).
-- ============================================================================

ALTER TABLE risk_limits
    ADD COLUMN IF NOT EXISTS max_amount_per_stock    NUMERIC(20, 4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS max_trades_per_strategy INTEGER        DEFAULT NULL;

COMMENT ON COLUMN risk_limits.max_amount_per_stock    IS
    'Maximum investment per stock per order in INR. NULL = no limit. Drives amount-based position sizing in rules-engine.';
COMMENT ON COLUMN risk_limits.max_trades_per_strategy IS
    'Maximum number of trades this strategy may fire per calendar day (IST). NULL = no limit.';
