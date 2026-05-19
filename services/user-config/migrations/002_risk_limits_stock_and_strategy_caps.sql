-- ============================================================================
-- User Config Service — Migration 002
-- Adds max_amount_per_stock and max_trades_per_strategy to risk_limits.
-- ============================================================================

ALTER TABLE risk_limits
    ADD COLUMN IF NOT EXISTS max_amount_per_stock    NUMERIC(20, 4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS max_trades_per_strategy INTEGER        DEFAULT NULL;

COMMENT ON COLUMN risk_limits.max_amount_per_stock    IS
    'Maximum investment per stock per order in ₹. NULL means no limit.';
COMMENT ON COLUMN risk_limits.max_trades_per_strategy IS
    'Maximum number of trades this strategy may fire per calendar day (IST). NULL means no limit.';
