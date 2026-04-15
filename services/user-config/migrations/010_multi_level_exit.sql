-- ============================================================================
-- Migration 010: Multi-Level Stop Loss and Take Profit
-- Adds support for users to configure up to 5 exit levels (SL or TP),
-- each with a price_pct (% from entry) and qty_pct (% of position to exit).
-- ============================================================================

-- Add take_profit_type to distinguish FIXED (single TP) from MULTI_LEVEL (N levels)
-- stop_loss_type already supports FIXED, TRAILING; we extend it with MULTI_LEVEL.
ALTER TABLE trade_configs
    ADD COLUMN IF NOT EXISTS take_profit_type  VARCHAR(20) DEFAULT 'FIXED',
    ADD COLUMN IF NOT EXISTS multi_level_sl    JSONB,
    ADD COLUMN IF NOT EXISTS multi_level_tp    JSONB;

-- Index for querying strategies that use multi-level exits
CREATE INDEX IF NOT EXISTS idx_tc_sl_type ON trade_configs(stop_loss_type);
CREATE INDEX IF NOT EXISTS idx_tc_tp_type ON trade_configs(take_profit_type);

-- ============================================================================
-- JSONB SCHEMA (enforced by application, documented here for reference):
--
-- multi_level_sl / multi_level_tp:
--   [
--     { "level_num": 1, "price_pct": 1.0, "qty_pct": 30.0 },
--     { "level_num": 2, "price_pct": 2.0, "qty_pct": 50.0 },
--     { "level_num": 3, "price_pct": 3.0, "qty_pct": 20.0 }
--   ]
--
-- Invariants (validated in application layer):
--   - Array length: 1..5
--   - level_num: unique, sequential from 1
--   - price_pct: positive, strictly increasing across levels
--   - qty_pct: positive, all values sum exactly to 100.0
--   - stop_loss_type = 'MULTI_LEVEL'  when multi_level_sl is non-null
--   - take_profit_type = 'MULTI_LEVEL' when multi_level_tp is non-null
-- ============================================================================
