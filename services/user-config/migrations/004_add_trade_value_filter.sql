-- ============================================================================
-- User Config Service — Migration 004
-- Adds the trade value (turnover) filter to strategy_conditions.
--
-- Trade value = day-cumulative volume x current LTP, taken from the Redis
-- market-data snapshot (market:<exch>:<token>) at evaluation time. Thresholds
-- are stored in INR CRORE to match min_market_cap/max_market_cap and to keep
-- the UI from showing raw rupee figures like 227254977.
--
-- trade_value_mode drives which bound applies, so an open-ended filter is
-- explicit rather than inferred from a zero bound:
--   NULL / ''  -> filter off
--   'ABOVE'    -> trade_value >= min_trade_value
--   'BELOW'    -> trade_value <= max_trade_value
--   'RANGE'    -> min_trade_value <= trade_value <= max_trade_value
--
-- NOTE: this is deliberately NOT the pre-existing min_volume BIGINT column,
-- which counts raw shares rather than rupee turnover. That column stays unused.
-- ============================================================================

ALTER TABLE strategy_conditions
    ADD COLUMN IF NOT EXISTS trade_value_mode TEXT           DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS min_trade_value  NUMERIC(20, 4) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS max_trade_value  NUMERIC(20, 4) DEFAULT NULL;

COMMENT ON COLUMN strategy_conditions.trade_value_mode IS
    'Trade value filter operator: ABOVE | BELOW | RANGE. NULL/empty = filter off.';
COMMENT ON COLUMN strategy_conditions.min_trade_value  IS
    'Minimum trade value (volume x LTP) in INR crore. Used by ABOVE and RANGE modes.';
COMMENT ON COLUMN strategy_conditions.max_trade_value  IS
    'Maximum trade value (volume x LTP) in INR crore. Used by BELOW and RANGE modes.';
