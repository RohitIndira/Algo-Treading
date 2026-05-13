-- 013_hft_bidding_strategy.sql
-- Adds 'HFT_BIDDING' to the strategies.strategy_type CHECK constraint.
--
-- Pattern mirrors 012_manthan_strategy.sql: drop the existing CHECK, add a
-- new one with the additional value. No row data changes.
--
-- The HFT strategy's trade_config JSON carries (all stored as one JSONB blob
-- in trade_configs.config, parsed by hft-engine on Entry):
--   symbol, isin, exchange, product_type, side (BUY|SELL|BOTH),
--   max_buy_qty, max_sell_qty, single_buy_qty, single_sell_qty,
--   buy_limit_price, sell_limit_price, tick_size,
--   window_start ("HH:MM" IST), window_end ("HH:MM" IST),
--   modify_on_price_change (bool, default true),
--   mode ("PAPER" | "LIVE", default "PAPER")
--
-- No new columns on trade_configs — the existing JSON column is reused.

ALTER TABLE strategies DROP CONSTRAINT IF EXISTS strategies_strategy_type_check;
ALTER TABLE strategies ADD CONSTRAINT strategies_strategy_type_check
    CHECK (strategy_type IN ('NEWS', '52W_BREAKOUT', 'MANTHAN', 'HFT_BIDDING'));

-- HFT strategies need fields that don't exist as typed columns on
-- trade_configs (symbol, isin, max_buy_qty, buy_limit_price, ...). Rather
-- than add a dozen HFT-only columns, we attach a JSONB blob. Existing
-- NEWS / 52W / MANTHAN strategies leave it NULL and continue to use the
-- typed columns.
ALTER TABLE trade_configs
    ADD COLUMN IF NOT EXISTS config_extra JSONB;

-- Partial index so the hft-engine's "list active HFT strategies for this
-- user" query is cheap during recovery on engine restart.
CREATE INDEX IF NOT EXISTS idx_strategies_hft_active
    ON strategies(user_id)
    WHERE strategy_type = 'HFT_BIDDING' AND deleted_at IS NULL;
