-- Migration 006: Add max_trades_per_strategy to orders table
--
-- Carries the strategy's daily trade cap (MaxTradesPerStrategy) onto each order
-- so the price monitor can enforce the same hard ceiling when a below_min watch
-- finally triggers — including after a service restart, where pending monitor
-- orders are reloaded from this table (reloadFromDB). Without persisting it, a
-- reloaded watch would trigger with no cap.
--
-- 0 = no limit (matches the rules-engine semantic). Existing rows default to 0,
-- which is safe: they are historical/uncapped rather than falsely capped.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS max_trades_per_strategy INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN orders.max_trades_per_strategy IS
    'Strategy daily trade cap carried from the signal; enforced at price-monitor trigger time. 0 = no limit.';
