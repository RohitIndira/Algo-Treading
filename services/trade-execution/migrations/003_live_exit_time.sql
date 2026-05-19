-- Migration 003: add live_exit_time to orders table
-- live_exit_time records the exact timestamp when a live position was closed
-- (force-exit, OCO trigger, or auto square-off). Previously the only way to
-- approximate this was updated_at, which changes on any row update and is
-- therefore unreliable as a dedicated exit timestamp.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS live_exit_time TIMESTAMPTZ;

-- Backfill existing closed live orders: use updated_at as best approximation.
-- These rows were closed before this column existed, so updated_at is the
-- closest timestamp available.
UPDATE orders
SET    live_exit_time = updated_at
WHERE  is_paper_trade  = false
  AND  live_exit_price IS NOT NULL
  AND  live_exit_time  IS NULL;

-- Partial index for fast closed-order queries that filter on exit time.
CREATE INDEX IF NOT EXISTS idx_orders_live_exit_time
    ON orders(user_id, live_exit_time)
    WHERE is_paper_trade = false AND live_exit_time IS NOT NULL;
