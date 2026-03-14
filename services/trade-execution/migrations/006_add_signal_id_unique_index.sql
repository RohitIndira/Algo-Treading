-- Migration 006: Add signal_id column and unique index for idempotency.
-- Prevents duplicate order execution when Kafka replays signals on restart.

-- Add signal_id column to track the originating trade signal.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS signal_id UUID;

-- Unique index ensures the same signal cannot create two orders.
-- Partial index: only enforce for non-null signal_id so existing rows are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_signal_id_unique
    ON orders (signal_id)
    WHERE signal_id IS NOT NULL;

-- Backfill: for existing rows, set signal_id = order_id (they share the same UUID
-- in the current rules-engine flow).
UPDATE orders SET signal_id = order_id WHERE signal_id IS NULL;
