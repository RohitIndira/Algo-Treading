-- Migration 002: Add paper_exit_time to orders table
--
-- paper_exit_time records the exact timestamp when a paper position was closed,
-- regardless of exit reason (SL hit, TP hit, trailing SL, force exit,
-- auto square-off, strategy deletion).
-- entry_time is already captured by the existing executed_at column.
--
-- Backfill strategy for existing rows:
--   Closed entry orders  (is_square_off_order = false, paper_exit_price IS NOT NULL)
--     → updated_at  — the trigger sets updated_at = NOW() at the same instant
--                     paper_exit_price is written, so it is the exact exit moment.
--   Exit leg records (is_square_off_order = true, paper_exit_price IS NOT NULL)
--     → executed_at — these records are created with executed_at = exit moment.
--   Open / non-paper orders → paper_exit_time stays NULL (correct).
--
-- All steps run inside one transaction. If anything fails the DB is unchanged.

BEGIN;

-- Step 1: Add column — no data is touched, safe to run on live table.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paper_exit_time TIMESTAMPTZ;

-- Step 2: Backfill closed ENTRY orders.
-- Only updates rows where paper_exit_time IS NULL so re-running is safe.
UPDATE orders
SET    paper_exit_time = updated_at
WHERE  is_paper_trade      = true
  AND  is_square_off_order = false
  AND  paper_exit_price    IS NOT NULL
  AND  paper_exit_time     IS NULL;

-- Step 3: Backfill EXIT LEG records (partial/full square-off audit rows).
UPDATE orders
SET    paper_exit_time = COALESCE(executed_at, updated_at)
WHERE  is_paper_trade      = true
  AND  is_square_off_order = true
  AND  paper_exit_price    IS NOT NULL
  AND  paper_exit_time     IS NULL;

-- Step 4: Index for fast closed-position queries by user.
CREATE INDEX IF NOT EXISTS idx_orders_paper_exit_time
    ON orders(user_id, paper_exit_time)
    WHERE is_paper_trade = true AND paper_exit_time IS NOT NULL;

COMMIT;
