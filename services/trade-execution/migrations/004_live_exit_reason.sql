-- Migration 004: add live_exit_reason to orders table
--
-- live_exit_reason records WHY a live position was closed, so the frontend can
-- show the same exit-reason context paper trades already have. Values mirror the
-- paper-trade exit reasons:
--   STOP_LOSS    — OCO/ML stop-loss leg filled
--   TAKE_PROFIT  — OCO/ML take-profit leg filled
--   FORCE_EXIT   — user-triggered force-exit (all / per-strategy)
--   SQUARE_OFF   — EOD / per-user auto square-off
--   MANUAL       — exit placed directly from the broker terminal
--
-- rejection_reason is intentionally left for genuine broker rejections only;
-- this dedicated column keeps exit semantics unambiguous.
--
-- All steps run inside one transaction. If anything fails the DB is unchanged.

BEGIN;

-- Step 1: Add column — no data is touched, safe to run on a live table.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS live_exit_reason TEXT;

-- Step 2: Backfill existing closed live orders. Rows closed before this column
-- existed were all user force-exits (the only path that wrote live_exit_price),
-- so FORCE_EXIT is the correct historical value.
UPDATE orders
SET    live_exit_reason = 'FORCE_EXIT'
WHERE  is_paper_trade   = false
  AND  live_exit_price  IS NOT NULL
  AND  live_exit_reason IS NULL;

COMMIT;
