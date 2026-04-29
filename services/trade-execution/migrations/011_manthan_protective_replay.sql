-- Migration 011: Protective-order replayer (custom GTC via daily AMO+SL replay)
--
-- Indira's broker silently downgrades GTC SL orders to DAY validity, so every
-- intraday SL is cancelled by the broker at 15:30 EOD and the position is
-- unprotected at next-day open. This migration adds the persistence pieces
-- needed by services/trade-execution/internal/manthan/protective_replay.go to
-- guarantee overnight protection by:
--
--   1. Submitting AMO + SL orders at 15:35 IST for the next session
--   2. Re-validating against fresh DPR at 09:14 IST pre-open
--   3. Hot-placing replacements at 09:15:30 IST for any conversion-rejected AMO
--
-- Schema additions
-- ─────────────────
--   trade_date DATE          — populated on every new order; replayer uses it
--                              as the daily idempotency key.
--   uniq_active_sl_per_day   — partial UNIQUE index that makes the daily AMO
--                              submit crash-safe: re-running the 15:35 cron
--                              cannot create a second active AMO row for the
--                              same (parent_order_id, trade_date).
--
-- Order-type extensions (no schema change — these are string values)
--   SL_SELL_AMO              — pending AMO+SL row written at 15:35 IST
--   SL_SELL_AMO_REJECTED     — terminal: AMO converted but exchange rejected
--                              (typically DPR breach despite our 50bps buffer)
--
-- Rollback
--   DROP INDEX IF EXISTS uniq_active_sl_per_day;
--   ALTER TABLE manthan_orders DROP COLUMN IF EXISTS trade_date;

BEGIN;

ALTER TABLE manthan_orders
    ADD COLUMN IF NOT EXISTS trade_date DATE;

-- Backfill from existing rows so the column is usable for queries on legacy data.
UPDATE manthan_orders
SET    trade_date = COALESCE(placed_at::date, created_at::date)
WHERE  trade_date IS NULL;

-- Idempotency: at most one *active* SL row per (entry, trade_date).
-- Terminal states (FILLED/CANCELLED/REJECTED/EXPIRED/...AMO_REJECTED) are
-- excluded so a rejected AMO doesn't block tomorrow's replay attempt.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_active_sl_per_day
    ON manthan_orders (parent_order_id, trade_date)
    WHERE order_type IN ('SL_SELL', 'SL_SELL_AMO')
      AND status NOT IN ('CANCELLED', 'REJECTED', 'FILLED', 'EXPIRED', 'SL_SELL_AMO_REJECTED');

-- Helps Phase B + Phase C find pending AMO rows quickly.
CREATE INDEX IF NOT EXISTS idx_mord_amo_pending
    ON manthan_orders (trade_date)
    WHERE order_type = 'SL_SELL_AMO' AND status NOT IN ('CANCELLED', 'REJECTED', 'FILLED', 'SL_SELL_AMO_REJECTED');

COMMENT ON COLUMN manthan_orders.trade_date IS
    'Trading day the order is intended for. Set by replayer (= next session) for AMO rows; for in-session rows = placed_at::date.';

COMMENT ON INDEX uniq_active_sl_per_day IS
    'Idempotency: no two active SL rows for the same position+day. Used by the protective replayer to make 15:35 IST AMO submission crash-safe.';

COMMIT;
