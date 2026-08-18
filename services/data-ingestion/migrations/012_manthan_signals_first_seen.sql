-- 012_manthan_signals_first_seen.sql  (signals_db)
--
-- WHY
--   Every publish (the 09:00 IST daily run and every sheet edit) re-emits
--   the WHOLE eligible Buy list under a new run_date, so a signal's
--   emitted_at is always "today". rules-engine needs to know when a stock
--   FIRST entered the list to enforce: a strategy created at time T acts
--   only on signals whose first appearance is AFTER T ("signal was there
--   yesterday, strategy created today → not for this strategy"). 2026-08-17:
--   FIV99 (created Fri 14th 21:50 IST) was dispatched the entire 26-stock
--   list on Monday's republish.
--
-- WHAT
--   first_seen_at — the instant the stock entered its CURRENT contiguous run
--   in manthan_signals. Carried forward day to day by the publisher; a stock
--   that leaves the list and comes back starts a new run (new signal).
--   Backfill walks run_dates in order: present on the previous publish day →
--   inherit; otherwise first_seen_at = that day's created_at.
--
-- IDEMPOTENT: re-runnable; only NULL first_seen_at rows are backfilled.

BEGIN;

ALTER TABLE manthan_signals ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ;

DO $$
DECLARE
    d     DATE;
    prevd DATE := NULL;
BEGIN
    FOR d IN SELECT DISTINCT run_date FROM manthan_signals ORDER BY run_date LOOP
        IF prevd IS NULL THEN
            UPDATE manthan_signals SET first_seen_at = created_at
            WHERE run_date = d AND first_seen_at IS NULL;
        ELSE
            UPDATE manthan_signals s
            SET first_seen_at = COALESCE(
                (SELECT p.first_seen_at FROM manthan_signals p
                  WHERE p.run_date = prevd AND p.symbol = s.symbol),
                s.created_at)
            WHERE s.run_date = d AND s.first_seen_at IS NULL;
        END IF;
        prevd := d;
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_manthan_signals_symbol_run ON manthan_signals(symbol, run_date DESC);

COMMIT;
