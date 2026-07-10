-- Migration 010: Scope manthan_signal_decisions uniqueness to ENTRY_BUY rows
--
-- Migration 003 added UNIQUE (strategy_id, symbol, decided_at) as
-- uq_msd_per_attempt. That intent was "no two entry decisions for the same
-- (strategy, symbol) in the same instant." Reasonable when the table only
-- held entries.
--
-- Migration 009 opened the table up to SL_MODIFY / EXIT_TSL / EXIT_MANUAL /
-- SL_CANCEL rows. Those can legitimately fire at the same instant as another
-- signal (e.g. entry decision + first trail-modify within the same second on
-- a fast market open). Keeping the constraint at table-wide scope forces a
-- retry loop or NOW() jitter in the publisher — both smells.
--
-- Fix: replace the table-wide UNIQUE with a partial index scoped to
-- signal_type='ENTRY_BUY'. Semantic intent preserved (no duplicate entries),
-- non-entry rows unrestricted.
--
-- Rollback: recreate the original table-wide UNIQUE.

BEGIN;

ALTER TABLE manthan_signal_decisions
    DROP CONSTRAINT IF EXISTS uq_msd_per_attempt;

CREATE UNIQUE INDEX IF NOT EXISTS uq_msd_entry_per_attempt
    ON manthan_signal_decisions (strategy_id, symbol, decided_at)
    WHERE signal_type = 'ENTRY_BUY';

COMMIT;

-- Verify:
--   SELECT indexdef FROM pg_indexes
--   WHERE indexname = 'uq_msd_entry_per_attempt';
--   -- expect: partial index WHERE signal_type = 'ENTRY_BUY'
