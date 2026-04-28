-- Migration 006: Enforce classification fields on active manthan_positions
--
-- BUG FIXED: Earlier projector code did INSERT manthan_positions without
-- carrying industry / mcap_bucket / index_name from the source decision row.
-- Result: active position rows had NULL/empty classification, which broke
-- the rebalancer's top-up phase ("EMA target = 0 for " — empty string lookup).
--
-- The projector code is now fixed to populate these. This migration adds a
-- CHECK constraint as defense-in-depth: any future code path that tries to
-- insert/update an ACTIVE or PARTIAL_ACTIVE row without classification will
-- fail LOUDLY at INSERT time, making this class of bug impossible to recur
-- silently.
--
-- EXITED / COOLDOWN rows are exempted — they're terminal and historical
-- legacy rows may have NULLs. Future EXITED rows naturally inherit from
-- ACTIVE so they'll have classification too, but the constraint doesn't
-- enforce it on terminal rows.
--
-- Backfill: assumes any existing ACTIVE/PARTIAL_ACTIVE rows already have
-- classification (the operator should run the backfill UPDATE separately
-- before applying this migration, or this ALTER will fail).
--
-- Rollback:
--   ALTER TABLE manthan_positions DROP CONSTRAINT chk_active_has_classification;

BEGIN;

-- Defensive: any active row missing classification at this point would
-- block the constraint. Surface them so the operator can backfill.
DO $$
DECLARE
    bad_count int;
BEGIN
    SELECT count(*) INTO bad_count
    FROM manthan_positions
    WHERE status IN ('ACTIVE','PARTIAL_ACTIVE')
      AND (industry IS NULL OR industry = ''
           OR mcap_bucket IS NULL OR mcap_bucket = ''
           OR index_name IS NULL OR index_name = '');
    IF bad_count > 0 THEN
        RAISE EXCEPTION
            'Cannot apply chk_active_has_classification: % active rows still missing classification. Backfill from manthan_signal_decisions first.',
            bad_count;
    END IF;
END $$;

ALTER TABLE manthan_positions
    ADD CONSTRAINT chk_active_has_classification CHECK (
        status NOT IN ('ACTIVE','PARTIAL_ACTIVE')
        OR (
            industry    IS NOT NULL AND industry    != ''
            AND mcap_bucket IS NOT NULL AND mcap_bucket != ''
            AND index_name  IS NOT NULL AND index_name  != ''
        )
    );

COMMENT ON CONSTRAINT chk_active_has_classification ON manthan_positions IS
    'Enforces that any ACTIVE/PARTIAL_ACTIVE position has industry, mcap_bucket and index_name populated. Required by the rebalancer top-up phase to look up the per-index EMA target.';

COMMIT;
