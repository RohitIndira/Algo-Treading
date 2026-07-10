-- Migration 009: Signal types + outbox pattern support
--
-- Extends manthan_signal_decisions to hold ALL signal types rules-engine fires,
-- not just entries. Follows the ratified final target
-- (docs/rules_engine_refactor.md §4.5) — one audit row per signal fired.
--
-- Signal types now covered:
--   ENTRY_BUY   — allocator picked a stock (existing behaviour)
--   SL_MODIFY   — trailing SL ratchet-up
--   EXIT_TSL    — LTP crossed trailing SL, rules-engine emits sell
--   EXIT_MANUAL — user-triggered exit
--   SL_CANCEL   — cancel a stuck/misplaced SL
--
-- Migration is NON-DESTRUCTIVE:
--   • New columns are additive.
--   • Existing entry-specific NOT NULL columns weakened to allow non-entry rows.
--     App-level CHECK enforces "ENTRY_BUY still requires them" so old code paths
--     don't silently miss required data.
--   • Existing status VARCHAR enum (PROPOSED/DISPATCHED/CONFIRMED/PARTIAL/
--     REJECTED/TIMED_OUT/CLOSED) is UNCHANGED — the outbox pattern uses the
--     existing PROPOSED → DISPATCHED transition with dispatched_at as the
--     "published_at" timestamp.
--
-- Rollback (if needed):
--   ALTER TABLE manthan_signal_decisions
--       DROP COLUMN signal_type,
--       DROP COLUMN parent_signal_id,
--       DROP COLUMN payload;
--   (existing rows/code paths unaffected)
--
-- The manthan_cooldown table already exists from migration 002 and needs no
-- changes for this refactor.

BEGIN;

-- =============================================================================
-- 1) New columns on manthan_signal_decisions
-- =============================================================================
-- signal_type   — discriminator across the 5 signal kinds
-- parent_signal_id — link SL_MODIFY / EXIT_TSL / EXIT_MANUAL / SL_CANCEL rows
--                    back to their original ENTRY_BUY signal
-- payload       — type-specific fields (new_trigger for SL_MODIFY,
--                 ltp_at_fire for EXIT_TSL, reason for EXIT_MANUAL, etc.)

ALTER TABLE manthan_signal_decisions
    ADD COLUMN IF NOT EXISTS signal_type       VARCHAR(32) NOT NULL DEFAULT 'ENTRY_BUY',
    ADD COLUMN IF NOT EXISTS parent_signal_id  UUID NULL REFERENCES manthan_signal_decisions(signal_id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS payload           JSONB NULL;

-- Validate signal_type enum. Extending later = add value here + one Go const.
ALTER TABLE manthan_signal_decisions
    DROP CONSTRAINT IF EXISTS chk_msd_signal_type;
ALTER TABLE manthan_signal_decisions
    ADD CONSTRAINT chk_msd_signal_type CHECK (signal_type IN (
        'ENTRY_BUY',
        'SL_MODIFY',
        'EXIT_TSL',
        'EXIT_MANUAL',
        'SL_CANCEL'
    ));

-- =============================================================================
-- 2) Weaken entry-only NOT NULL constraints (non-entry rows won't have them)
-- =============================================================================
-- ENTRY_BUY still requires ltp_at_decision, intended_qty, intended_invested,
-- initial_sl_target — enforced by CHECK below, not by NOT NULL.
-- SL_MODIFY / EXIT_TSL / EXIT_MANUAL / SL_CANCEL use payload JSONB instead.

ALTER TABLE manthan_signal_decisions
    ALTER COLUMN ltp_at_decision   DROP NOT NULL,
    ALTER COLUMN intended_qty      DROP NOT NULL,
    ALTER COLUMN intended_invested DROP NOT NULL,
    ALTER COLUMN initial_sl_target DROP NOT NULL;

-- Enforce entry-row completeness at the DB layer. A row with
-- signal_type='ENTRY_BUY' still must have all four fields set — protects
-- against future callers accidentally shipping incomplete entry rows.
-- Non-entry rows are exempt and use payload for their data.
ALTER TABLE manthan_signal_decisions
    DROP CONSTRAINT IF EXISTS chk_msd_entry_fields_required;
ALTER TABLE manthan_signal_decisions
    ADD CONSTRAINT chk_msd_entry_fields_required CHECK (
        signal_type != 'ENTRY_BUY' OR (
            ltp_at_decision   IS NOT NULL AND
            intended_qty      IS NOT NULL AND
            intended_invested IS NOT NULL AND
            initial_sl_target IS NOT NULL
        )
    );

-- Non-entry rows must carry a payload (their type-specific fields live there).
ALTER TABLE manthan_signal_decisions
    DROP CONSTRAINT IF EXISTS chk_msd_non_entry_needs_payload;
ALTER TABLE manthan_signal_decisions
    ADD CONSTRAINT chk_msd_non_entry_needs_payload CHECK (
        signal_type = 'ENTRY_BUY' OR payload IS NOT NULL
    );

-- =============================================================================
-- 3) Indexes for the new query patterns
-- =============================================================================

-- "Show me all signals of type X for user Y on symbol Z" — audit query.
CREATE INDEX IF NOT EXISTS idx_msd_type_user_symbol
    ON manthan_signal_decisions(signal_type, user_id, symbol, decided_at DESC);

-- "Show me the full lifecycle for entry X" — traverse via parent_signal_id.
CREATE INDEX IF NOT EXISTS idx_msd_parent_signal
    ON manthan_signal_decisions(parent_signal_id)
    WHERE parent_signal_id IS NOT NULL;

-- Outbox worker: "find signals we INSERTed but haven't published yet"
-- (status='PROPOSED' with no dispatched_at). Already covered by existing
-- idx_msd_pending — no new index needed here.

-- =============================================================================
-- 4) Column comments (for anyone querying via \d or psql)
-- =============================================================================

COMMENT ON COLUMN manthan_signal_decisions.signal_type IS
    'What kind of signal rules-engine fired: ENTRY_BUY, SL_MODIFY, EXIT_TSL, EXIT_MANUAL, SL_CANCEL.';
COMMENT ON COLUMN manthan_signal_decisions.parent_signal_id IS
    'For SL_MODIFY / EXIT_* / SL_CANCEL rows: FK to the original ENTRY_BUY signal_id. NULL for entries.';
COMMENT ON COLUMN manthan_signal_decisions.payload IS
    'Type-specific fields as JSONB. SL_MODIFY: {new_trigger, new_limit, old_trigger, ltp_when_fired}. EXIT_TSL: {ltp_at_fire, sl_at_fire}. EXIT_MANUAL: {reason}. SL_CANCEL: {reason}. NULL for ENTRY_BUY (fields live in explicit columns).';

COMMIT;

-- =============================================================================
-- Verification queries (run after migration)
-- =============================================================================
--
-- 1. New columns present:
--    \d manthan_signal_decisions
--    -- expect: signal_type, parent_signal_id, payload
--
-- 2. Existing rows retroactively marked ENTRY_BUY:
--    SELECT signal_type, count(*)
--    FROM manthan_signal_decisions
--    GROUP BY signal_type;
--    -- expect: ENTRY_BUY = <existing row count>, others = 0
--
-- 3. NOT NULL weakening applied:
--    SELECT column_name, is_nullable
--    FROM information_schema.columns
--    WHERE table_name = 'manthan_signal_decisions'
--      AND column_name IN ('ltp_at_decision','intended_qty','intended_invested','initial_sl_target');
--    -- expect: all is_nullable = YES
--
-- 4. CHECK constraints in place:
--    SELECT conname, pg_get_constraintdef(oid)
--    FROM pg_constraint
--    WHERE conrelid = 'manthan_signal_decisions'::regclass
--      AND conname LIKE 'chk_msd_%'
--    ORDER BY conname;
--    -- expect: chk_msd_entry_fields_required, chk_msd_non_entry_needs_payload,
--    --         chk_msd_signal_type, chk_msd_status (from mig 003)
--
-- 5. Try INSERT of a non-entry row (should succeed):
--    -- INSERT INTO manthan_signal_decisions
--    --   (signal_id, user_id, strategy_id, symbol, parent_signal_id,
--    --    signal_type, payload, decided_at, status)
--    -- VALUES
--    --   (gen_random_uuid(), 'S4450', 'a844a844-...', 'IDEA',
--    --    '<entry-signal-id>', 'SL_MODIFY',
--    --    '{"new_trigger":12.85,"old_trigger":12.60}'::jsonb, NOW(), 'PROPOSED');
--
-- 6. Try INSERT of an entry row WITHOUT ltp_at_decision (should FAIL):
--    -- INSERT INTO manthan_signal_decisions
--    --   (signal_id, user_id, strategy_id, symbol, signal_type, decided_at, status)
--    -- VALUES
--    --   (gen_random_uuid(), 'S4450', 'a844a844-...', 'BROKEN',
--    --    'ENTRY_BUY', NOW(), 'PROPOSED');
--    -- expect: ERROR — chk_msd_entry_fields_required
