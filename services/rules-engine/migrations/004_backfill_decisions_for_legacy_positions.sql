-- Migration 004: Backfill manthan_signal_decisions for pre-Step-3 legacy rows
--
-- After 003_signal_decisions_and_position_events:
--   - new manthan_positions rows MUST have signal_id (written by projector
--     from a manthan_signal_decisions row that already exists)
--   - LEGACY rows (4 of them as of 2026-04-27: KINGFA, APARINDS, LLOYDSME,
--     NATIONALUM) have signal_id = NULL and no matching decisions row
--
-- This migration:
--   1. Generates a synthetic signal_id for each legacy row (gen_random_uuid)
--   2. Inserts a backfill manthan_signal_decisions row in a terminal status
--      that mirrors the position's current state (ACTIVE→CONFIRMED,
--      EXITED→CLOSED). decided_at = position's entry_time so the audit log
--      stays chronologically meaningful.
--   3. Backfills manthan_positions.signal_id with the new UUID + a
--      placeholder broker_order_id from manthan_orders if available.
--
-- Idempotent — re-running has no effect (only touches rows where
-- signal_id IS NULL).
--
-- Safe — does NOT change status or any business field on legacy rows; only
-- fills in linkage columns. Rollback is just to clear the columns again.

BEGIN;

-- 1. Generate UUIDs for legacy rows (in a CTE we can reference later).
WITH legacy AS (
    SELECT
        id,
        strategy_id,
        user_id,
        symbol,
        isin,
        industry,
        mcap_bucket,
        index_name,
        entry_price,
        quantity,
        invested_amt,
        ema_alloc_pct,
        current_sl,
        status,
        entry_time,
        gen_random_uuid() AS new_signal_id
    FROM manthan_positions
    WHERE signal_id IS NULL
),

-- 2. Insert a backfill decision row for each. Status reflects current
-- position state; rejection_reason notes this is a backfill.
inserted AS (
    INSERT INTO manthan_signal_decisions (
        signal_id, user_id, strategy_id, symbol, isin,
        decided_at,
        ltp_at_decision, ema_alloc_pct, intended_qty, intended_invested,
        initial_sl_target, industry, mcap_bucket, index_name,
        status, dispatched_at, final_status_at,
        rejection_reason
    )
    SELECT
        l.new_signal_id, l.user_id, l.strategy_id, l.symbol, l.isin,
        l.entry_time,
        l.entry_price,
        COALESCE(l.ema_alloc_pct, 0),
        l.quantity,
        l.invested_amt,
        COALESCE(l.current_sl, l.entry_price * 0.80),
        l.industry, l.mcap_bucket, l.index_name,
        CASE
            WHEN l.status = 'EXITED' THEN 'CLOSED'
            WHEN l.status IN ('ACTIVE','PARTIAL_ACTIVE') THEN 'CONFIRMED'
            ELSE 'CONFIRMED'
        END,
        l.entry_time,                               -- dispatched_at = entry_time
        CASE WHEN l.status = 'EXITED' THEN l.entry_time ELSE NULL END,
        'backfilled by migration 004 (pre-CQRS row, no original decision)'
    FROM legacy l
    ON CONFLICT (signal_id) DO NOTHING            -- safety: re-run noop
    RETURNING signal_id
)

-- 3. Backfill manthan_positions with the generated signal_id.
UPDATE manthan_positions p
SET
    signal_id = l.new_signal_id,
    -- broker_order_id pulled from manthan_orders (different DB) is not
    -- straightforward without dblink; leave NULL — reconciler will
    -- populate on its next pass when it sees the active row.
    event_seq = COALESCE(p.event_seq, 0)
FROM (
    SELECT
        id,
        gen_random_uuid() AS new_signal_id  -- unused — see note
    FROM manthan_positions
    WHERE signal_id IS NULL
) AS l
WHERE p.id = l.id AND p.signal_id IS NULL;

-- NOTE on the above: the CTE-based INSERT above used its own gen_random_uuid
-- per-row, but it was scoped inside the CTE and the UUID is not visible
-- here. Postgres re-evaluates gen_random_uuid() per call, so we'd get
-- different UUIDs in step 2 vs step 3 if we used CTE chaining. That's why
-- we split into two passes:
--   Pass A (CTE inserted): one decision per legacy row, fresh UUID
--   Pass B (this UPDATE): generates a NEW UUID per legacy row and links it
--
-- Result: each legacy position has both a decision AND a position row, but
-- their signal_ids differ. To fix this properly we need a per-row
-- generation plus join — done in the corrected version below.

ROLLBACK;  -- discard the broken approach above and use the corrected one


-- =============================================================================
-- Corrected approach: do it row-by-row in PL/pgSQL so the UUID generation is
-- a SINGLE value used in both the decisions INSERT and the positions UPDATE.
-- =============================================================================

BEGIN;

DO $$
DECLARE
    legacy_row record;
    new_sid uuid;
    final_status varchar(20);
    final_at timestamptz;
BEGIN
    FOR legacy_row IN
        SELECT * FROM manthan_positions WHERE signal_id IS NULL
    LOOP
        new_sid := gen_random_uuid();

        IF legacy_row.status = 'EXITED' THEN
            final_status := 'CLOSED';
            final_at := COALESCE(legacy_row.exit_time, legacy_row.entry_time);
        ELSE
            final_status := 'CONFIRMED';
            final_at := NULL;
        END IF;

        INSERT INTO manthan_signal_decisions (
            signal_id, user_id, strategy_id, symbol, isin,
            decided_at,
            ltp_at_decision, ema_alloc_pct, intended_qty, intended_invested,
            initial_sl_target, industry, mcap_bucket, index_name,
            status, dispatched_at, final_status_at,
            rejection_reason
        ) VALUES (
            new_sid, legacy_row.user_id, legacy_row.strategy_id,
            legacy_row.symbol, legacy_row.isin,
            legacy_row.entry_time,
            legacy_row.entry_price,
            COALESCE(legacy_row.ema_alloc_pct, 0),
            legacy_row.quantity,
            legacy_row.invested_amt,
            COALESCE(legacy_row.current_sl, legacy_row.entry_price * 0.80),
            legacy_row.industry, legacy_row.mcap_bucket, legacy_row.index_name,
            final_status,
            legacy_row.entry_time,
            final_at,
            'backfilled by migration 004 (pre-CQRS row, no original decision)'
        )
        ON CONFLICT (signal_id) DO NOTHING;

        UPDATE manthan_positions
        SET signal_id = new_sid
        WHERE id = legacy_row.id AND signal_id IS NULL;

        RAISE NOTICE 'Backfilled position id=% symbol=% status=% → signal_id=%',
            legacy_row.id, legacy_row.symbol, legacy_row.status, new_sid;
    END LOOP;
END $$;

COMMIT;

-- =============================================================================
-- Verification queries (run after this migration)
-- =============================================================================
--
-- Expect: zero NULL signal_ids on manthan_positions
--   SELECT count(*) FROM manthan_positions WHERE signal_id IS NULL;
--
-- Expect: matching decision per legacy position
--   SELECT p.id, p.symbol, p.status, d.status AS decision_status
--   FROM manthan_positions p
--   LEFT JOIN manthan_signal_decisions d ON d.signal_id = p.signal_id
--   ORDER BY p.id;
