-- Migration 005: Open up event types + decisions table to handle manual user interference
--
-- Real-world scenarios this enables:
--   - User logs into broker mobile app and sells our position manually
--   - User cancels our SL order without selling (position becomes unprotected)
--   - User buys more of the same symbol outside our system
--   - Broker order-book vs our DB diverge for any other reason
--
-- Rules-engine + trade-execution will detect these via a new
-- ExternalActivityDetector goroutine (Step 2). When detected, an event is
-- published to manthan.execution.events; the projector flips the position
-- to EXITED with exit_reason='MANUAL_EXIT' and sets user_override_until to
-- block re-entry for a cool-down window (default 3 days).
--
-- This migration is non-destructive:
--   1. Widens manthan_position_events.event_type CHECK to accept the new types
--   2. Adds manthan_signal_decisions.user_override_until (nullable timestamp)
--   3. Widens manthan_signal_decisions.status CHECK to accept MANUALLY_EXITED
--
-- Rollback (if ever needed):
--   ALTER TABLE manthan_position_events DROP CONSTRAINT chk_mpe_event_type;
--   ALTER TABLE manthan_position_events ADD CONSTRAINT chk_mpe_event_type ...
--   ALTER TABLE manthan_signal_decisions DROP COLUMN user_override_until;
--   ALTER TABLE manthan_signal_decisions DROP CONSTRAINT chk_msd_status;
--   ALTER TABLE manthan_signal_decisions ADD CONSTRAINT chk_msd_status ...

BEGIN;

-- =============================================================================
-- 1) Widen manthan_position_events.event_type to accept manual-interference events
-- =============================================================================

ALTER TABLE manthan_position_events
    DROP CONSTRAINT IF EXISTS chk_mpe_event_type;

ALTER TABLE manthan_position_events
    ADD CONSTRAINT chk_mpe_event_type CHECK (event_type IN (
        -- existing types (must stay accepted)
        'ENTRY_FILLED',
        'ENTRY_PARTIAL_FILL',
        'ENTRY_REJECTED',
        'ENTRY_TIMED_OUT',
        'SL_PLACED',
        'SL_MODIFIED',
        'SL_REJECTED',
        'SL_FILLED',
        'EXIT_FILLED',
        'RECONCILER_DRIFT_FIX',

        -- new types — manual user interference outside our system
        'MANUAL_EXIT_DETECTED',           -- user sold the whole position
        'MANUAL_PARTIAL_EXIT_DETECTED',   -- user sold some, kept some
        'MANUAL_BUY_DETECTED',            -- user bought more of same symbol
        'MANUAL_SL_CANCELLED_DETECTED',   -- user cancelled our SL order
        'EXTERNAL_QTY_MISMATCH'           -- broker qty != db qty, generic
    ));

COMMENT ON COLUMN manthan_position_events.event_type IS
    'Event class. Includes broker-confirmed lifecycle events plus MANUAL_*_DETECTED for user interference outside our system (detected via WSS / position polling / reconciler).';

-- =============================================================================
-- 2) Add user_override_until column to manthan_signal_decisions
-- =============================================================================
-- When a user manually exits a position, we set this to NOW() + N days. The
-- allocator checks this before generating a new entry decision for the same
-- (strategy_id, symbol). Prevents the algo from fighting the user's manual
-- intent. NULL means no override (default).

ALTER TABLE manthan_signal_decisions
    ADD COLUMN IF NOT EXISTS user_override_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_msd_user_override
    ON manthan_signal_decisions(strategy_id, symbol, user_override_until)
    WHERE user_override_until IS NOT NULL;

COMMENT ON COLUMN manthan_signal_decisions.user_override_until IS
    'When set, allocator must not generate new decisions for (strategy_id, symbol) until this time has passed. Set on MANUAL_EXIT_DETECTED to respect user intent.';

-- =============================================================================
-- 3) Widen manthan_signal_decisions.status to accept MANUALLY_EXITED
-- =============================================================================
-- New status reflects "position closed by user, not by algo" — distinct from
-- CLOSED (algo-driven exit) for accounting / forensics.

ALTER TABLE manthan_signal_decisions
    DROP CONSTRAINT IF EXISTS chk_msd_status;

ALTER TABLE manthan_signal_decisions
    ADD CONSTRAINT chk_msd_status CHECK (status IN (
        'PROPOSED',
        'DISPATCHED',
        'CONFIRMED',
        'PARTIAL',
        'REJECTED',
        'TIMED_OUT',
        'CLOSED',            -- algo-driven exit (SL hit / EOD)
        'MANUALLY_EXITED'    -- user-driven exit detected outside our system
    ));

COMMENT ON COLUMN manthan_signal_decisions.status IS
    'Lifecycle state. CLOSED = algo-exited; MANUALLY_EXITED = user exited via broker app/web.';

-- =============================================================================
-- 4) Widen manthan_positions.exit_reason hints (optional doc-only, no constraint)
-- =============================================================================
-- exit_reason is a free-text VARCHAR(50). New permitted values, by convention:
--   SL_TRIGGERED       — algo SL fired
--   MANUAL_EXIT        — user fully exited
--   MANUAL_PARTIAL     — user partial exit
--   EOD_SQUAREOFF      — broker auto-square-off (margin call, intraday close)
--   RECONCILER_FIX     — reconciler corrected drift
-- No CHECK constraint added — the column stays free-form for future
-- exit reasons we haven't anticipated.

COMMIT;

-- =============================================================================
-- Verification queries (run after migration)
-- =============================================================================
--
-- 1. New event_type CHECK accepts MANUAL_*:
--    \d manthan_position_events
--    -- should show 15 allowed values in chk_mpe_event_type
--
-- 2. New column exists:
--    \d manthan_signal_decisions
--    -- should include user_override_until TIMESTAMPTZ
--
-- 3. New status MANUALLY_EXITED accepted:
--    \d manthan_signal_decisions
--    -- should show 8 values in chk_msd_status
--
-- 4. Quick smoke test (will roll back):
--    BEGIN;
--    INSERT INTO manthan_position_events (signal_id, event_seq, event_type, source, raw_payload)
--    VALUES ('00000000-0000-0000-0000-000000000000'::uuid, 1, 'MANUAL_EXIT_DETECTED', 'API_POLLER', '{}'::jsonb);
--    ROLLBACK;
