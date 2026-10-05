-- Migration 014: flexi-caps audit schema (manthan_signal_decisions)
--
-- TARGET DB: trading_db (the Manthan portfolio DB — on prod that is the
--            docker Postgres on host port 5442; locally localhost:5432).
--            Apply with `psql -v ON_ERROR_STOP=1 -f` like every other file
--            in this directory (ON_ERROR_STOP so a refused pre-flight guard
--            stops the whole script instead of running the later statements).
--            Verified 2026-10-05 on a schema copy: fresh apply, re-apply
--            (idempotent), apply with rows present, and the guard refusing an
--            out-of-band value while leaving the live constraint untouched.
--
-- WHY
--   Flexi caps ("Provable-Void Release", design brief 2026-09-30) lets a
--   strategy lend a bucket's unused 50% base-cap slots to another bucket
--   when the donor bucket provably has no eligible name for the strategy
--   today. Before it is ever turned on it runs in DRY-RUN, writing one
--   FLEXI_EVAL row per evaluated signal so the operator can compare what the
--   engine WOULD have done against what it did. Those rows need:
--
--     signal_type = 'FLEXI_EVAL'   — a new discriminator (chk_msd_signal_type
--                                    is a closed CHECK list, mig 009)
--     status      = 'EVALUATED'    — a terminal, non-lifecycle status so the
--                                    rows are inert to every existing reader:
--                                    the recovery worker scans PROPOSED only,
--                                    lookupParentSignalID wants DISPATCHED/
--                                    CONFIRMED/PARTIAL, dashboards count
--                                    REJECTED (chk_msd_status, mig 003/005)
--     flexi_grant JSONB NULL       — on-mode only: the borrowed-slot record
--                                    stored on the real ENTRY_BUY row, kept
--                                    OUT of kafka_payload so the trade-signals
--                                    bytes the recovery worker re-publishes
--                                    are unchanged in every mode
--
--   Using REJECTED + a reason prefix instead was rejected: it pollutes the
--   rejection dashboards and shares uq_msd_entry_per_attempt's ENTRY_BUY
--   scope. FLEXI_EVAL rows carry their event in `payload` (the column that
--   chk_msd_non_entry_needs_payload actually checks — NOT kafka_payload).
--
--   FLEXI_EVAL rows are FIRST-WRITE-WINS per (strategy, symbol, run_date):
--   the deterministic signal_id carries no outcome/attempt discriminator and
--   the writer uses ON CONFLICT DO NOTHING, so a same-day re-evaluation with
--   a different outcome is dropped. The Phase-1 evidence queries therefore
--   see the first evaluation of each symbol per day (normal days publish
--   each symbol once, so this is a fidelity limit, not a failed dry-run).
--
-- SAFETY / LOCKING — read before running
--   * Additive only. No existing row changes value or meaning.
--   * Run OFF-HOURS (outside 09:00–15:30 IST and the 16:35 EOD window).
--     Every ALTER below takes ACCESS EXCLUSIVE on manthan_signal_decisions
--     for the duration of ITS transaction; the script is split into several
--     transactions so each lock is brief:
--       tx1  pre-flight guard + DROP/ADD ... NOT VALID (x2) + ADD COLUMN —
--            metadata-only, no table scan, ACCESS EXCLUSIVE for milliseconds.
--       then VALIDATE CONSTRAINT (x2), each in its own implicit transaction —
--            full-table scan under SHARE UPDATE EXCLUSIVE, so rules-engine's
--            INSERT/UPDATE/SELECT on the table continue meanwhile. (Putting
--            NOT VALID and VALIDATE in ONE transaction would hold the
--            ACCESS EXCLUSIVE from the ADD across the scan — that is exactly
--            what this layout avoids.)
--       then CREATE INDEX IF NOT EXISTS (plain, not CONCURRENTLY) — SHARE
--            lock: concurrent reads continue, writes wait for the build. The
--            table is small (thousands of rows), so this is sub-second; a
--            CONCURRENTLY build was rejected because a failed CONCURRENTLY
--            leaves an INVALID index that IF NOT EXISTS would then skip.
--     Net: never run this during market hours anyway — the publisher's
--     inserts and the allocator's 500 ms user-override query would wait on
--     the index build / tx1 and the override check fails OPEN.
--   * Pre-flight guard (tx1): the two CHECKs are re-created with explicit
--     lists (mig 009 + FLEXI_EVAL, mig 005 + EVALUATED). If the LIVE
--     constraint admits any value that is NOT in the new list (added
--     out-of-band), the guard RAISEs and tx1 rolls back — this script never
--     silently NARROWS a constraint. Fix the list below, then re-run.
--   * Every existing value is a member of the widened lists, so VALIDATE
--     cannot fail on data.
--   * Idempotent: DROP CONSTRAINT IF EXISTS / ADD COLUMN IF NOT EXISTS /
--     CREATE INDEX IF NOT EXISTS; re-running is a no-op apart from the
--     brief re-create of the two CHECKs.
--   * rules-engine probes this schema at boot (probeFlexiSchema): with
--     MANTHAN_FLEXI_CAPS_MODE != off and this migration NOT applied, the
--     mode is forced off with an Error log — never a silent zero-row dry-run.
--
-- ENABLE PROCEDURE (after this migration) — see migrations/README.md
--   PM2 caches the env at the last `pm2 start/restart <file>`; editing .env
--   alone and `pm2 restart rules-engine` does NOT pick the new keys up via
--   the env block. Use:
--     pm2 restart deployments/separate-namespace/ecosystem.config.js --only rules-engine --update-env
--   then grep the boot log for "Manthan flexi caps ENABLED mode=dry_run".
--
-- ROLLBACK (only with MANTHAN_FLEXI_CAPS_MODE unset/off everywhere):
--   DELETE FROM manthan_signal_decisions WHERE signal_type = 'FLEXI_EVAL';
--   ALTER TABLE manthan_signal_decisions DROP COLUMN IF EXISTS flexi_grant;
--   DROP INDEX IF EXISTS idx_msd_flexi_eval;
--   then re-add chk_msd_signal_type (mig 009 list) and chk_msd_status
--   (mig 005 list) without the new values.

-- =============================================================================
-- tx1: pre-flight guard + metadata-only changes (brief ACCESS EXCLUSIVE)
-- =============================================================================
BEGIN;

-- Pre-flight: refuse to narrow either CHECK. Compares the quoted literals in
-- the live constraint definition against the lists re-created below. A
-- missing constraint (fresh DB) passes — there is nothing to narrow.
DO $$
DECLARE
    live_def  text;
    lit       text;
    allowed_signal_types text[] := ARRAY['ENTRY_BUY','SL_MODIFY','EXIT_TSL','EXIT_MANUAL','SL_CANCEL','FLEXI_EVAL'];
    allowed_statuses     text[] := ARRAY['PROPOSED','DISPATCHED','CONFIRMED','PARTIAL','REJECTED','TIMED_OUT','CLOSED','MANUALLY_EXITED','EVALUATED'];
BEGIN
    SELECT pg_get_constraintdef(oid) INTO live_def
    FROM pg_constraint
    WHERE conrelid = 'manthan_signal_decisions'::regclass AND conname = 'chk_msd_signal_type';
    IF live_def IS NOT NULL THEN
        FOR lit IN SELECT m[1] FROM regexp_matches(live_def, '''([^'']*)''', 'g') AS m LOOP
            IF NOT (lit = ANY (allowed_signal_types)) THEN
                RAISE EXCEPTION 'migration 014: live chk_msd_signal_type admits % which the new list does not — refusing to narrow the constraint. Live def: %', lit, live_def;
            END IF;
        END LOOP;
    END IF;

    SELECT pg_get_constraintdef(oid) INTO live_def
    FROM pg_constraint
    WHERE conrelid = 'manthan_signal_decisions'::regclass AND conname = 'chk_msd_status';
    IF live_def IS NOT NULL THEN
        FOR lit IN SELECT m[1] FROM regexp_matches(live_def, '''([^'']*)''', 'g') AS m LOOP
            IF NOT (lit = ANY (allowed_statuses)) THEN
                RAISE EXCEPTION 'migration 014: live chk_msd_status admits % which the new list does not — refusing to narrow the constraint. Live def: %', lit, live_def;
            END IF;
        END LOOP;
    END IF;
END $$;

-- 1) signal_type: admit FLEXI_EVAL (NOT VALID here; VALIDATE below, own tx)
ALTER TABLE manthan_signal_decisions
    DROP CONSTRAINT IF EXISTS chk_msd_signal_type;
ALTER TABLE manthan_signal_decisions
    ADD CONSTRAINT chk_msd_signal_type CHECK (signal_type IN (
        'ENTRY_BUY',
        'SL_MODIFY',
        'EXIT_TSL',
        'EXIT_MANUAL',
        'SL_CANCEL',
        'FLEXI_EVAL'       -- flexi-caps evaluation (dry_run prediction / on-mode fact)
    )) NOT VALID;

-- 2) status: admit EVALUATED (NOT VALID here; VALIDATE below, own tx)
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
        'MANUALLY_EXITED',   -- user-driven exit detected outside our system
        'EVALUATED'          -- FLEXI_EVAL terminal state: never dispatched, never retried
    )) NOT VALID;

-- 3) flexi_grant — on-mode borrowed-slot audit on the real ENTRY_BUY row
ALTER TABLE manthan_signal_decisions
    ADD COLUMN IF NOT EXISTS flexi_grant JSONB NULL;

COMMENT ON COLUMN manthan_signal_decisions.flexi_grant IS
    'Flexi caps (on-mode only): {recipient, donors, base, ceiling, held_before, spare_before, pool_free, plan} when this entry took a slot above its bucket''s base cap. NULL for every base-cap entry and in dry_run/off. Never part of kafka_payload.';

COMMIT;

-- =============================================================================
-- VALIDATE — each statement is its own transaction (psql autocommit), so the
-- full-table scan runs under SHARE UPDATE EXCLUSIVE and does NOT block
-- rules-engine's reads/writes. Both are no-ops on data (every existing value
-- is in the widened lists).
-- =============================================================================
ALTER TABLE manthan_signal_decisions
    VALIDATE CONSTRAINT chk_msd_signal_type;

ALTER TABLE manthan_signal_decisions
    VALIDATE CONSTRAINT chk_msd_status;

-- =============================================================================
-- 4) Partial index for the dry-run shadow book + evidence queries
--    (own implicit transaction; SHARE lock for the sub-second build)
-- =============================================================================
-- loadShadowBook: WHERE strategy_id=$1 AND signal_type='FLEXI_EVAL' AND
-- status='EVALUATED' AND payload->>'run_date'=$2 AND payload->>'outcome'=…
-- Evidence sweeps group by (strategy_id, decided_at::date).
CREATE INDEX IF NOT EXISTS idx_msd_flexi_eval
    ON manthan_signal_decisions (strategy_id, decided_at)
    WHERE signal_type = 'FLEXI_EVAL';

-- =============================================================================
-- Verification (run after migration)
-- =============================================================================
--   SELECT conname, convalidated, pg_get_constraintdef(oid)
--   FROM pg_constraint
--   WHERE conrelid = 'manthan_signal_decisions'::regclass
--     AND conname IN ('chk_msd_signal_type','chk_msd_status');
--   -- expect: both convalidated = t; lists contain 'FLEXI_EVAL' / 'EVALUATED'
--
--   SELECT column_name FROM information_schema.columns
--   WHERE table_name='manthan_signal_decisions' AND column_name='flexi_grant';
--   -- expect: 1 row
--
--   SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_msd_flexi_eval';
--   -- expect: partial index WHERE signal_type = 'FLEXI_EVAL'
