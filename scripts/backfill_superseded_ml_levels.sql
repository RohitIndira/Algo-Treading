-- ============================================================================
-- Backfill: relabel mislabelled CANCELLED multi-level exit rows as SUPERSEDED
-- ============================================================================
--
-- Context (Option C fix — branch fix/ml-paper-exit-superseded):
--   Before the fix, when the paper monitor closed an ML position via its SL/TP
--   safety-net, the un-fired ladder levels were left PENDING and later swept to
--   CANCELLED by strategy deactivation. That made it look like the remaining qty
--   was abandoned — when in fact it WAS exited (see the PAPER_EXITED event).
--
--   This script relabels those historical rows from CANCELLED to SUPERSEDED so
--   the closed-positions data is honest. It ONLY touches levels whose entry order
--   actually has a PAPER_EXITED execution event — i.e. the position genuinely
--   exited. Genuinely-cancelled levels (strategy stopped while position open,
--   no PAPER_EXITED) are left as CANCELLED.
--
-- SAFETY:
--   * Review the PREVIEW output before running the UPDATE.
--   * The UPDATE is wrapped in a transaction — inspect, then COMMIT or ROLLBACK.
--   * Idempotent: re-running changes nothing once rows are SUPERSEDED.
-- ============================================================================

-- ── 1. PREVIEW: rows that WILL be changed ───────────────────────────────────
SELECT ml.entry_order_id,
       o.symbol,
       o.strategy_name,
       ml.exit_type,
       ml.level_num,
       ml.status                AS current_status,
       'SUPERSEDED'             AS new_status,
       ee.created_at            AS paper_exited_at
FROM   multi_level_exit_levels ml
JOIN   orders o            ON o.order_id = ml.entry_order_id
JOIN   execution_events ee ON ee.order_id = ml.entry_order_id
                          AND ee.event_type = 'PAPER_EXITED'
WHERE  ml.status = 'CANCELLED'
  AND  o.is_paper_trade = true
ORDER  BY ee.created_at, ml.entry_order_id, ml.exit_type, ml.level_num;

-- ── 2. APPLY (review preview above first) ───────────────────────────────────
-- Uncomment the block below to apply. Inspect the row count, then COMMIT/ROLLBACK.
--
-- BEGIN;
--
-- UPDATE multi_level_exit_levels ml
-- SET    status     = 'SUPERSEDED',
--        updated_at = NOW()
-- FROM   orders o
-- WHERE  o.order_id = ml.entry_order_id
--   AND  ml.status  = 'CANCELLED'
--   AND  o.is_paper_trade = true
--   AND  EXISTS (
--          SELECT 1 FROM execution_events ee
--          WHERE  ee.order_id = ml.entry_order_id
--            AND  ee.event_type = 'PAPER_EXITED'
--        );
--
-- -- Verify the affected count looks right, then:
-- -- COMMIT;   -- or  ROLLBACK;
