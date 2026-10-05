-- 015: one-time backfill for two reporting lags (2026-10-05). Idempotent.
--
-- (1) manthan_signal_decisions.status for ENTRY_BUY rows froze at DISPATCHED
--     because no fill/exit path advanced it. The code now sets CONFIRMED on
--     a confirmed fill and CLOSED / MANUALLY_EXITED on a confirmed exit
--     (positions_persist.go syncEntryDecision). This brings history in line
--     using the position row's own status as truth.
-- (2) manthan_portfolio_state.active_count was written from the in-memory
--     count BEFORE the new position was added, lagging by one. The code now
--     re-derives it from manthan_positions after every persist; this
--     re-derives it once for every strategy.
BEGIN;

UPDATE manthan_signal_decisions d
SET status = 'CONFIRMED', final_status_at = COALESCE(d.final_status_at, mp.updated_at, now())
FROM manthan_positions mp
WHERE d.signal_type = 'ENTRY_BUY' AND d.status = 'DISPATCHED'
  AND mp.signal_id IS NOT NULL AND d.signal_id = mp.signal_id
  AND mp.status IN ('ACTIVE','EXIT_PENDING','PARTIALLY_FILLED');

UPDATE manthan_signal_decisions d
SET status = CASE WHEN UPPER(COALESCE(mp.exit_reason,'')) LIKE '%MANUAL%'
                  THEN 'MANUALLY_EXITED' ELSE 'CLOSED' END,
    final_status_at = COALESCE(mp.exit_time, mp.updated_at, now())
FROM manthan_positions mp
WHERE d.signal_type = 'ENTRY_BUY' AND d.status IN ('DISPATCHED','CONFIRMED')
  AND mp.signal_id IS NOT NULL AND d.signal_id = mp.signal_id
  AND mp.status = 'EXITED';

UPDATE manthan_portfolio_state ps
SET active_count = (SELECT COUNT(*) FROM manthan_positions mp
                    WHERE mp.strategy_id = ps.strategy_id
                      AND mp.status IN ('PENDING_ENTRY','PARTIALLY_FILLED','ACTIVE','EXIT_PENDING')),
    updated_at = now();

COMMIT;
