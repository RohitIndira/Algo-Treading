-- 016_no_duplicate_active_entry.sql
--
-- DB-level defense against concurrent duplicate entry-order placement.
--
-- Rules-engine already emits idempotent trade signals (deterministic
-- signal_id + ON CONFLICT dedup — see rules-engine commit f2e72da).
-- BUT trade-execution's signal_inbox → entry_handler pipeline is also
-- concurrent (4 workers per inbox_worker by default), and the current
-- dedup path in entry_handler is:
--
--     existing := repo.GetActiveEntryBySymbol(strategy_id, symbol)
--     if existing == nil { repo.InsertOrder(...) }
--
-- That's a check-then-insert, not atomic. Two workers picking up two
-- signals (different signal_ids, same strategy+symbol) within
-- milliseconds of each other BOTH see "no existing entry" AND BOTH
-- insert. Result: two LIMIT BUY orders sent to the broker, both fill,
-- double position. Observed 2026-07-15 (CUB placed 17ms apart, id=2 and
-- id=4, both FILLED at broker, orphan 90 shares).
--
-- Even with the rules-engine fix, this in-process race remains
-- theoretically reachable IF:
--   - the two racing signals arrived at trade-execution before the
--     first one's decision insert landed in rules-engine's DB, OR
--   - some future path publishes to trade-signals without going through
--     rules-engine (manual injection, upstream service change), OR
--   - a third party publishes to the same Kafka topic
--
-- Defense in depth: a partial UNIQUE INDEX at the trade-execution DB
-- level. Whatever the upstream does, we NEVER hold two active entry
-- orders for the same (strategy, symbol, order_type) at the broker.
--
-- Semantics:
--
--   "for a given (strategy_id, symbol, order_type), only ONE row may be
--    in a non-terminal state (PENDING / PLACED / PARTIAL) at a time."
--
-- Scope carefully:
--   - Only entry orders (LIMIT_BUY, MARKET_BUY). SL orders use
--     different order_type values (SL_LIMIT_SELL etc) and are unaffected
--     — each entry can still have its own SL order sitting active
--     alongside it.
--   - Only non-terminal states. Once an entry reaches FILLED / CANCELLED
--     / REJECTED, the row leaves the partial index and a subsequent
--     entry for the same (strategy, symbol) is allowed (e.g. after a
--     manual exit + rebuy, or after price reversal + strategic re-entry).
--   - Per-strategy scope: same symbol across DIFFERENT strategies still
--     allowed (multi-user with the same stock is fine).
--
-- When the second concurrent insert races and hits this index, Postgres
-- returns error 23505 (unique_violation). Repository.InsertOrder detects
-- this via the `pq` driver's pgerror code and returns the
-- ErrDuplicateActiveEntry sentinel. entry_handler.go treats that as a
-- non-retryable "sibling worker won the race" outcome — logs, returns
-- gracefully, does NOT touch the broker.
--
-- Idempotency: CREATE INDEX IF NOT EXISTS is safe to re-run. If an
-- existing row would violate the new constraint the CREATE would fail
-- — but at the time of introduction the ops runbook is "flush all
-- non-terminal rows first" and the local db is clean. In production,
-- run this migration during a maintenance window with the same
-- precondition, or during off-hours when no active entries exist.

CREATE UNIQUE INDEX IF NOT EXISTS uq_manthan_orders_active_entry
    ON public.manthan_orders (strategy_id, symbol, order_type)
    WHERE order_type IN ('LIMIT_BUY', 'MARKET_BUY')
      AND status     IN ('PENDING',  'PLACED', 'PARTIAL');

COMMENT ON INDEX public.uq_manthan_orders_active_entry IS
    'Only one non-terminal entry order per (strategy, symbol, order_type). Prevents concurrent duplicate placement race — see 016_no_duplicate_active_entry.sql';
