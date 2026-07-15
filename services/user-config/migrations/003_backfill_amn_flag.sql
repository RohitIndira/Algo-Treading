-- ============================================================================
-- User Config Service — Backfill process_after_market_news for pre-existing
-- strategies. Migration 003.
--
-- Before migration 002, process_after_market_news was creation-time-only and was
-- NOT stored on the strategies table (db:"-"), so 002's ADD COLUMN defaulted every
-- existing strategy to false — including strategies the user originally created as
-- AMN strategies. Those strategies would then never show the reactivation AMN
-- preview.
--
-- The original value IS recoverable: the STRATEGY_CREATED outbox event marshalled
-- the full strategy as JSON, and process_after_market_news has a JSON tag, so the
-- retained execution_outbox payload records the true creation-time value. This
-- migration restores the flag from that authoritative record.
--
-- Idempotent: only flips false → true where the outbox proves the strategy was
-- AMN; re-running is a no-op. Safe (updates nothing) if execution_outbox has been
-- pruned — in that case old AMN strategies cannot be auto-detected and must be
-- corrected another way (e.g. recreated, or flagged manually).
--
-- Apply with:
--   psql -h <host> -U <user> -d <db> -f migrations/003_backfill_amn_flag.sql
-- ============================================================================

UPDATE strategies s
SET process_after_market_news = true
WHERE s.process_after_market_news = false
  AND EXISTS (
    SELECT 1
    FROM execution_outbox o
    WHERE o.aggregate_id = s.strategy_id
      AND o.event_type = 'STRATEGY_CREATED'
      AND (o.payload ->> 'process_after_market_news') = 'true'
  );
