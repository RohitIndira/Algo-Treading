-- Migration 003: publish-durability flag for broker_events
--
-- Purpose:
--   The inline Kafka publish in the WSS listener / reconciler is best-effort —
--   if the broker's Kafka is briefly unreachable the row is safely in
--   broker_events but the order.events fan-out is silently lost, with nothing
--   to re-publish it. This column lets a background worker (publisher.RunWorker)
--   find rows that never made it to Kafka and republish them in id order.
--
--     published_at IS NULL  → not yet on order.events; worker will publish it
--     published_at = <ts>   → confirmed produced to Kafka (inline or by worker)
--
-- Ordering:
--   The worker scans WHERE published_at IS NULL ORDER BY id ASC. Because ids are
--   assigned in insert order, publishing in id order preserves per-broker_order_id
--   FIFO (the same ordering the Hash partitioner guarantees on the wire).
--
-- CRITICAL backfill:
--   ADD COLUMN leaves every EXISTING row with published_at = NULL. Without the
--   backfill below, the worker's first pass would treat the ENTIRE historical
--   broker_events table as "unpublished" and re-emit every past event to Kafka —
--   the exact mass re-publish that corrupted positions_db on 2026-07-17. We
--   stamp existing rows with their observed_at so history stays out of Kafka,
--   consistent with the reconciler boot-cutoff philosophy (history is NOT
--   re-emitted). Only rows inserted AFTER this migration participate in the
--   durability retry.

BEGIN;

ALTER TABLE broker_events
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

-- One-time backfill: treat all pre-existing rows as already published so the
-- worker never re-emits history. Safe to re-run (idempotent — only touches
-- rows still NULL, and after the first run there are none from before now).
UPDATE broker_events
   SET published_at = observed_at
 WHERE published_at IS NULL;

-- Hot path for the worker: find the oldest unpublished rows fast.
CREATE INDEX IF NOT EXISTS idx_broker_events_unpublished
    ON broker_events (id)
    WHERE published_at IS NULL;

COMMENT ON COLUMN broker_events.published_at IS
    'When this row was confirmed produced to order.events Kafka (inline or by publisher.RunWorker). NULL = pending publish; the worker retries NULL rows in id order.';

COMMIT;
