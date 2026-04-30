-- Migration 012: Transactional inbox for at-least-once signal processing.
--
-- Why this exists
-- ───────────────
-- Before this migration the trade-execution signal_consumer used kafka-go's
-- `ReadMessage` which auto-commits the offset on the next call regardless
-- of whether the handler succeeded. During an AU004 (broker session expired)
-- storm a MANTHAN_SL_MODIFY for NATIONALUM was consumed, the handler failed
-- with `indira: session expired`, and the offset was committed anyway —
-- silently losing the trail update. Broker SL stayed at the previous trigger
-- while rules-engine internal state moved on; the divergence persisted until
-- the next price tick.
--
-- The fix is the standard transactional-inbox pattern (Stripe / Shopify /
-- Uber-grade): the consumer's only job is to durably persist the message,
-- then commit Kafka. A separate worker pool drains the inbox with bounded
-- backoff + DLQ. This gives us:
--
--   * at-least-once delivery (Kafka offset commits AFTER inbox INSERT)
--   * no head-of-line blocking (workers pick due rows; one stuck row
--     doesn't block others on the same partition)
--   * crash safety (mid-run kill leaves rows in RUNNING; lock expires and
--     another worker picks them up)
--   * SQL-observable retry queue + DLQ for paging
--   * idempotency via UNIQUE(signal_id) — Kafka redeliveries are no-ops
--
-- Status state machine:
--
--   PENDING ──► RUNNING ──► DONE
--                  │
--                  └──► FAILED ──► (next_attempt_at) ──► RUNNING ──► …
--                          │
--                          └──► DLQ  (attempts >= max OR poison)
--
-- Rollback:
--   DROP TABLE signal_inbox;

BEGIN;

CREATE TABLE IF NOT EXISTS signal_inbox (
    id               BIGSERIAL PRIMARY KEY,
    -- signal_id is the rules-engine's order_id (e.g. "slmod-NATIONALUM-ce001cfe").
    -- UNIQUE makes Kafka redelivery a no-op via ON CONFLICT DO NOTHING.
    signal_id        TEXT        NOT NULL UNIQUE,
    user_id          TEXT        NOT NULL,
    -- MANTHAN_ENTRY | MANTHAN_TOPUP | MANTHAN_SL_MODIFY | MANTHAN_SL_EXIT
    order_type       TEXT        NOT NULL,
    -- Full original Kafka message body — handlers parse this themselves so
    -- the inbox stays signal-shape-agnostic.
    payload          JSONB       NOT NULL,
    -- Kafka coordinates for traceability / replay tooling.
    kafka_topic      TEXT        NOT NULL,
    kafka_partition  INT         NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'PENDING',
    attempts         INT         NOT NULL DEFAULT 0,
    last_error       TEXT,
    -- Coarse classification used for backoff policy + DLQ decisions:
    --   AUTH_EXPIRED   → fast retry, 30s
    --   BROKER_REJECT  → exp backoff, capped 5 min
    --   POISON         → DLQ immediately (parse failure, no signal_id)
    --   TRANSIENT      → exp backoff
    last_error_class TEXT,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    CONSTRAINT signal_inbox_status_chk
        CHECK (status IN ('PENDING','RUNNING','DONE','FAILED','DLQ'))
);

-- Hot index: workers pull due rows ordered by next_attempt_at. Partial
-- index keeps it tiny — DONE rows (the bulk of the table over time) never
-- enter the index.
CREATE INDEX IF NOT EXISTS idx_signal_inbox_due
    ON signal_inbox (next_attempt_at)
    WHERE status IN ('PENDING','FAILED');

-- Used by the auth-restored fast-path worker wake: when a user re-logs in,
-- preferentially pull their pending rows.
CREATE INDEX IF NOT EXISTS idx_signal_inbox_user_pending
    ON signal_inbox (user_id)
    WHERE status IN ('PENDING','FAILED');

COMMENT ON TABLE  signal_inbox IS
    'Transactional inbox for trade-signals consumer. Consumer writes here + commits Kafka; worker pool drains with retry/DLQ. See migration 012 for design rationale.';
COMMENT ON COLUMN signal_inbox.last_error_class IS
    'Coarse error class for backoff policy: AUTH_EXPIRED | BROKER_REJECT | POISON | TRANSIENT.';

COMMIT;
