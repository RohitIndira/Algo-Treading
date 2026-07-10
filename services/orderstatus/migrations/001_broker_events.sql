-- Migration 001: broker_events (source-of-truth event log for orderstatus svc)
--
-- Purpose:
--   Every observation of broker order state — from WSS (real-time), REST
--   orderbook polling (15s drift check), or REST reconciliation (5min) —
--   lands here as an immutable row. This is the event log the whole
--   orderstatus svc is built around.
--
-- Downstream:
--   The orderstatus Kafka publisher publishes `order.events` after every
--   successful INSERT here, so positions svc / trade-execution / api-gateway
--   can subscribe. First writer wins on (broker_order_id, event_seq).
--
-- Boot recovery:
--   On startup, orderstatus svc queries this table for the max event_seq
--   per broker_order_id and calls broker.GetOrderBook to catch up on any
--   state changes that happened while it was down. Then resumes WSS.
--
-- Design:
--   Append-only. No UPDATEs. Idempotent via UNIQUE(broker_order_id, event_seq).

BEGIN;

CREATE TABLE IF NOT EXISTS broker_events (
    id                BIGSERIAL PRIMARY KEY,

    broker_order_id   VARCHAR(50)  NOT NULL,   -- Codify UniqueCode / nstOrdNo
    exchange_order_id VARCHAR(50),             -- NSE order number (may be "0" pre-exchange)
    event_seq         BIGINT       NOT NULL,   -- broker exch time in microseconds
                                               -- deterministic across WSS/REST paths

    source            VARCHAR(20)  NOT NULL,
        -- WSS | REST_ORDERBOOK | REST_RECONCILER | REST_TRADEBOOK
    event_type        VARCHAR(32)  NOT NULL,
        -- PLACED / STATUS_CHANGED / FILLED / PARTIALLY_FILLED /
        -- CANCELLED / REJECTED / MODIFIED / TRIGGERED / EXPIRED
    status            VARCHAR(32),             -- verified enum (PENDING, EXECUTED, CANCELLED, ...)
    oms_status_code   INT,                     -- Codify's OMSOrderStatus (8, 10, 15, ...)

    -- Business context (denormalized from WSS/REST payload for query speed)
    user_id           VARCHAR(64)  NOT NULL,
    symbol            VARCHAR(32),
    exchange          VARCHAR(8),
    buy_sell          CHAR(1),                  -- '1'=BUY '2'=SELL
    order_type        VARCHAR(32),              -- REGULAR LIMIT, REGULAR MARKET, ...
    product           VARCHAR(32),

    order_price       NUMERIC(14,4),
    trigger_price     NUMERIC(14,4),
    quantity          INTEGER,
    filled_qty        INTEGER,
    traded_price      NUMERIC(14,4),
    pending_qty       INTEGER,
    reason            TEXT,

    -- Full raw payload for forensics (WSS event JSON or REST orderbook row)
    raw_payload       JSONB        NOT NULL,

    broker_ts_ms      BIGINT,                    -- broker's timestamp (may be 0 for REST paths)
    observed_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Idempotency: same broker event arriving twice (WSS + REST poll race)
    -- becomes a no-op on ON CONFLICT DO NOTHING.
    CONSTRAINT uq_broker_events_seq UNIQUE (broker_order_id, event_seq),

    CONSTRAINT chk_broker_events_source CHECK (source IN (
        'WSS', 'REST_ORDERBOOK', 'REST_RECONCILER', 'REST_TRADEBOOK'
    ))
);

-- "Show me every event for this order" — the most common query
CREATE INDEX IF NOT EXISTS idx_broker_events_order
    ON broker_events (broker_order_id, event_seq DESC);

-- "Show me recent events for this user" — audit / support queries
CREATE INDEX IF NOT EXISTS idx_broker_events_user_time
    ON broker_events (user_id, observed_at DESC);

-- "Recent fills" — publisher hot path (may drop later if unused)
CREATE INDEX IF NOT EXISTS idx_broker_events_fills
    ON broker_events (observed_at DESC)
    WHERE event_type IN ('FILLED', 'PARTIALLY_FILLED');

COMMENT ON TABLE  broker_events IS
    'Append-only event log — every broker state observation lands here first. See services/orderstatus/README.md for the full design.';
COMMENT ON COLUMN broker_events.event_seq IS
    'Broker exchange timestamp in microseconds. Deterministic across WSS + REST paths — same broker event → same event_seq → dedup via UNIQUE constraint.';
COMMENT ON COLUMN broker_events.source IS
    'Which observation path saw this event first. WSS = real-time push; REST_ORDERBOOK = 15s drift check; REST_RECONCILER = 5min full sweep; REST_TRADEBOOK = fill audit lookup.';

COMMIT;

-- Verification:
--   \d broker_events
--   -- expect: PK id, UNIQUE (broker_order_id, event_seq), 3 indexes
