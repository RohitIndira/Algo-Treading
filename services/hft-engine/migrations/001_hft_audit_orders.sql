-- 001_hft_audit_orders.sql
-- Audit log for every Place / Modify / Cancel / Fill that the HFT engine
-- emits or observes. Lives in the `execution_db` database (same DB as
-- manthan_orders) so the dashboard can join in a single query.
--
-- Writes are append-only and async (batched every 1s by audit.Writer) — the
-- hot tick loop never blocks on DB. Reads are infrequent (post-mortem,
-- dashboard). We do NOT use this table to reconstruct state on restart —
-- broker order-book + trade-book are authoritative for that.

CREATE TABLE IF NOT EXISTS hft_audit_orders (
    id              BIGSERIAL    PRIMARY KEY,
    strategy_id     UUID         NOT NULL,
    user_id         VARCHAR(50)  NOT NULL,
    symbol          VARCHAR(30)  NOT NULL,

    side            CHAR(1)      NOT NULL,             -- 'B' (buy) or 'S' (sell)
    action          VARCHAR(20)  NOT NULL,             -- PLACE | MODIFY | CANCEL | FILL | REJECT | PLACE_STUB | CANCEL_STUB
    chunk_seq       INT,                                -- which chunk on this side (1, 2, 3, ...)

    qty             INT          NOT NULL,             -- order qty for PLACE/MODIFY, fill qty for FILL
    price           NUMERIC(12,2),                     -- limit price; for FILL, the fill price
    broker_order_id VARCHAR(50),                       -- Indira order id (nullable on REJECT)
    broker_status   VARCHAR(20),                       -- broker's reply status if any
    error_msg       TEXT,                              -- populated on REJECT/CANCEL with reason

    mode            VARCHAR(10)  NOT NULL DEFAULT 'PAPER',  -- 'PAPER' | 'LIVE' — easy filter for live ops
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Per-strategy timeline lookup (dashboard "show me strategy X's last 100 events").
CREATE INDEX IF NOT EXISTS idx_hft_audit_strategy_time
    ON hft_audit_orders(strategy_id, created_at DESC);

-- Per-broker-order lookup (debugging: "what happened to NZWKE0001A<5?").
CREATE INDEX IF NOT EXISTS idx_hft_audit_broker_order
    ON hft_audit_orders(broker_order_id)
    WHERE broker_order_id IS NOT NULL;

-- Per-user lookup ordered by time (compliance: "all orders this user placed
-- recently"). The day filter is then a cheap WHERE on the timestamp column.
-- We avoid (created_at::date) here because Postgres treats it as STABLE not
-- IMMUTABLE under non-UTC timezone settings and refuses to index it.
CREATE INDEX IF NOT EXISTS idx_hft_audit_user_time
    ON hft_audit_orders(user_id, created_at DESC);
