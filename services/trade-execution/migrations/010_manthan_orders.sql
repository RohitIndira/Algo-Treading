-- Migration 010: Manthan order execution tracking
--
-- Tracks every order placed with the broker for Manthan strategy.
-- Links to manthan_positions (in rules-engine DB via strategy_id + symbol).
-- State machine: PENDING → PLACED → FILLED / PARTIAL / REJECTED / CANCELLED

CREATE TABLE IF NOT EXISTS manthan_orders (
    id               BIGSERIAL PRIMARY KEY,
    signal_id        VARCHAR(100) UNIQUE,          -- idempotency key (from rules-engine)
    strategy_id      UUID NOT NULL,
    user_id          VARCHAR(255) NOT NULL,
    symbol           VARCHAR(30) NOT NULL,
    isin             VARCHAR(12),
    exchange         VARCHAR(3) DEFAULT 'NSE',

    -- Order classification
    order_type       VARCHAR(20) NOT NULL,          -- LIMIT_BUY, SL_SELL, MARKET_SELL, AMO_SELL, SL_MODIFY
    order_side       VARCHAR(10) NOT NULL,          -- BUY, SELL
    product_type     VARCHAR(20) DEFAULT 'CNC',     -- CNC = delivery

    -- Pricing
    qty              INT NOT NULL,
    filled_qty       INT DEFAULT 0,
    limit_price      NUMERIC(12,2),
    trigger_price    NUMERIC(12,2),                 -- SL trigger
    avg_fill_price   NUMERIC(12,2),                 -- actual fill from broker

    -- Broker tracking
    broker_order_id  VARCHAR(50),                   -- Indira order ID
    broker_status    VARCHAR(50),                   -- raw broker status string
    indira_symbol    VARCHAR(100),                  -- e.g., "STK_GALLANTT_EQ_NSE_13337"
    exchange_token   VARCHAR(20),                   -- e.g., "13337"

    -- State machine
    status           VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- PENDING, PLACED, FILLED, PARTIAL, REJECTED, CANCELLED, EXPIRED
    -- SL specific: SL_PLACED, SL_TRIGGERED, SL_FILLED, SL_MODIFY_PENDING

    retry_count      INT DEFAULT 0,
    max_retries      INT DEFAULT 3,
    last_error       TEXT,

    -- Linked to entry (for SL orders)
    parent_order_id  BIGINT REFERENCES manthan_orders(id),

    -- Timestamps
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    placed_at        TIMESTAMPTZ,
    filled_at        TIMESTAMPTZ,
    cancelled_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mord_strategy   ON manthan_orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_mord_user       ON manthan_orders(user_id);
CREATE INDEX IF NOT EXISTS idx_mord_symbol     ON manthan_orders(symbol, status);
CREATE INDEX IF NOT EXISTS idx_mord_broker_id  ON manthan_orders(broker_order_id) WHERE broker_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mord_status     ON manthan_orders(status) WHERE status NOT IN ('FILLED', 'CANCELLED', 'REJECTED');
CREATE INDEX IF NOT EXISTS idx_mord_signal     ON manthan_orders(signal_id) WHERE signal_id IS NOT NULL;

-- Execution events audit trail (every state change)
CREATE TABLE IF NOT EXISTS manthan_order_events (
    id               BIGSERIAL PRIMARY KEY,
    order_id         BIGINT NOT NULL REFERENCES manthan_orders(id),
    event_type       VARCHAR(30) NOT NULL,          -- PLACED, FILL, PARTIAL_FILL, REJECTED, CANCELLED, MODIFIED, EMERGENCY_SELL
    old_status       VARCHAR(20),
    new_status       VARCHAR(20),
    broker_status    VARCHAR(50),
    price            NUMERIC(12,2),
    qty              INT,
    detail           TEXT,                           -- extra context (error msg, etc.)
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mord_events_order ON manthan_order_events(order_id);
