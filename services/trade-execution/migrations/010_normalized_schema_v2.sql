-- ============================================================================
-- Migration 010: Normalized Schema V2
-- Replaces the 40+ column orders god-table with a proper event-sourced design
-- Incorporates: partitioning, idempotency, signal_metrics, position tracking
-- ============================================================================

-- ============================================================================
-- 1. INSTRUMENTS (Master data for stocks/instruments)
-- ============================================================================
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id   SERIAL          PRIMARY KEY,
    stock_code      BIGINT          NOT NULL,
    symbol          VARCHAR(50)     NOT NULL,
    exchange        VARCHAR(10)     NOT NULL,   -- NSE / BSE
    isin            VARCHAR(20),
    company_name    VARCHAR(200),
    instrument_type VARCHAR(10)     NOT NULL DEFAULT 'STK', -- STK / OPT / FUT / IDX
    exchange_token  VARCHAR(50),                -- excToken for Codifi API
    lot_size        INT             NOT NULL DEFAULT 1,
    tick_size       DECIMAL(10,4),
    series          VARCHAR(10),                -- EQ / BE etc.
    codifi_symbol   VARCHAR(100),               -- Full Codifi format: STK_TCS_EQ_NSE_11536
    is_active       BOOLEAN         NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_instruments_stock_exchange UNIQUE (stock_code, exchange)
);

CREATE INDEX IF NOT EXISTS idx_instruments_symbol ON instruments(symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_stock_code ON instruments(stock_code);
CREATE INDEX IF NOT EXISTS idx_instruments_codifi ON instruments(codifi_symbol) WHERE codifi_symbol IS NOT NULL;

-- ============================================================================
-- 2. BROKER_ACCOUNTS (One row per user, replaces per-order bearer_token)
-- ============================================================================
CREATE TABLE IF NOT EXISTS broker_accounts (
    account_id       SERIAL         PRIMARY KEY,
    user_id          VARCHAR(50)    NOT NULL,
    broker_name      VARCHAR(20)    NOT NULL DEFAULT 'INDIRA',  -- INDIRA (Codifi)
    broker_user_id   VARCHAR(100),              -- Indira user ID
    app_id           VARCHAR(100),              -- Application ID from frontend
    source           VARCHAR(20)    NOT NULL DEFAULT 'WEB', -- IOS / AND / WEB
    bearer_token     TEXT,                      -- Encrypted JWT bearer token
    is_active        BOOLEAN        NOT NULL DEFAULT true,
    token_updated_at TIMESTAMPTZ,               -- When token was last refreshed
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_broker_accounts_user_broker UNIQUE (user_id, broker_name)
);

CREATE INDEX IF NOT EXISTS idx_broker_accounts_user ON broker_accounts(user_id);

-- ============================================================================
-- 3. ORDERS V2 (Lean ~25 columns, down from 40+)
-- ============================================================================
CREATE TABLE IF NOT EXISTS orders_v2 (
    order_id        UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    instrument_id   INT             REFERENCES instruments(instrument_id),
    account_id      INT             REFERENCES broker_accounts(account_id) ON DELETE SET NULL,

    -- Identity
    user_id         VARCHAR(50)     NOT NULL,
    strategy_id     VARCHAR(50)     NOT NULL,
    strategy_name   VARCHAR(255)    NOT NULL DEFAULT '',
    event_id        UUID            NOT NULL,       -- News event that triggered
    signal_id       UUID,                           -- Kafka dedup

    -- Order Specification
    order_type      VARCHAR(10)     NOT NULL,       -- MARKET / LIMIT / STOP_LOSS / SL-M
    order_side      VARCHAR(10)     NOT NULL,       -- BUY / SELL
    quantity        INT             NOT NULL,
    price           DECIMAL(15,2),                  -- NULL for MARKET
    validity        VARCHAR(10)     NOT NULL DEFAULT 'DAY', -- DAY / IOC
    product_type    VARCHAR(20)     NOT NULL DEFAULT 'INTRADAY',
    trading_mode    VARCHAR(10)     NOT NULL DEFAULT 'LIVE', -- PAPER / LIVE

    -- Current State (denormalized for fast reads, updated from fills)
    status          VARCHAR(20)     NOT NULL DEFAULT 'RECEIVED',
    filled_qty      INT             NOT NULL DEFAULT 0,
    avg_fill_price  DECIMAL(15,2),

    -- Risk
    risk_approved   BOOLEAN         NOT NULL DEFAULT false,
    risk_score      DECIMAL(5,2),

    -- Timestamps
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    submitted_at    TIMESTAMPTZ,
    executed_at     TIMESTAMPTZ,

    -- Error
    error_message   TEXT,
    retry_count     INT             NOT NULL DEFAULT 0,

    -- Constraints
    CONSTRAINT chk_orders_v2_qty_positive CHECK (quantity > 0),
    CONSTRAINT chk_orders_v2_price_positive CHECK (price IS NULL OR price > 0)
);

-- Signal dedup
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_v2_signal_unique
    ON orders_v2 (signal_id) WHERE signal_id IS NOT NULL;

-- Core query indexes
CREATE INDEX IF NOT EXISTS idx_orders_v2_user_time ON orders_v2(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_v2_status ON orders_v2(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_v2_strategy ON orders_v2(strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_orders_v2_event ON orders_v2(event_id);
CREATE INDEX IF NOT EXISTS idx_orders_v2_instrument ON orders_v2(instrument_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_v2_trading_mode ON orders_v2(trading_mode, status, created_at DESC);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_v2_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_v2_updated_at
    BEFORE UPDATE ON orders_v2
    FOR EACH ROW EXECUTE FUNCTION update_v2_updated_at();

-- ============================================================================
-- 4. ORDER_STATUS_HISTORY (Append-only ledger, PARTITIONED by month)
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_status_history (
    id              BIGSERIAL,
    order_id        UUID            NOT NULL,
    from_status     VARCHAR(20),                    -- NULL on first entry
    to_status       VARCHAR(20)     NOT NULL,
    source          VARCHAR(20)     NOT NULL,       -- SYSTEM / BROKER / USER / SCHEDULER / STRATEGY
    reason          TEXT,                            -- Human-readable why
    broker_raw_data JSONB,                          -- Full WS payload when source=BROKER
    actor           VARCHAR(50),                    -- Who triggered (user_id or system name)
    dedup_key       VARCHAR(128),                   -- Optional hash for idempotency
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Create partitions for current and next 3 months
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date := start_date + '1 month'::interval;
        partition_name := 'order_status_history_' || to_char(start_date, 'YYYY_MM');

        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF order_status_history
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_date, end_date
            );
        END IF;
    END LOOP;
END $$;

-- Default partition for anything outside defined ranges
CREATE TABLE IF NOT EXISTS order_status_history_default
    PARTITION OF order_status_history DEFAULT;

CREATE INDEX IF NOT EXISTS idx_osh_order_time ON order_status_history(order_id, created_at);
CREATE INDEX IF NOT EXISTS idx_osh_status_time ON order_status_history(to_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_osh_source ON order_status_history(source, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_osh_dedup ON order_status_history(dedup_key, created_at)
    WHERE dedup_key IS NOT NULL;

-- ============================================================================
-- 5. FILLS (One row per partial fill, PARTITIONED by month)
-- ============================================================================
CREATE TABLE IF NOT EXISTS fills (
    fill_id             UUID        NOT NULL DEFAULT gen_random_uuid(),
    order_id            UUID        NOT NULL,
    fill_qty            INT         NOT NULL,
    fill_price          DECIMAL(15,2) NOT NULL,     -- After DecimalLocator adjustment
    exchange_trade_no   VARCHAR(50),                -- Exchange trade ID (dedup partials)
    exchange_order_no   VARCHAR(50),                -- OrderNumber from broker WS
    broker_order_id     VARCHAR(50),                -- Codifi orderId / UniqueCode
    filled_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (fill_id, filled_at),

    CONSTRAINT chk_fills_qty CHECK (fill_qty > 0),
    CONSTRAINT chk_fills_price CHECK (fill_price > 0)
) PARTITION BY RANGE (filled_at);

-- Create partitions
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date := start_date + '1 month'::interval;
        partition_name := 'fills_' || to_char(start_date, 'YYYY_MM');

        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF fills
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_date, end_date
            );
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS fills_default PARTITION OF fills DEFAULT;

CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id, filled_at);
CREATE INDEX IF NOT EXISTS idx_fills_broker ON fills(broker_order_id) WHERE broker_order_id IS NOT NULL;
-- Composite unique for idempotency: same broker order + same exchange trade = duplicate
CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_dedup
    ON fills(broker_order_id, exchange_trade_no, filled_at)
    WHERE broker_order_id IS NOT NULL AND exchange_trade_no IS NOT NULL;

-- ============================================================================
-- 6. BROKER_REQUESTS (Every Codifi API call logged, PARTITIONED by month)
-- ============================================================================
CREATE TABLE IF NOT EXISTS broker_requests (
    id              BIGSERIAL,
    order_id        UUID            NOT NULL,
    action          VARCHAR(10)     NOT NULL,       -- PLACE / MODIFY / CANCEL
    broker_name     VARCHAR(20)     NOT NULL DEFAULT 'INDIRA',
    attempt         INT             NOT NULL DEFAULT 0,

    -- Codifi Request
    request_url     VARCHAR(255),
    request_body    JSONB,

    -- Codifi Response
    http_status     INT,
    response_body   JSONB,                          -- {orderId, infoID, infoMsg, ...}
    broker_order_id VARCHAR(50),                    -- orderId from response

    -- Diagnostics
    latency_ms      INT,
    error_type      VARCHAR(20),                    -- TIMEOUT / AUTH / BUSINESS / NETWORK
    error_message   TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date := start_date + '1 month'::interval;
        partition_name := 'broker_requests_' || to_char(start_date, 'YYYY_MM');

        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF broker_requests
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_date, end_date
            );
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS broker_requests_default PARTITION OF broker_requests DEFAULT;

CREATE INDEX IF NOT EXISTS idx_br_order ON broker_requests(order_id, created_at);
CREATE INDEX IF NOT EXISTS idx_br_error ON broker_requests(error_type, created_at DESC) WHERE error_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_br_broker_ord ON broker_requests(broker_order_id) WHERE broker_order_id IS NOT NULL;

-- ============================================================================
-- 7. STOP_LOSS_CONFIG (1:1 with order, separated from orders table)
-- ============================================================================
CREATE TABLE IF NOT EXISTS stop_loss_config (
    id                  SERIAL          PRIMARY KEY,
    order_id            UUID            NOT NULL UNIQUE,
    sl_type             VARCHAR(10)     NOT NULL DEFAULT 'FIXED',   -- FIXED / TRAILING
    stop_loss_price     DECIMAL(15,2),
    take_profit_price   DECIMAL(15,2),
    stop_loss_pct       DECIMAL(10,4),      -- Original SL % from strategy
    take_profit_pct     DECIMAL(10,4),      -- Original TP % from strategy
    trailing_pct        DECIMAL(10,4),      -- Trailing SL %
    max_monitor_price   DECIMAL(15,2),      -- Ceiling for price monitor
    highest_price       DECIMAL(15,2),      -- Tracked by price monitor (trailing)
    is_active           BOOLEAN         NOT NULL DEFAULT true,
    triggered_at        TIMESTAMPTZ,        -- NULL until SL/TP triggers
    trigger_reason      VARCHAR(20),        -- SL_HIT / TP_HIT / PRICE_EXCEEDED
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_slc_active ON stop_loss_config(is_active, sl_type) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_slc_order ON stop_loss_config(order_id);

-- ============================================================================
-- 8. ORDER_GROUPS (OCO / Bracket / Square-off group tracking)
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_groups (
    group_id    UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    group_type  VARCHAR(20)     NOT NULL,       -- OCO / BRACKET / SQUARE_OFF
    user_id     VARCHAR(50)     NOT NULL,
    status      VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE', -- ACTIVE / COMPLETED / CANCELLED
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_og_status ON order_groups(status) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_og_user ON order_groups(user_id, status);

-- ============================================================================
-- 9. ORDER_GROUP_LEGS (Maps orders to group legs)
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_group_legs (
    id          SERIAL          PRIMARY KEY,
    group_id    UUID            NOT NULL REFERENCES order_groups(group_id) ON DELETE CASCADE,
    order_id    UUID            NOT NULL,
    leg_role    VARCHAR(20)     NOT NULL,       -- ENTRY / SL_LEG / TP_LEG
    sequence    INT             NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_ogl_group_order UNIQUE (group_id, order_id)
);

CREATE INDEX IF NOT EXISTS idx_ogl_order ON order_group_legs(order_id);
CREATE INDEX IF NOT EXISTS idx_ogl_group ON order_group_legs(group_id);

-- ============================================================================
-- 10. POSITIONS (Aggregated position tracking)
--     NO entry_order_id / exit_order_id — use position_fills as source of truth
-- ============================================================================
CREATE TABLE IF NOT EXISTS positions (
    position_id     UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    instrument_id   INT             NOT NULL REFERENCES instruments(instrument_id),
    user_id         VARCHAR(50)     NOT NULL,
    strategy_id     VARCHAR(50),
    strategy_name   VARCHAR(255),
    trading_mode    VARCHAR(10)     NOT NULL,       -- PAPER / LIVE
    side            VARCHAR(10)     NOT NULL,       -- LONG / SHORT

    -- Quantities & prices (derived from position_fills → fills)
    open_qty        INT             NOT NULL DEFAULT 0,
    avg_entry_price DECIMAL(15,2),
    avg_exit_price  DECIMAL(15,2),

    -- P&L
    realized_pnl    DECIMAL(15,2),
    total_charges   DECIMAL(10,2)   NOT NULL DEFAULT 0,
    net_pnl         DECIMAL(15,2),

    -- Exit info
    exit_reason     VARCHAR(20),        -- SL_HIT / TP_HIT / MANUAL / SQUARE_OFF /
                                        -- STRATEGY_DELETED / PRICE_EXCEEDED / BROKER_CANCELLED

    -- Status
    status          VARCHAR(10)     NOT NULL DEFAULT 'OPEN',  -- OPEN / CLOSED
    opened_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pos_user_status ON positions(user_id, status, trading_mode);
CREATE INDEX IF NOT EXISTS idx_pos_strategy ON positions(strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_pos_instrument ON positions(instrument_id, status);
CREATE INDEX IF NOT EXISTS idx_pos_open ON positions(status, trading_mode) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS idx_pos_closed_date ON positions(user_id, closed_at DESC) WHERE status = 'CLOSED';

CREATE TRIGGER trg_positions_updated_at
    BEFORE UPDATE ON positions
    FOR EACH ROW EXECUTE FUNCTION update_v2_updated_at();

-- ============================================================================
-- 11. POSITION_FILLS (Links fills ↔ positions, source of truth)
-- ============================================================================
CREATE TABLE IF NOT EXISTS position_fills (
    id              SERIAL          PRIMARY KEY,
    position_id     UUID            NOT NULL REFERENCES positions(position_id) ON DELETE CASCADE,
    fill_id         UUID            NOT NULL,       -- → fills(fill_id) (no FK due to partitioning)
    order_id        UUID            NOT NULL,       -- → orders_v2(order_id) for fast lookup
    direction       VARCHAR(5)      NOT NULL,       -- ENTRY / EXIT
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pf_position_fill UNIQUE (position_id, fill_id)
);

CREATE INDEX IF NOT EXISTS idx_pf_position ON position_fills(position_id);
CREATE INDEX IF NOT EXISTS idx_pf_fill ON position_fills(fill_id);
CREATE INDEX IF NOT EXISTS idx_pf_order ON position_fills(order_id);

-- ============================================================================
-- 12. DAILY_PNL_SUMMARY (Precomputed dashboard stats)
-- ============================================================================
CREATE TABLE IF NOT EXISTS daily_pnl_summary (
    id              SERIAL          PRIMARY KEY,
    user_id         VARCHAR(50)     NOT NULL,
    strategy_id     VARCHAR(50),                    -- NULL = all strategies
    trading_mode    VARCHAR(10)     NOT NULL,       -- PAPER / LIVE
    trade_date      DATE            NOT NULL,

    -- Stats
    total_trades    INT             NOT NULL DEFAULT 0,
    winning_trades  INT             NOT NULL DEFAULT 0,
    losing_trades   INT             NOT NULL DEFAULT 0,
    total_invested  DECIMAL(15,2)   NOT NULL DEFAULT 0,
    gross_pnl       DECIMAL(15,2)   NOT NULL DEFAULT 0,
    total_charges   DECIMAL(10,2)   NOT NULL DEFAULT 0,
    net_pnl         DECIMAL(15,2)   NOT NULL DEFAULT 0,
    max_drawdown    DECIMAL(15,2),
    win_rate        DECIMAL(5,2),                   -- (winners / total) * 100

    CONSTRAINT uq_dpnl UNIQUE (user_id, strategy_id, trade_date, trading_mode)
);

CREATE INDEX IF NOT EXISTS idx_dpnl_user_date ON daily_pnl_summary(user_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_dpnl_strategy ON daily_pnl_summary(strategy_id, trade_date DESC);

-- ============================================================================
-- 13. SIGNAL_METRICS (End-to-end latency tracking across all services)
--     PARTITIONED by month for fast growth
-- ============================================================================
CREATE TABLE IF NOT EXISTS signal_metrics (
    id                          BIGSERIAL,
    signal_id                   UUID            NOT NULL,
    order_id                    UUID,
    user_id                     VARCHAR(50)     NOT NULL,
    strategy_id                 VARCHAR(50),
    symbol                      VARCHAR(50),
    exchange                    VARCHAR(10),
    trading_mode                VARCHAR(10),        -- PAPER / LIVE
    final_status                VARCHAR(20),        -- FILLED / REJECTED / CANCELLED / FAILED

    -- Timestamps at each pipeline stage
    signal_generated_at         TIMESTAMPTZ,        -- Rules-engine: signal created
    signal_published_at         TIMESTAMPTZ,        -- Rules-engine: published to Kafka
    signal_consumed_at          TIMESTAMPTZ,        -- Trade-execution: consumed from Kafka
    risk_check_started_at       TIMESTAMPTZ,        -- Risk check start
    risk_check_completed_at     TIMESTAMPTZ,        -- Risk check done
    order_created_at            TIMESTAMPTZ,        -- Order row inserted in DB
    broker_request_sent_at      TIMESTAMPTZ,        -- Codifi API request sent
    broker_response_received_at TIMESTAMPTZ,        -- Codifi API response received
    broker_ws_first_update_at   TIMESTAMPTZ,        -- First WS status update
    broker_ws_filled_at         TIMESTAMPTZ,        -- WS EXECUTED/TRADED received
    position_opened_at          TIMESTAMPTZ,        -- Position record created

    -- Computed latencies (milliseconds)
    kafka_latency_ms            INT,                -- signal_published → signal_consumed
    processing_latency_ms       INT,                -- signal_consumed → broker_request_sent
    broker_api_latency_ms       INT,                -- broker_request_sent → broker_response_received
    broker_fill_latency_ms      INT,                -- broker_response → broker_ws_filled
    total_e2e_ms                INT,                -- signal_generated → broker_ws_filled

    -- Metadata
    broker_api_retries          INT             NOT NULL DEFAULT 0,
    error_type                  VARCHAR(20),        -- TIMEOUT / AUTH / BUSINESS / RISK_REJECTED
    error_message               TEXT,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date := start_date + '1 month'::interval;
        partition_name := 'signal_metrics_' || to_char(start_date, 'YYYY_MM');

        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF signal_metrics
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_date, end_date
            );
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS signal_metrics_default PARTITION OF signal_metrics DEFAULT;

CREATE INDEX IF NOT EXISTS idx_sm_signal ON signal_metrics(signal_id, created_at);
CREATE INDEX IF NOT EXISTS idx_sm_order ON signal_metrics(order_id, created_at) WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sm_user ON signal_metrics(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sm_strategy ON signal_metrics(strategy_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sm_latency ON signal_metrics(total_e2e_ms, created_at DESC) WHERE total_e2e_ms IS NOT NULL;

-- ============================================================================
-- PARTITION MAINTENANCE: Create partitions for future months
-- Run this monthly via pg_cron or external scheduler:
--
--   SELECT create_monthly_partitions();
-- ============================================================================
CREATE OR REPLACE FUNCTION create_monthly_partitions()
RETURNS void AS $$
DECLARE
    tables TEXT[] := ARRAY['order_status_history', 'fills', 'broker_requests', 'signal_metrics'];
    tbl TEXT;
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOREACH tbl IN ARRAY tables LOOP
        -- Create next 3 months of partitions
        FOR i IN 0..3 LOOP
            start_date := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
            end_date := start_date + '1 month'::interval;
            partition_name := tbl || '_' || to_char(start_date, 'YYYY_MM');

            IF NOT EXISTS (
                SELECT 1 FROM pg_class WHERE relname = partition_name
            ) THEN
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                    partition_name, tbl, start_date, end_date
                );
                RAISE NOTICE 'Created partition: %', partition_name;
            END IF;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS
-- ============================================================================
COMMENT ON TABLE instruments IS 'Stock/instrument master with Codifi symbol format';
COMMENT ON TABLE broker_accounts IS 'Encrypted Codifi auth per user (replaces per-order tokens)';
COMMENT ON TABLE orders_v2 IS 'Lean order record (~25 cols). Current state only, history in order_status_history';
COMMENT ON TABLE order_status_history IS 'Append-only ledger: every status change with reason + raw WS data. Partitioned monthly.';
COMMENT ON TABLE fills IS 'Individual partial fills with exact prices. Partitioned monthly.';
COMMENT ON TABLE broker_requests IS 'Every Codifi API call logged with request/response/latency. Partitioned monthly.';
COMMENT ON TABLE stop_loss_config IS 'SL/TP/trailing config separated from order. 1:1 with orders_v2.';
COMMENT ON TABLE order_groups IS 'OCO/Bracket/Square-off group-level tracking';
COMMENT ON TABLE order_group_legs IS 'Maps orders to group legs (ENTRY/SL_LEG/TP_LEG)';
COMMENT ON TABLE positions IS 'Aggregated position with entry/exit/PnL. Derived from position_fills.';
COMMENT ON TABLE position_fills IS 'Links fills to positions. Source of truth for entry vs exit fills.';
COMMENT ON TABLE daily_pnl_summary IS 'Precomputed daily P&L stats for dashboard. Updated on position close.';
COMMENT ON TABLE signal_metrics IS 'End-to-end latency tracking from signal generation to fill. Partitioned monthly.';
