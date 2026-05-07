-- ============================================================================
-- Trade Execution Service — Initial Schema (Go-Live)
-- Single migration combining all prior incremental migrations.
-- Tables: instruments, broker_accounts, orders, order_status_history,
--         fills, broker_requests, stop_loss_config, order_groups,
--         order_group_legs, positions, position_fills, daily_pnl_summary,
--         signal_metrics, multi_level_exit_levels, user_square_off_config
-- ============================================================================

-- Shared trigger function for updated_at
CREATE OR REPLACE FUNCTION update_v2_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 1. INSTRUMENTS
-- ============================================================================
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id   SERIAL          PRIMARY KEY,
    stock_code      BIGINT          NOT NULL,
    symbol          VARCHAR(50)     NOT NULL,
    exchange        VARCHAR(10)     NOT NULL,
    isin            VARCHAR(20),
    company_name    VARCHAR(200),
    instrument_type VARCHAR(10)     NOT NULL DEFAULT 'STK',
    exchange_token  VARCHAR(50),
    lot_size        INT             NOT NULL DEFAULT 1,
    tick_size       DECIMAL(10,4),
    series          VARCHAR(10),
    codifi_symbol   VARCHAR(100),
    is_active       BOOLEAN         NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_instruments_stock_exchange UNIQUE (stock_code, exchange)
);

CREATE INDEX IF NOT EXISTS idx_instruments_symbol    ON instruments(symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_stock_code ON instruments(stock_code);
CREATE INDEX IF NOT EXISTS idx_instruments_codifi    ON instruments(codifi_symbol) WHERE codifi_symbol IS NOT NULL;

-- ============================================================================
-- 2. BROKER_ACCOUNTS
-- ============================================================================
CREATE TABLE IF NOT EXISTS broker_accounts (
    account_id       SERIAL         PRIMARY KEY,
    user_id          VARCHAR(50)    NOT NULL,
    broker_name      VARCHAR(20)    NOT NULL DEFAULT 'INDIRA',
    broker_user_id   VARCHAR(100),
    app_id           VARCHAR(100),
    source           VARCHAR(20)    NOT NULL DEFAULT 'WEB',
    bearer_token     TEXT,
    is_active        BOOLEAN        NOT NULL DEFAULT true,
    token_updated_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_broker_accounts_user_broker UNIQUE (user_id, broker_name)
);

CREATE INDEX IF NOT EXISTS idx_broker_accounts_user ON broker_accounts(user_id);

-- ============================================================================
-- 3. ORDERS
-- ============================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id               VARCHAR(50)     NOT NULL,
    strategy_id           VARCHAR(50)     NOT NULL,
    strategy_name         VARCHAR(255)    NOT NULL DEFAULT '',
    event_id              UUID            NOT NULL,
    signal_id             UUID,

    -- Instrument info (denormalised for query speed)
    stock_code            BIGINT          NOT NULL DEFAULT 0,
    exchange              VARCHAR(10)     NOT NULL DEFAULT '',
    symbol                VARCHAR(100)    NOT NULL DEFAULT '',

    -- Order details
    order_type            VARCHAR(20)     NOT NULL,
    order_side            VARCHAR(10)     NOT NULL,
    quantity              INT             NOT NULL,
    price                 DECIMAL(15,2),
    stop_loss             DECIMAL(15,2),
    take_profit           DECIMAL(15,2),
    target_price          DECIMAL(15,2),
    validity              VARCHAR(10)     NOT NULL DEFAULT 'DAY',
    product_type          VARCHAR(20)     NOT NULL DEFAULT 'INTRADAY',

    -- Status
    status                VARCHAR(20)     NOT NULL DEFAULT 'RECEIVED',
    rejection_reason      TEXT,
    error_message         TEXT,
    retry_count           INT             NOT NULL DEFAULT 0,

    -- Broker integration
    indira_order_id       VARCHAR(100),
    indira_response       TEXT,
    odin_order_id         VARCHAR(100),
    odin_response         TEXT,
    broker_status         VARCHAR(50),
    broker_ws_data        TEXT,
    exchange_order_number VARCHAR(100),

    -- Frontend auth (passed-through for broker calls)
    bearer_token          TEXT,
    app_id                VARCHAR(100),
    source                VARCHAR(20),

    -- Stop loss config
    stop_loss_type        VARCHAR(20),
    trailing_sl_pct       DECIMAL(10,4),
    highest_price         DECIMAL(15,2),

    -- Square-off flags
    is_square_off_order   BOOLEAN         NOT NULL DEFAULT false,
    auto_square_off_time  VARCHAR(5),

    -- Paper vs live
    is_paper_trade        BOOLEAN         NOT NULL DEFAULT false,
    trading_mode          VARCHAR(10)     NOT NULL DEFAULT 'LIVE',
    paper_exit_price      DECIMAL(15,2),
    paper_pnl             DECIMAL(15,2),

    -- Live exit
    live_exit_price       DECIMAL(15,2),
    live_pnl              DECIMAL(15,2),

    -- Price monitor helpers
    current_pct_change    DECIMAL(10,4)   NOT NULL DEFAULT 0,
    max_monitor_price     DECIMAL(15,2),

    -- OCO (One-Cancels-the-Other) group
    oco_group_id          UUID,
    oco_role              VARCHAR(20),
    parent_order_id       UUID,

    -- Execution details
    filled_quantity       INT             NOT NULL DEFAULT 0,
    filled_price          DECIMAL(15,2),
    commission            DECIMAL(15,2),
    total_cost            DECIMAL(15,2),

    -- Risk
    risk_approved         BOOLEAN         NOT NULL DEFAULT false,
    risk_score            DECIMAL(10,4),

    -- Timestamps
    created_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    submitted_at          TIMESTAMPTZ,
    executed_at           TIMESTAMPTZ,

    CONSTRAINT chk_orders_qty_positive   CHECK (quantity > 0),
    CONSTRAINT chk_orders_price_positive CHECK (price IS NULL OR price > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_signal_unique             ON orders(signal_id) WHERE signal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_user_time                        ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status                           ON orders(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_strategy                         ON orders(strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_orders_strategy_id                      ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_event                            ON orders(event_id);
CREATE INDEX IF NOT EXISTS idx_orders_stock_code                       ON orders(stock_code);
CREATE INDEX IF NOT EXISTS idx_orders_created_at                       ON orders(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_trading_mode                     ON orders(trading_mode, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_sq_off_time                      ON orders(auto_square_off_time, user_id) WHERE auto_square_off_time IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_oco_group                        ON orders(oco_group_id) WHERE oco_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_oco_active                       ON orders(oco_group_id, status) WHERE oco_group_id IS NOT NULL AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED', 'FAILED');
CREATE INDEX IF NOT EXISTS idx_orders_indira_id                        ON orders(indira_order_id) WHERE indira_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_product_type                     ON orders(product_type, status) WHERE is_paper_trade = false;
CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_number            ON orders(exchange_order_number) WHERE exchange_order_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_paper_trade                      ON orders(user_id, is_paper_trade, status) WHERE is_paper_trade = true;
CREATE INDEX IF NOT EXISTS idx_orders_paper_symbol                     ON orders(symbol, is_paper_trade, status) WHERE is_paper_trade = true;
CREATE INDEX IF NOT EXISTS idx_orders_paper_closed                     ON orders(user_id, is_paper_trade, paper_exit_price) WHERE is_paper_trade = true AND paper_exit_price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_paper_open                       ON orders(user_id, is_paper_trade, status) WHERE is_paper_trade = true AND status = 'FILLED';
CREATE INDEX IF NOT EXISTS idx_orders_live_closed                      ON orders(user_id, is_paper_trade, live_exit_price) WHERE is_paper_trade = false AND live_exit_price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_live_open                        ON orders(user_id, is_paper_trade, status) WHERE is_paper_trade = false AND status IN ('FILLED', 'PARTIALLY_FILLED');

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_v2_updated_at();

-- Execution events (order lifecycle log)
CREATE TABLE IF NOT EXISTS execution_events (
    id          SERIAL      PRIMARY KEY,
    order_id    UUID        NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type  VARCHAR(20) NOT NULL,
    event_data  JSONB,
    created_at  TIMESTAMP   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_events_order_id   ON execution_events(order_id);
CREATE INDEX IF NOT EXISTS idx_execution_events_created_at ON execution_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_events_type       ON execution_events(event_type);

-- ============================================================================
-- 4. ORDER_STATUS_HISTORY (partitioned by month)
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_status_history (
    id              BIGSERIAL,
    order_id        UUID            NOT NULL,
    from_status     VARCHAR(20),
    to_status       VARCHAR(20)     NOT NULL,
    source          VARCHAR(20)     NOT NULL,
    reason          TEXT,
    broker_raw_data JSONB,
    actor           VARCHAR(50),
    dedup_key       VARCHAR(128),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

DO $$
DECLARE
    start_date DATE; end_date DATE; partition_name TEXT; i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date     := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date       := start_date + '1 month'::interval;
        partition_name := 'order_status_history_' || to_char(start_date, 'YYYY_MM');
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF order_status_history FOR VALUES FROM (%L) TO (%L)',
                           partition_name, start_date, end_date);
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS order_status_history_default PARTITION OF order_status_history DEFAULT;

CREATE INDEX IF NOT EXISTS idx_osh_order_time ON order_status_history(order_id, created_at);
CREATE INDEX IF NOT EXISTS idx_osh_status_time ON order_status_history(to_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_osh_source ON order_status_history(source, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_osh_dedup ON order_status_history(dedup_key, created_at) WHERE dedup_key IS NOT NULL;

-- ============================================================================
-- 5. FILLS (partitioned by month)
-- ============================================================================
CREATE TABLE IF NOT EXISTS fills (
    fill_id             UUID        NOT NULL DEFAULT gen_random_uuid(),
    order_id            UUID        NOT NULL,
    fill_qty            INT         NOT NULL,
    fill_price          DECIMAL(15,2) NOT NULL,
    exchange_trade_no   VARCHAR(50),
    exchange_order_no   VARCHAR(50),
    broker_order_id     VARCHAR(50),
    filled_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (fill_id, filled_at),

    CONSTRAINT chk_fills_qty   CHECK (fill_qty > 0),
    CONSTRAINT chk_fills_price CHECK (fill_price > 0)
) PARTITION BY RANGE (filled_at);

DO $$
DECLARE
    start_date DATE; end_date DATE; partition_name TEXT; i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date     := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date       := start_date + '1 month'::interval;
        partition_name := 'fills_' || to_char(start_date, 'YYYY_MM');
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF fills FOR VALUES FROM (%L) TO (%L)',
                           partition_name, start_date, end_date);
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS fills_default PARTITION OF fills DEFAULT;

CREATE INDEX IF NOT EXISTS idx_fills_order  ON fills(order_id, filled_at);
CREATE INDEX IF NOT EXISTS idx_fills_broker ON fills(broker_order_id) WHERE broker_order_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_dedup
    ON fills(broker_order_id, exchange_trade_no, filled_at)
    WHERE broker_order_id IS NOT NULL AND exchange_trade_no IS NOT NULL;

-- ============================================================================
-- 6. BROKER_REQUESTS (partitioned by month)
-- ============================================================================
CREATE TABLE IF NOT EXISTS broker_requests (
    id              BIGSERIAL,
    order_id        UUID            NOT NULL,
    action          VARCHAR(10)     NOT NULL,
    broker_name     VARCHAR(20)     NOT NULL DEFAULT 'INDIRA',
    attempt         INT             NOT NULL DEFAULT 0,
    request_url     VARCHAR(255),
    request_body    JSONB,
    http_status     INT,
    response_body   JSONB,
    broker_order_id VARCHAR(50),
    latency_ms      INT,
    error_type      VARCHAR(20),
    error_message   TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

DO $$
DECLARE
    start_date DATE; end_date DATE; partition_name TEXT; i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date     := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date       := start_date + '1 month'::interval;
        partition_name := 'broker_requests_' || to_char(start_date, 'YYYY_MM');
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF broker_requests FOR VALUES FROM (%L) TO (%L)',
                           partition_name, start_date, end_date);
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS broker_requests_default PARTITION OF broker_requests DEFAULT;

CREATE INDEX IF NOT EXISTS idx_br_order     ON broker_requests(order_id, created_at);
CREATE INDEX IF NOT EXISTS idx_br_error     ON broker_requests(error_type, created_at DESC) WHERE error_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_br_broker_ord ON broker_requests(broker_order_id) WHERE broker_order_id IS NOT NULL;

-- ============================================================================
-- 7. STOP_LOSS_CONFIG
-- ============================================================================
CREATE TABLE IF NOT EXISTS stop_loss_config (
    id                  SERIAL          PRIMARY KEY,
    order_id            UUID            NOT NULL UNIQUE,
    sl_type             VARCHAR(10)     NOT NULL DEFAULT 'FIXED',
    stop_loss_price     DECIMAL(15,2),
    take_profit_price   DECIMAL(15,2),
    stop_loss_pct       DECIMAL(10,4),
    take_profit_pct     DECIMAL(10,4),
    trailing_pct        DECIMAL(10,4),
    max_monitor_price   DECIMAL(15,2),
    highest_price       DECIMAL(15,2),
    is_active           BOOLEAN         NOT NULL DEFAULT true,
    triggered_at        TIMESTAMPTZ,
    trigger_reason      VARCHAR(20),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_slc_active ON stop_loss_config(is_active, sl_type) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_slc_order  ON stop_loss_config(order_id);

-- ============================================================================
-- 8. ORDER_GROUPS
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_groups (
    group_id    UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    group_type  VARCHAR(20)     NOT NULL,
    user_id     VARCHAR(50)     NOT NULL,
    status      VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_og_status ON order_groups(status) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_og_user   ON order_groups(user_id, status);

-- ============================================================================
-- 9. ORDER_GROUP_LEGS
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_group_legs (
    id          SERIAL          PRIMARY KEY,
    group_id    UUID            NOT NULL REFERENCES order_groups(group_id) ON DELETE CASCADE,
    order_id    UUID            NOT NULL,
    leg_role    VARCHAR(20)     NOT NULL,
    sequence    INT             NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_ogl_group_order UNIQUE (group_id, order_id)
);

CREATE INDEX IF NOT EXISTS idx_ogl_order ON order_group_legs(order_id);
CREATE INDEX IF NOT EXISTS idx_ogl_group ON order_group_legs(group_id);

-- ============================================================================
-- 10. POSITIONS
-- ============================================================================
CREATE TABLE IF NOT EXISTS positions (
    position_id     UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    instrument_id   INT             NOT NULL REFERENCES instruments(instrument_id),
    user_id         VARCHAR(50)     NOT NULL,
    strategy_id     VARCHAR(50),
    strategy_name   VARCHAR(255),
    trading_mode    VARCHAR(10)     NOT NULL,
    side            VARCHAR(10)     NOT NULL,
    open_qty        INT             NOT NULL DEFAULT 0,
    avg_entry_price DECIMAL(15,2),
    avg_exit_price  DECIMAL(15,2),
    realized_pnl    DECIMAL(15,2),
    total_charges   DECIMAL(10,2)   NOT NULL DEFAULT 0,
    net_pnl         DECIMAL(15,2),
    exit_reason     VARCHAR(20),
    status          VARCHAR(10)     NOT NULL DEFAULT 'OPEN',
    opened_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pos_user_status ON positions(user_id, status, trading_mode);
CREATE INDEX IF NOT EXISTS idx_pos_strategy    ON positions(strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_pos_instrument  ON positions(instrument_id, status);
CREATE INDEX IF NOT EXISTS idx_pos_open        ON positions(status, trading_mode) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS idx_pos_closed_date ON positions(user_id, closed_at DESC) WHERE status = 'CLOSED';

CREATE TRIGGER trg_positions_updated_at
    BEFORE UPDATE ON positions
    FOR EACH ROW EXECUTE FUNCTION update_v2_updated_at();

-- ============================================================================
-- 11. POSITION_FILLS
-- ============================================================================
CREATE TABLE IF NOT EXISTS position_fills (
    id          SERIAL          PRIMARY KEY,
    position_id UUID            NOT NULL REFERENCES positions(position_id) ON DELETE CASCADE,
    fill_id     UUID            NOT NULL,
    order_id    UUID            NOT NULL,
    direction   VARCHAR(5)      NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pf_position_fill UNIQUE (position_id, fill_id)
);

CREATE INDEX IF NOT EXISTS idx_pf_position ON position_fills(position_id);
CREATE INDEX IF NOT EXISTS idx_pf_fill     ON position_fills(fill_id);
CREATE INDEX IF NOT EXISTS idx_pf_order    ON position_fills(order_id);

-- ============================================================================
-- 12. DAILY_PNL_SUMMARY
-- ============================================================================
CREATE TABLE IF NOT EXISTS daily_pnl_summary (
    id              SERIAL          PRIMARY KEY,
    user_id         VARCHAR(50)     NOT NULL,
    strategy_id     VARCHAR(50),
    trading_mode    VARCHAR(10)     NOT NULL,
    trade_date      DATE            NOT NULL,
    total_trades    INT             NOT NULL DEFAULT 0,
    winning_trades  INT             NOT NULL DEFAULT 0,
    losing_trades   INT             NOT NULL DEFAULT 0,
    total_invested  DECIMAL(15,2)   NOT NULL DEFAULT 0,
    gross_pnl       DECIMAL(15,2)   NOT NULL DEFAULT 0,
    total_charges   DECIMAL(10,2)   NOT NULL DEFAULT 0,
    net_pnl         DECIMAL(15,2)   NOT NULL DEFAULT 0,
    max_drawdown    DECIMAL(15,2),
    win_rate        DECIMAL(5,2),

    CONSTRAINT uq_dpnl UNIQUE (user_id, strategy_id, trade_date, trading_mode)
);

CREATE INDEX IF NOT EXISTS idx_dpnl_user_date ON daily_pnl_summary(user_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_dpnl_strategy  ON daily_pnl_summary(strategy_id, trade_date DESC);

-- ============================================================================
-- 13. SIGNAL_METRICS (partitioned by month)
-- ============================================================================
CREATE TABLE IF NOT EXISTS signal_metrics (
    id                          BIGSERIAL,
    signal_id                   UUID            NOT NULL,
    order_id                    UUID,
    user_id                     VARCHAR(50)     NOT NULL,
    strategy_id                 VARCHAR(50),
    symbol                      VARCHAR(50),
    exchange                    VARCHAR(10),
    trading_mode                VARCHAR(10),
    final_status                VARCHAR(20),
    signal_generated_at         TIMESTAMPTZ,
    signal_published_at         TIMESTAMPTZ,
    signal_consumed_at          TIMESTAMPTZ,
    risk_check_started_at       TIMESTAMPTZ,
    risk_check_completed_at     TIMESTAMPTZ,
    order_created_at            TIMESTAMPTZ,
    broker_request_sent_at      TIMESTAMPTZ,
    broker_response_received_at TIMESTAMPTZ,
    broker_ws_first_update_at   TIMESTAMPTZ,
    broker_ws_filled_at         TIMESTAMPTZ,
    position_opened_at          TIMESTAMPTZ,
    kafka_latency_ms            INT,
    processing_latency_ms       INT,
    broker_api_latency_ms       INT,
    broker_fill_latency_ms      INT,
    total_e2e_ms                INT,
    broker_api_retries          INT             NOT NULL DEFAULT 0,
    error_type                  VARCHAR(20),
    error_message               TEXT,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

DO $$
DECLARE
    start_date DATE; end_date DATE; partition_name TEXT; i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date     := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
        end_date       := start_date + '1 month'::interval;
        partition_name := 'signal_metrics_' || to_char(start_date, 'YYYY_MM');
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF signal_metrics FOR VALUES FROM (%L) TO (%L)',
                           partition_name, start_date, end_date);
        END IF;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS signal_metrics_default PARTITION OF signal_metrics DEFAULT;

CREATE INDEX IF NOT EXISTS idx_sm_signal   ON signal_metrics(signal_id, created_at);
CREATE INDEX IF NOT EXISTS idx_sm_order    ON signal_metrics(order_id, created_at) WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sm_user     ON signal_metrics(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sm_strategy ON signal_metrics(strategy_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sm_latency  ON signal_metrics(total_e2e_ms, created_at DESC) WHERE total_e2e_ms IS NOT NULL;

-- ============================================================================
-- 14. MULTI_LEVEL_EXIT_LEVELS
-- ============================================================================
CREATE TABLE IF NOT EXISTS multi_level_exit_levels (
    id              SERIAL          PRIMARY KEY,
    entry_order_id  UUID            NOT NULL,
    exit_type       VARCHAR(5)      NOT NULL CHECK (exit_type IN ('SL', 'TP')),
    level_num       INT             NOT NULL CHECK (level_num BETWEEN 1 AND 5),
    price_pct       DECIMAL(10,4)   NOT NULL CHECK (price_pct > 0),
    qty_pct         DECIMAL(10,4)   NOT NULL CHECK (qty_pct > 0),
    trigger_price    DECIMAL(15,2),
    exit_qty         INT,
    status           VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    exit_order_id    UUID,
    broker_order_id  VARCHAR(50),
    triggered_at     TIMESTAMPTZ,
    exit_price       DECIMAL(15,2),
    -- Rebalancing audit columns (set when opposite-side level fires and this
    -- level's qty is reduced to fit the remaining position size).
    original_exit_qty INT,           -- qty as first computed from entry fill; never changed after set
    rebalanced_at     TIMESTAMPTZ,   -- when qty was last reduced
    rebalance_reason  VARCHAR(50),   -- e.g. "SL_L1_TRIGGERED", "TP_L2_TRIGGERED"
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_ml_entry_type_level UNIQUE (entry_order_id, exit_type, level_num)
);

-- For existing deployments: add rebalancing audit columns if not present.
ALTER TABLE multi_level_exit_levels ADD COLUMN IF NOT EXISTS original_exit_qty INT;
ALTER TABLE multi_level_exit_levels ADD COLUMN IF NOT EXISTS rebalanced_at     TIMESTAMPTZ;
ALTER TABLE multi_level_exit_levels ADD COLUMN IF NOT EXISTS rebalance_reason  VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_ml_entry_order ON multi_level_exit_levels(entry_order_id);
CREATE INDEX IF NOT EXISTS idx_ml_active_sl   ON multi_level_exit_levels(exit_type, status)
    WHERE exit_type = 'SL' AND status IN ('PENDING', 'ACTIVE');
CREATE INDEX IF NOT EXISTS idx_ml_active_tp   ON multi_level_exit_levels(exit_type, status)
    WHERE exit_type = 'TP' AND status IN ('PENDING', 'ACTIVE');
CREATE INDEX IF NOT EXISTS idx_ml_broker_order ON multi_level_exit_levels(broker_order_id)
    WHERE broker_order_id IS NOT NULL;

CREATE OR REPLACE FUNCTION update_ml_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ml_updated_at
    BEFORE UPDATE ON multi_level_exit_levels
    FOR EACH ROW EXECUTE FUNCTION update_ml_updated_at();

-- ============================================================================
-- 15. USER_SQUARE_OFF_CONFIG
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_square_off_config (
    user_id         VARCHAR(50) PRIMARY KEY,
    square_off_time VARCHAR(5)  NOT NULL,
    enabled         BOOLEAN     NOT NULL DEFAULT true,
    updated_at      TIMESTAMP   DEFAULT NOW()
);

-- ============================================================================
-- PARTITION MAINTENANCE FUNCTION (run monthly via pg_cron)
-- ============================================================================
CREATE OR REPLACE FUNCTION create_monthly_partitions()
RETURNS void AS $$
DECLARE
    tables TEXT[] := ARRAY['order_status_history', 'fills', 'broker_requests', 'signal_metrics'];
    tbl TEXT; start_date DATE; end_date DATE; partition_name TEXT; i INT;
BEGIN
    FOREACH tbl IN ARRAY tables LOOP
        FOR i IN 0..3 LOOP
            start_date     := date_trunc('month', CURRENT_DATE) + (i || ' months')::interval;
            end_date       := start_date + '1 month'::interval;
            partition_name := tbl || '_' || to_char(start_date, 'YYYY_MM');
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
                EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                               partition_name, tbl, start_date, end_date);
                RAISE NOTICE 'Created partition: %', partition_name;
            END IF;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
