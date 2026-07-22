-- ============================================================================
-- Algo Trading System - Complete Database Schema
-- Database: trading_db
-- Description: Creates all tables, indexes, triggers, and functions
--              for a fresh PostgreSQL instance.
-- ============================================================================

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- GENERIC HELPER: auto-update updated_at
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- USER CONFIG SERVICE
-- ============================================================================

-- Strategies
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        VARCHAR(255) NOT NULL,
    strategy_name  VARCHAR(255) NOT NULL,
    description    TEXT,
    active         BOOLEAN      DEFAULT false,
    trading_mode   VARCHAR(20)  DEFAULT 'PAPER' CHECK (trading_mode IN ('PAPER', 'LIVE')),
    version        INTEGER      DEFAULT 1,

    -- True when this strategy runs the After-Market-News backfill. Drives the
    -- reactivation AMN-preview requirement and the reactivation backfill trigger.
    process_after_market_news BOOLEAN NOT NULL DEFAULT false,

    created_at     TIMESTAMPTZ  DEFAULT NOW(),
    updated_at     TIMESTAMPTZ  DEFAULT NOW(),
    deleted_at     TIMESTAMPTZ  DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_strategies_user_id    ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_strategies_active     ON strategies(active);
CREATE INDEX IF NOT EXISTS idx_strategies_deleted_at ON strategies(deleted_at);

-- Enforce unique strategy name per user (only among non-deleted strategies)
CREATE UNIQUE INDEX IF NOT EXISTS uq_strategies_user_name
    ON strategies(user_id, strategy_name)
    WHERE deleted_at IS NULL;

CREATE TRIGGER update_strategies_updated_at BEFORE UPDATE ON strategies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Strategy conditions (one row per strategy)
CREATE TABLE IF NOT EXISTS strategy_conditions (
    condition_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id        UUID NOT NULL UNIQUE REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    match_all_news     BOOLEAN  DEFAULT false,
    impact_score_min   INTEGER  DEFAULT 0,
    impact_score_max   INTEGER  DEFAULT 10,
    sentiments         TEXT[],
    news_categories    TEXT[],
    stock_codes        BIGINT[],
    min_market_cap     DECIMAL,
    max_market_cap     DECIMAL,
    market_cap_types   TEXT[],
    min_price_change_pct DECIMAL,
    max_price_change_pct DECIMAL,
    min_volume         BIGINT,
    exchanges          TEXT[],
    created_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Trade configuration (one row per strategy)
CREATE TABLE IF NOT EXISTS trade_configs (
    trade_config_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id       UUID        NOT NULL UNIQUE REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    order_type        VARCHAR(50) NOT NULL,
    product_type      VARCHAR(50) NOT NULL,
    validity          VARCHAR(50) NOT NULL,
    quantity          INTEGER     NOT NULL,
    exchange          VARCHAR(20) NOT NULL,
    order_side        VARCHAR(20) NOT NULL DEFAULT 'BUY',
    limit_price       DECIMAL,
    stop_loss_pct     DECIMAL,
    take_profit_pct   DECIMAL,
    trailing_sl_pct   DECIMAL,
    stop_loss_type    VARCHAR(20) DEFAULT 'FIXED',
    take_profit_type  VARCHAR(20) DEFAULT 'FIXED',
    multi_level_sl    JSONB,
    multi_level_tp    JSONB,
    trade_window_start VARCHAR(5) NOT NULL DEFAULT '',
    trade_window_end   VARCHAR(5) NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Risk limits (one row per strategy)
CREATE TABLE IF NOT EXISTS risk_limits (
    risk_limit_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id               UUID    NOT NULL UNIQUE REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    max_daily_trades          INTEGER,
    max_per_trade_risk        DECIMAL,
    max_portfolio_exposure_pct DECIMAL,
    max_loss_per_day          DECIMAL,
    enable_risk_checks        BOOLEAN    DEFAULT true,
    enable_auto_square_off    BOOLEAN    DEFAULT false,
    auto_square_off_time      VARCHAR(5) DEFAULT '15:15',
    max_amount_per_stock      NUMERIC(20, 4) DEFAULT NULL,
    max_trades_per_strategy   INTEGER        DEFAULT NULL,
    created_at                TIMESTAMPTZ DEFAULT NOW()
);

-- Transactional outbox for Kafka publishing
CREATE TABLE IF NOT EXISTS execution_outbox (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id UUID         NOT NULL,
    event_type   VARCHAR(255) NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  DEFAULT NOW(),
    processed    BOOLEAN      DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_execution_outbox_processed ON execution_outbox(processed) WHERE processed = false;

-- AMN activations (parent: one row per strategy per activation-day)
CREATE TABLE IF NOT EXISTS amn_activations (
    activation_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id      UUID         NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    user_id          VARCHAR(255) NOT NULL,
    trading_date     DATE         NOT NULL,
    source           VARCHAR(16)  NOT NULL DEFAULT 'CREATE' CHECK (source IN ('CREATE', 'REACTIVATE')),
    strategy_version INTEGER      NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ  DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  DEFAULT NOW()
);

-- One activation record per strategy per trading day. Re-activating the same day
-- upserts this row (and replaces its child stocks) rather than duplicating.
CREATE UNIQUE INDEX IF NOT EXISTS uq_amn_activations_strategy_date
    ON amn_activations(strategy_id, trading_date);

CREATE INDEX IF NOT EXISTS idx_amn_activations_strategy_date
    ON amn_activations(strategy_id, trading_date DESC);

CREATE INDEX IF NOT EXISTS idx_amn_activations_user
    ON amn_activations(user_id);

-- AMN activation stocks (child: one row per user-selected stock)
CREATE TABLE IF NOT EXISTS amn_activation_stocks (
    id              BIGSERIAL   PRIMARY KEY,
    activation_id   UUID        NOT NULL REFERENCES amn_activations(activation_id) ON DELETE CASCADE,
    isin            VARCHAR(20) NOT NULL,
    symbol          VARCHAR(64) NOT NULL DEFAULT '',
    nse_code        BIGINT      NOT NULL DEFAULT 0,
    -- bucket: 'place'   → order fired immediately at live LTP.
    --         'monitor' → price-watch selection at target_price.
    bucket          VARCHAR(8)  NOT NULL DEFAULT 'place' CHECK (bucket IN ('place', 'monitor')),
    target_price    DECIMAL     NOT NULL DEFAULT 0,   -- monitor trigger level (0 for 'place')
    entry_price     DECIMAL     NOT NULL DEFAULT 0,   -- preview-time expected fill, tick-rounded
    quantity        INTEGER     NOT NULL DEFAULT 0,   -- preview-time qty from per-stock budget
    invested_amount DECIMAL     NOT NULL DEFAULT 0,   -- entry_price * quantity snapshot
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_amn_activation_stocks_activation
    ON amn_activation_stocks(activation_id);

-- The same stock appears once per activation record.
CREATE UNIQUE INDEX IF NOT EXISTS uq_amn_activation_stocks_activation_isin
    ON amn_activation_stocks(activation_id, isin);

-- ============================================================================
-- TRADE EXECUTION SERVICE
-- ============================================================================

-- User credentials (Indira Securities broker auth)
CREATE TABLE IF NOT EXISTS user_credentials (
    id                   SERIAL PRIMARY KEY,
    user_id              VARCHAR(50)  NOT NULL UNIQUE,
    indira_user_id       VARCHAR(100) NOT NULL,
    indira_app_id        VARCHAR(100) NOT NULL,
    indira_source        VARCHAR(20)  NOT NULL DEFAULT 'WEB',
    indira_bearer_token  TEXT         NOT NULL,
    created_at           TIMESTAMP DEFAULT NOW(),
    updated_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_credentials_user_id ON user_credentials(user_id);

CREATE TRIGGER update_user_credentials_updated_at BEFORE UPDATE ON user_credentials
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    order_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        VARCHAR(50)  NOT NULL,
    strategy_id    VARCHAR(50)  NOT NULL,
    strategy_name  VARCHAR(255) DEFAULT '',
    event_id       UUID         NOT NULL,

    -- Stock information
    stock_code     BIGINT       NOT NULL,
    exchange       VARCHAR(10)  NOT NULL,
    symbol         VARCHAR(50)  NOT NULL,

    -- Order details
    order_type     VARCHAR(10)  NOT NULL,
    order_side     VARCHAR(10)  NOT NULL,
    quantity       INT          NOT NULL,
    price          DECIMAL(15,2),

    -- Stop loss and take profit
    stop_loss      DECIMAL(15,2),
    take_profit    DECIMAL(15,2),

    -- Order validity / product
    validity       VARCHAR(10)  DEFAULT 'DAY',
    product_type   VARCHAR(20)  DEFAULT 'INTRADAY',
    target_price   DECIMAL(15,2),

    -- Order status
    status         VARCHAR(20)  NOT NULL DEFAULT 'RECEIVED',

    -- Broker API integration (Indira Securities)
    indira_order_id  VARCHAR(50),
    indira_response  TEXT,

    -- Legacy Odin fields (kept for backward compat)
    odin_order_id    VARCHAR(50),
    odin_response    TEXT,

    -- Raw broker WebSocket data
    broker_status          VARCHAR(50),
    broker_ws_data         JSONB,
    exchange_order_number  VARCHAR(50),

    -- Frontend auth data (temporary, for broker API calls)
    bearer_token   TEXT,
    app_id         VARCHAR(100),
    source         VARCHAR(20),

    -- Stop loss configuration
    stop_loss_type   VARCHAR(20) DEFAULT 'FIXED',
    trailing_sl_pct  DECIMAL(10,4),
    highest_price    DECIMAL(15,2),

    -- Auto square-off
    is_square_off_order   BOOLEAN     DEFAULT false,
    -- Per-user override: HH:MM IST at which all this user's positions are force-closed.
    -- NULL = use the global default (15:05 live, 15:00 paper).
    auto_square_off_time  VARCHAR(5),

    -- Paper trading
    is_paper_trade    BOOLEAN      DEFAULT false,
    trading_mode      VARCHAR(10)  DEFAULT 'LIVE',
    paper_exit_price  DECIMAL(15,2),
    paper_pnl         DECIMAL(15,2),
    paper_exit_time   TIMESTAMPTZ,

    -- Live trading exit
    live_exit_price   DECIMAL(15,2),
    live_pnl          DECIMAL(15,2),
    live_exit_time    TIMESTAMPTZ,
    -- Why a live position was closed: STOP_LOSS | TAKE_PROFIT | FORCE_EXIT |
    -- SQUARE_OFF | MANUAL. rejection_reason stays for genuine broker rejections.
    live_exit_reason  TEXT,

    -- Price context at order creation
    current_pct_change DECIMAL(10,4) DEFAULT 0,
    max_monitor_price  DECIMAL(15,2) DEFAULT 0,

    -- Strategy daily trade cap, enforced at price-monitor trigger time (pkg/tradecap). 0 = no limit.
    max_trades_per_strategy INTEGER NOT NULL DEFAULT 0,

    -- OCO (One-Cancels-the-Other) group
    oco_group_id    UUID,
    oco_role        VARCHAR(20),
    parent_order_id UUID,

    -- Signal deduplication
    signal_id       UUID,

    -- Execution details
    filled_quantity INT          DEFAULT 0,
    filled_price    DECIMAL(15,2),
    commission      DECIMAL(10,2),
    total_cost      DECIMAL(15,2),

    -- Risk approval
    risk_approved   BOOLEAN      DEFAULT false,
    risk_score      DECIMAL(5,2),

    -- Timestamps
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    submitted_at    TIMESTAMP,
    executed_at     TIMESTAMP,

    -- Error handling
    error_message     TEXT,
    rejection_reason  TEXT,
    retry_count       INT DEFAULT 0,

    -- Constraints
    CONSTRAINT chk_quantity_positive  CHECK (quantity > 0),
    CONSTRAINT chk_price_positive    CHECK (price IS NULL OR price > 0),
    CONSTRAINT chk_filled_quantity   CHECK (filled_quantity >= 0 AND filled_quantity <= quantity)
);

-- Orders indexes
CREATE INDEX IF NOT EXISTS idx_orders_user_id      ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status       ON orders(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_event_id     ON orders(event_id);
CREATE INDEX IF NOT EXISTS idx_orders_strategy_id  ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_stock_code   ON orders(stock_code);
CREATE INDEX IF NOT EXISTS idx_orders_created_at   ON orders(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_odin_id      ON orders(odin_order_id)    WHERE odin_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_indira_id    ON orders(indira_order_id)  WHERE indira_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_product_type ON orders(product_type, status) WHERE is_paper_trade = false;
CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_number ON orders(exchange_order_number) WHERE exchange_order_number IS NOT NULL;

-- Paper trading indexes
CREATE INDEX IF NOT EXISTS idx_orders_auto_sq_off_time ON orders(auto_square_off_time, user_id)
    WHERE auto_square_off_time IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_paper_trade  ON orders(user_id, is_paper_trade, status)        WHERE is_paper_trade = true;
CREATE INDEX IF NOT EXISTS idx_orders_paper_symbol ON orders(symbol, is_paper_trade, status)          WHERE is_paper_trade = true;
CREATE INDEX IF NOT EXISTS idx_orders_paper_closed ON orders(user_id, is_paper_trade, paper_exit_price) WHERE is_paper_trade = true AND paper_exit_price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_paper_open   ON orders(user_id, is_paper_trade, status)        WHERE is_paper_trade = true AND status = 'FILLED';
CREATE INDEX IF NOT EXISTS idx_orders_paper_exit_time ON orders(user_id, paper_exit_time) WHERE is_paper_trade = true AND paper_exit_time IS NOT NULL;

-- Live trading indexes
CREATE INDEX IF NOT EXISTS idx_orders_live_closed  ON orders(user_id, is_paper_trade, live_exit_price) WHERE is_paper_trade = false AND live_exit_price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_live_open    ON orders(user_id, is_paper_trade, status)         WHERE is_paper_trade = false AND status IN ('FILLED', 'PARTIALLY_FILLED');
CREATE INDEX IF NOT EXISTS idx_orders_live_exit_time ON orders(user_id, live_exit_time) WHERE is_paper_trade = false AND live_exit_time IS NOT NULL;

-- OCO indexes
CREATE INDEX IF NOT EXISTS idx_orders_oco_group    ON orders(oco_group_id)          WHERE oco_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_oco_active   ON orders(oco_group_id, status)  WHERE oco_group_id IS NOT NULL AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED', 'FAILED');

-- Signal dedup
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_signal_id_unique ON orders(signal_id) WHERE signal_id IS NOT NULL;

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Execution events (order lifecycle log)
CREATE TABLE IF NOT EXISTS execution_events (
    id          SERIAL PRIMARY KEY,
    order_id    UUID        NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type  VARCHAR(20) NOT NULL,
    event_data  JSONB,
    created_at  TIMESTAMP   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_events_order_id   ON execution_events(order_id);
CREATE INDEX IF NOT EXISTS idx_execution_events_created_at ON execution_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_events_type       ON execution_events(event_type);

-- ============================================================================
-- RULES ENGINE SERVICE
-- ============================================================================

-- Trade signals
CREATE TABLE IF NOT EXISTS trade_signals (
    signal_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       VARCHAR(255) NOT NULL UNIQUE,

    -- User and strategy info
    user_id        VARCHAR(255) NOT NULL,
    strategy_id    UUID         NOT NULL,
    strategy_name  VARCHAR(255) NOT NULL,

    -- Event info
    event_id       VARCHAR(255) NOT NULL,

    -- Stock details
    stock_code     BIGINT       NOT NULL,
    symbol         VARCHAR(50)  NOT NULL,
    exchange       VARCHAR(20)  NOT NULL,

    -- Order details
    order_type     VARCHAR(20)  NOT NULL,
    order_side     VARCHAR(10)  NOT NULL,
    quantity       INTEGER      NOT NULL,
    price          DECIMAL(15,2) NOT NULL,
    stop_loss      DECIMAL(15,2),
    take_profit    DECIMAL(15,2),

    -- Matching info
    match_score    DECIMAL(5,2) NOT NULL,
    impact_score   INTEGER      NOT NULL,
    sentiment      VARCHAR(50),
    news_category  VARCHAR(255),

    -- Execution tracking
    status           VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- IMMEDIATE = real trade placed now; MONITORING = price-monitor watch (not a
    -- trade, and not counted against the daily cap, until its target triggers).
    signal_kind      VARCHAR(20) NOT NULL DEFAULT 'IMMEDIATE',
    execution_price  DECIMAL(15,2),
    execution_time   TIMESTAMP,
    broker_order_id  VARCHAR(255),
    error_message    TEXT,

    -- Timestamps
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Metadata
    metadata    JSONB
);

CREATE INDEX IF NOT EXISTS idx_trade_signals_user_id     ON trade_signals(user_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_strategy_id ON trade_signals(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_status      ON trade_signals(status);
CREATE INDEX IF NOT EXISTS idx_trade_signals_stock_code  ON trade_signals(stock_code);
CREATE INDEX IF NOT EXISTS idx_trade_signals_created_at  ON trade_signals(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_signals_event_id    ON trade_signals(event_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_user_status ON trade_signals(user_id, status, created_at DESC);

CREATE TRIGGER trade_signals_updated_at BEFORE UPDATE ON trade_signals
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- MULTI-LEVEL EXIT LEVELS (migration 014)
-- ============================================================================

CREATE TABLE IF NOT EXISTS multi_level_exit_levels (
    id              SERIAL          PRIMARY KEY,
    entry_order_id  UUID            NOT NULL,
    exit_type       VARCHAR(5)      NOT NULL CHECK (exit_type IN ('SL', 'TP')),
    level_num       INT             NOT NULL CHECK (level_num BETWEEN 1 AND 5),
    price_pct       DECIMAL(10,4)   NOT NULL CHECK (price_pct > 0),
    qty_pct         DECIMAL(10,4)   NOT NULL CHECK (qty_pct > 0),
    trigger_price   DECIMAL(15,2),
    exit_qty        INT,
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    exit_order_id   UUID,
    broker_order_id VARCHAR(50),
    triggered_at    TIMESTAMPTZ,
    exit_price      DECIMAL(15,2),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ml_entry_type_level UNIQUE (entry_order_id, exit_type, level_num)
);

CREATE INDEX IF NOT EXISTS idx_ml_entry_order ON multi_level_exit_levels(entry_order_id);
CREATE INDEX IF NOT EXISTS idx_ml_active_sl   ON multi_level_exit_levels(exit_type, status) WHERE exit_type = 'SL' AND status IN ('PENDING', 'ACTIVE');
CREATE INDEX IF NOT EXISTS idx_ml_active_tp   ON multi_level_exit_levels(exit_type, status) WHERE exit_type = 'TP' AND status IN ('PENDING', 'ACTIVE');
CREATE INDEX IF NOT EXISTS idx_ml_broker_order ON multi_level_exit_levels(broker_order_id)  WHERE broker_order_id IS NOT NULL;

CREATE OR REPLACE FUNCTION update_ml_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ml_updated_at
    BEFORE UPDATE ON multi_level_exit_levels
    FOR EACH ROW EXECUTE FUNCTION update_ml_updated_at();

-- ============================================================================
-- USER AUTO SQUARE-OFF CONFIG (migration 016)
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_square_off_config (
    user_id         VARCHAR(50) PRIMARY KEY,
    square_off_time VARCHAR(5)  NOT NULL,
    enabled         BOOLEAN     NOT NULL DEFAULT true,
    updated_at      TIMESTAMP   DEFAULT NOW()
);

-- ============================================================================
-- TABLE COMMENTS
-- ============================================================================

COMMENT ON TABLE strategies              IS 'User trading strategies configuration';
COMMENT ON TABLE strategy_conditions     IS 'Conditions/filters for strategy triggers';
COMMENT ON TABLE trade_configs           IS 'Trade execution configuration per strategy';
COMMENT ON TABLE risk_limits             IS 'Risk management limits per strategy';
COMMENT ON TABLE execution_outbox        IS 'Transactional outbox for reliable Kafka publishing';
COMMENT ON TABLE user_credentials        IS 'Indira Securities broker authentication credentials';
COMMENT ON TABLE orders                  IS 'Order records submitted to broker';
COMMENT ON TABLE execution_events        IS 'Event log for order lifecycle tracking';
COMMENT ON TABLE trade_signals           IS 'Trade signals generated by rules engine';
COMMENT ON TABLE multi_level_exit_levels IS 'Runtime state of each partial SL/TP exit level per entry order';
COMMENT ON TABLE user_square_off_config  IS 'Per-user auto square-off time stored natively in trade-execution';
