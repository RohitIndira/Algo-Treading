-- ============================================================================
-- User Config Service — Initial Schema (Go-Live)
-- Single migration combining all prior incremental migrations.
-- Tables: strategies, strategy_conditions, trade_configs,
--         risk_limits, execution_outbox
-- ============================================================================

-- ============================================================================
-- 1. STRATEGIES
-- ============================================================================
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id   UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       VARCHAR(255)  NOT NULL,
    strategy_name VARCHAR(255)  NOT NULL,
    description   TEXT,
    active        BOOLEAN       DEFAULT false,
    trading_mode  VARCHAR(20)   DEFAULT 'PAPER' CHECK (trading_mode IN ('PAPER', 'LIVE')),
    version       INTEGER       DEFAULT 1,
    deleted_at    TIMESTAMPTZ   DEFAULT NULL,
    created_at    TIMESTAMPTZ   DEFAULT NOW(),
    updated_at    TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategies_user_id    ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_strategies_active     ON strategies(active);
CREATE INDEX IF NOT EXISTS idx_strategies_deleted_at ON strategies(deleted_at);

-- Enforce unique strategy name per user (only among non-deleted strategies)
CREATE UNIQUE INDEX IF NOT EXISTS uq_strategies_user_name
    ON strategies(user_id, strategy_name)
    WHERE deleted_at IS NULL;

-- ============================================================================
-- 2. STRATEGY_CONDITIONS
-- ============================================================================
CREATE TABLE IF NOT EXISTS strategy_conditions (
    condition_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id         UUID        NOT NULL UNIQUE REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    match_all_news      BOOLEAN     DEFAULT false,
    impact_score_min    INTEGER     DEFAULT 0,
    impact_score_max    INTEGER     DEFAULT 10,
    sentiments          TEXT[],
    news_categories     TEXT[],
    stock_codes         BIGINT[],
    min_market_cap      DECIMAL,
    max_market_cap      DECIMAL,
    market_cap_types    TEXT[],
    min_price_change_pct DECIMAL,
    max_price_change_pct DECIMAL,
    min_volume          BIGINT,
    exchanges           TEXT[],
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- 3. TRADE_CONFIGS
-- ============================================================================
CREATE TABLE IF NOT EXISTS trade_configs (
    trade_config_id    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id        UUID        NOT NULL UNIQUE REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    order_type         VARCHAR(50) NOT NULL,
    product_type       VARCHAR(50) NOT NULL,
    validity           VARCHAR(50) NOT NULL,
    quantity           INTEGER     NOT NULL,
    exchange           VARCHAR(20) NOT NULL,
    order_side         VARCHAR(20) NOT NULL DEFAULT 'BUY',
    limit_price        DECIMAL,
    stop_loss_pct      DECIMAL,
    take_profit_pct    DECIMAL,
    trailing_sl_pct    DECIMAL,
    stop_loss_type     VARCHAR(20) DEFAULT 'FIXED',
    take_profit_type   VARCHAR(20) DEFAULT 'FIXED',
    multi_level_sl     JSONB,
    multi_level_tp     JSONB,
    trade_window_start VARCHAR(5)  NOT NULL DEFAULT '',
    trade_window_end   VARCHAR(5)  NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tc_sl_type ON trade_configs(stop_loss_type);
CREATE INDEX IF NOT EXISTS idx_tc_tp_type ON trade_configs(take_profit_type);

COMMENT ON COLUMN trade_configs.trade_window_start IS 'Start of trade window HH:MM IST (24h). Empty = no restriction.';
COMMENT ON COLUMN trade_configs.trade_window_end   IS 'End of trade window HH:MM IST (24h). Empty = no restriction.';

-- ============================================================================
-- 4. RISK_LIMITS
-- ============================================================================
CREATE TABLE IF NOT EXISTS risk_limits (
    risk_limit_id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id                UUID        NOT NULL UNIQUE REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    max_daily_trades           INTEGER,
    max_per_trade_risk         DECIMAL,
    max_portfolio_exposure_pct DECIMAL,
    max_loss_per_day           DECIMAL,
    enable_risk_checks         BOOLEAN     DEFAULT true,
    enable_auto_square_off     BOOLEAN     DEFAULT false,
    auto_square_off_time       VARCHAR(5)  DEFAULT '15:15',
    created_at                 TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- 5. EXECUTION_OUTBOX (Transactional Outbox Pattern)
-- ============================================================================
CREATE TABLE IF NOT EXISTS execution_outbox (
    id           BIGSERIAL   PRIMARY KEY,
    aggregate_id UUID        NOT NULL,
    event_type   VARCHAR(255) NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    processed    BOOLEAN     DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_execution_outbox_processed ON execution_outbox(processed) WHERE processed = false;
