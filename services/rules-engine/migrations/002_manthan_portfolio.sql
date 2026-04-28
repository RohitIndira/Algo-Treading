-- Migration 002: Manthan portfolio tracking
-- Tracks per-user, per-stock positions, P&L, SL state in real-time.

CREATE TABLE IF NOT EXISTS manthan_positions (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     UUID NOT NULL,
    user_id         VARCHAR(255) NOT NULL,
    symbol          VARCHAR(30) NOT NULL,
    isin            VARCHAR(12),
    industry        VARCHAR(100),
    mcap_bucket     VARCHAR(10),
    index_name      VARCHAR(30),

    entry_price     NUMERIC(12,2) NOT NULL,
    quantity        INT NOT NULL,
    invested_amt    NUMERIC(14,2) NOT NULL,
    ema_alloc_pct   NUMERIC(6,4),

    high_since_entry NUMERIC(12,2),
    current_sl       NUMERIC(12,2),
    last_trail_level NUMERIC(12,2),

    status          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE, EXITED, COOLDOWN
    exit_price      NUMERIC(12,2),
    realized_pnl    NUMERIC(14,2),
    exit_reason     VARCHAR(50),  -- SL_TRIGGERED, MANUAL

    entry_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exit_time       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_manthan_position UNIQUE (strategy_id, symbol, entry_time)
);

CREATE INDEX IF NOT EXISTS idx_manthan_pos_user      ON manthan_positions(user_id);
CREATE INDEX IF NOT EXISTS idx_manthan_pos_strategy  ON manthan_positions(strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_manthan_pos_active    ON manthan_positions(status) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_manthan_pos_symbol    ON manthan_positions(symbol, status);

-- Portfolio summary per strategy (current capital, position count)
CREATE TABLE IF NOT EXISTS manthan_portfolio_state (
    strategy_id     UUID PRIMARY KEY,
    user_id         VARCHAR(255) NOT NULL,
    initial_capital NUMERIC(14,2) NOT NULL,
    current_capital NUMERIC(14,2) NOT NULL,
    max_positions   INT NOT NULL,
    per_stock_base  NUMERIC(14,2) NOT NULL,
    active_count    INT NOT NULL DEFAULT 0,
    total_realized_pnl NUMERIC(14,2) NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_manthan_pstate_user ON manthan_portfolio_state(user_id);

-- Cooldown entries (stocks awaiting 20% ATH correction for re-entry)
CREATE TABLE IF NOT EXISTS manthan_cooldown (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     UUID NOT NULL,
    symbol          VARCHAR(30) NOT NULL,
    ath_at_exit     NUMERIC(12,2) NOT NULL,
    exit_price      NUMERIC(12,2) NOT NULL,
    reentry_below   NUMERIC(12,2) NOT NULL,
    exit_time       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cleared         BOOLEAN NOT NULL DEFAULT FALSE,
    cleared_at      TIMESTAMPTZ,

    CONSTRAINT uq_manthan_cooldown UNIQUE (strategy_id, symbol)
);
