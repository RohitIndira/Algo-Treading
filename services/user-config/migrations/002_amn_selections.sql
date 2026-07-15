-- ============================================================================
-- User Config Service — AMN persisted selections & day-wise activation history
-- Migration 002.
--
-- Adds:
--   1. strategies.process_after_market_news  — persisted "is this an AMN
--      strategy" flag (was creation-time-only / db:"-" before). Reactivation and
--      the UI need this to decide whether to require an AMN preview + selection.
--   2. amn_activations        — one row per strategy per trading day (parent).
--   3. amn_activation_stocks  — one row per user-selected stock (child), including
--      the 'monitor' (price-watch) picks with their target price.
--
-- Idempotent (CREATE ... IF NOT EXISTS / ADD COLUMN IF NOT EXISTS), matching the
-- style of 001_init.sql. Apply with:
--   psql -h <host> -U <user> -d <db> -f migrations/002_amn_selections.sql
-- ============================================================================

-- ============================================================================
-- 1. STRATEGIES — persist the AMN opt-in flag
-- ============================================================================
ALTER TABLE strategies
    ADD COLUMN IF NOT EXISTS process_after_market_news BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN strategies.process_after_market_news IS
    'True when this strategy runs the After-Market-News backfill. Drives the reactivation AMN-preview requirement and the reactivation backfill trigger.';

-- ============================================================================
-- 2. AMN_ACTIVATIONS  (parent: one row per strategy per activation-day)
-- ============================================================================
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

-- Newest-first lookups for "what did this strategy pick, per day".
CREATE INDEX IF NOT EXISTS idx_amn_activations_strategy_date
    ON amn_activations(strategy_id, trading_date DESC);

CREATE INDEX IF NOT EXISTS idx_amn_activations_user
    ON amn_activations(user_id);

-- ============================================================================
-- 3. AMN_ACTIVATION_STOCKS  (child: one row per selected stock)
-- ============================================================================
CREATE TABLE IF NOT EXISTS amn_activation_stocks (
    id              BIGSERIAL   PRIMARY KEY,
    activation_id   UUID        NOT NULL REFERENCES amn_activations(activation_id) ON DELETE CASCADE,
    isin            VARCHAR(20) NOT NULL,
    symbol          VARCHAR(64) NOT NULL DEFAULT '',
    nse_code        BIGINT      NOT NULL DEFAULT 0,
    -- bucket: 'place'   → order fired immediately at live LTP.
    --         'monitor' → price-watch selection at target_price (the "monitoring" pick).
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

COMMENT ON COLUMN amn_activation_stocks.bucket IS
    '''place'' = immediate order at live LTP; ''monitor'' = price-watch selection triggering at target_price.';
