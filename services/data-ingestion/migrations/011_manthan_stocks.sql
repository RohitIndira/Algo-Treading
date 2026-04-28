-- Migration 011: Manthan strategy — processed stocks and signals
--
-- Two tables:
--   manthan_stocks   — every BuySignal candidate we processed (with status
--                      + reason for audit). One row per symbol per run_date.
--   manthan_signals  — only the eligible stocks (status='ELIGIBLE'). This is
--                      what downstream services (Kafka consumers) read from.

CREATE TABLE IF NOT EXISTS manthan_stocks (
    id              BIGSERIAL PRIMARY KEY,
    run_date        DATE NOT NULL DEFAULT CURRENT_DATE,
    symbol          VARCHAR(30) NOT NULL,
    isin            VARCHAR(12),
    company_name    VARCHAR(255),
    industry        VARCHAR(100),
    bse_code        VARCHAR(30),
    nse_symbol      VARCHAR(30),

    market_cap      NUMERIC(14,2),
    pe              NUMERIC(10,2),
    pat             NUMERIC(14,2),
    fscore          NUMERIC(5,2),
    eps             NUMERIC(10,2),

    latest_price    NUMERIC(12,2),
    ath_close       NUMERIC(12,2),
    week52_high     NUMERIC(12,2),
    week52_low      NUMERIC(12,2),

    mcap_bucket     VARCHAR(10),                 -- LARGE / MID / SMALL
    index_name      VARCHAR(30),                 -- NIFTY50 / NFTYMCP150 / NTYSLCP250
    allocation      NUMERIC(6,4),                -- 0.0-1.0 from IndicesGrade

    status          VARCHAR(20) NOT NULL,        -- ELIGIBLE / FILTER_REJECTED / DATA_DROPPED
    reason          VARCHAR(255),                -- reason string when not ELIGIBLE
    ath_entry       VARCHAR(10),                 -- always "Buy" for processed rows

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_manthan_stock_per_day UNIQUE (run_date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_manthan_stocks_run_date   ON manthan_stocks(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_manthan_stocks_status     ON manthan_stocks(status, run_date DESC);
CREATE INDEX IF NOT EXISTS idx_manthan_stocks_symbol     ON manthan_stocks(symbol);


CREATE TABLE IF NOT EXISTS manthan_signals (
    id              BIGSERIAL PRIMARY KEY,
    run_date        DATE NOT NULL DEFAULT CURRENT_DATE,
    symbol          VARCHAR(30) NOT NULL,
    isin            VARCHAR(12),
    industry        VARCHAR(100),
    mcap_bucket     VARCHAR(10),
    index_name      VARCHAR(30),

    market_cap      NUMERIC(14,2),
    pe              NUMERIC(10,2),
    fscore          NUMERIC(5,2),
    pat             NUMERIC(14,2),

    latest_price    NUMERIC(12,2),
    ath_close       NUMERIC(12,2),
    week52_high     NUMERIC(12,2),

    published_to_kafka_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_manthan_signal_per_day UNIQUE (run_date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_manthan_signals_run_date ON manthan_signals(run_date DESC);
