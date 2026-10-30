-- 013_manthan_refdata.sql  (signals_db)
--
-- WHY
--   Until 2026-10-30 every Manthan input came from one Google Sheet: live
--   GOOGLEFINANCE cells for price/PE/market cap, pasted Accord exports for
--   PAT/EPS/F-score, a LifeTimeHigh tab frozen since 27 Mar, and a hand-typed
--   "Buy" column as the entry trigger. Each outage traced back to that sheet
--   (blank PAT on SGFIN 2026-10-29, PE=0 drops, stale ATH). The operator
--   decided to replace it with API data and a mechanical trigger.
--
-- WHAT
--   Reference tables that the pipeline's DB-backed reader (MANTHAN_SOURCE=db)
--   assembles into the same ReadResult the sheet reader produces today:
--
--   ref_equity_eod        one row per company per trading day (equities API /eod)
--   ref_fundamentals      one row per company per quarter per requested basis
--                         (equities API /fundamentals; all three bases kept so
--                         the operator's "preferred" choice can be revisited)
--   ref_price_levels      vendor's intraday-high ATH / 52w levels (price-levels
--                         API) — kept for cross-checks, NOT the trigger basis
--   ref_ath_close         OUR close-based all-time-high per symbol: seeded from
--                         public history, rolled forward nightly from ref_equity_eod
--   ref_index_close       daily closes of the three bucket indices
--   ref_index_allocation  nightly EMA 21/50/100 score (−6..+6) → allocation
--   ref_industry_map      API industry name → Manthan (Accord) industry name
--                         used by the 25% sector cap
--   ref_load_runs         audit of every loader run (what, how many rows, error)

CREATE TABLE IF NOT EXISTS ref_equity_eod (
    trade_date          DATE          NOT NULL,
    isin                VARCHAR(12)   NOT NULL,
    nse_symbol          VARCHAR(30),
    bse_code            VARCHAR(30),
    company_name        VARCHAR(255),
    industry            VARCHAR(120),            -- API taxonomy (see ref_industry_map)
    sector              VARCHAR(120),
    close_price         NUMERIC(14,4) NOT NULL,
    shares_outstanding  NUMERIC(20,0),
    market_cap_cr       NUMERIC(16,4),
    loaded_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, isin)
);
CREATE INDEX IF NOT EXISTS idx_ref_equity_eod_symbol_date ON ref_equity_eod (nse_symbol, trade_date DESC);

CREATE TABLE IF NOT EXISTS ref_fundamentals (
    period              CHAR(7)       NOT NULL,  -- quarter end, YYYY-MM
    isin                VARCHAR(12)   NOT NULL,
    basis               VARCHAR(12)   NOT NULL,  -- requested: preferred | consolidated | standalone
    nse_symbol          VARCHAR(30),
    bse_code            VARCHAR(30),
    pat_cr              NUMERIC(16,4),
    pat_period_type     VARCHAR(4),              -- TTM | FY
    pat_basis           VARCHAR(12),             -- what the vendor actually used
    eps_ttm             NUMERIC(14,4),
    f_score             NUMERIC(5,2),
    loaded_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (period, isin, basis),
    CONSTRAINT chk_ref_fund_basis CHECK (basis IN ('preferred','consolidated','standalone'))
);
CREATE INDEX IF NOT EXISTS idx_ref_fundamentals_isin ON ref_fundamentals (isin, basis, period DESC);

CREATE TABLE IF NOT EXISTS ref_price_levels (
    symbol              VARCHAR(30)   PRIMARY KEY,
    co_code             BIGINT,
    company_name        VARCHAR(255),
    week52_high         NUMERIC(14,4),
    week52_low          NUMERIC(14,4),
    ath_intraday        NUMERIC(14,4),
    loaded_at           TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ref_ath_close (
    symbol              VARCHAR(30)   PRIMARY KEY,
    isin                VARCHAR(12),
    ath_close           NUMERIC(14,4) NOT NULL,
    ath_date            DATE,
    week52_high_close   NUMERIC(14,4),
    week52_low_close    NUMERIC(14,4),
    source              VARCHAR(40)   NOT NULL,  -- seed:yahoo | seed:eod | roll:eod
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ref_index_close (
    index_name          VARCHAR(30)   NOT NULL,  -- NIFTY50 | NFTYMCP150 | NTYSLCP250
    trade_date          DATE          NOT NULL,
    close               NUMERIC(14,4) NOT NULL,
    source              VARCHAR(40),
    loaded_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (index_name, trade_date)
);

CREATE TABLE IF NOT EXISTS ref_index_allocation (
    index_name          VARCHAR(30)   NOT NULL,
    trade_date          DATE          NOT NULL,  -- the close the score was computed on
    close               NUMERIC(14,4) NOT NULL,
    ema21               NUMERIC(14,4) NOT NULL,
    ema50               NUMERIC(14,4) NOT NULL,
    ema100              NUMERIC(14,4) NOT NULL,
    score               SMALLINT      NOT NULL,  -- −6..+6
    allocation          NUMERIC(6,4)  NOT NULL,  -- 0.10..1.00 (never 0)
    computed_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (index_name, trade_date),
    CONSTRAINT chk_ref_idx_score CHECK (score BETWEEN -6 AND 6),
    CONSTRAINT chk_ref_idx_alloc CHECK (allocation > 0 AND allocation <= 1)
);

CREATE TABLE IF NOT EXISTS ref_industry_map (
    api_industry        VARCHAR(120)  PRIMARY KEY,
    manthan_industry    VARCHAR(120)  NOT NULL,
    note                VARCHAR(255),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ref_load_runs (
    id                  BIGSERIAL     PRIMARY KEY,
    kind                VARCHAR(30)   NOT NULL,  -- eod | fundamentals | price_levels | ath_roll | index_ema | ath_seed
    params              TEXT,
    rows_written        INTEGER       NOT NULL DEFAULT 0,
    ok                  BOOLEAN       NOT NULL,
    error               TEXT,
    started_at          TIMESTAMPTZ   NOT NULL,
    finished_at         TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ref_load_runs_kind_time ON ref_load_runs (kind, finished_at DESC);

-- Mechanical Buy trigger (replaces the hand-typed ATH_Entry="Buy" column).
-- One row per symbol per IST trading day, written the first time the live LTP
-- trades above the stored close-based ATH. The pipeline's DB reader turns
-- today's rows into BuySignal rows; rules-engine/first_seen dedup downstream
-- is unchanged.
CREATE TABLE IF NOT EXISTS ref_buy_triggers (
    symbol              VARCHAR(30)   NOT NULL,
    trade_date          DATE          NOT NULL,
    triggered_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    ltp                 NUMERIC(14,4) NOT NULL,  -- price that crossed
    ath_close           NUMERIC(14,4) NOT NULL,  -- the ATH it crossed
    source              VARCHAR(40)   NOT NULL,  -- live:ext-redis | eod:close | manual:<admin>
    PRIMARY KEY (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_ref_buy_triggers_date ON ref_buy_triggers (trade_date DESC);
