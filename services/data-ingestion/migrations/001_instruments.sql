-- Migration 001: Instruments master table
-- Stores all NSE/BSE stock instruments with metadata

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id SERIAL PRIMARY KEY,
    token VARCHAR(20) NOT NULL,               -- Broker token (e.g., "11536")
    isin VARCHAR(12),                          -- ISIN code (e.g., "INE002A01018")
    symbol VARCHAR(30) NOT NULL,               -- Trading symbol (e.g., "RELIANCE")
    series VARCHAR(10) NOT NULL DEFAULT 'EQ',  -- Series (EQ, BE, etc.)
    company_name VARCHAR(255),
    nse_code VARCHAR(30),                      -- NSE scrip code
    bse_code VARCHAR(30),                      -- BSE scrip code
    exchange VARCHAR(3) NOT NULL DEFAULT 'NSE' CHECK (exchange IN ('NSE', 'BSE')),
    sector VARCHAR(100),
    mcap_type VARCHAR(20),                     -- largecap, midcap, smallcap, microcap
    lot_size INTEGER DEFAULT 1,
    tick_size NUMERIC(10,4) DEFAULT 0.05,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Unique constraint: one instrument per token+exchange
CREATE UNIQUE INDEX IF NOT EXISTS idx_instruments_token_exchange
    ON instruments(token, exchange);

-- Lookup indexes
CREATE INDEX IF NOT EXISTS idx_instruments_symbol ON instruments(symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_isin ON instruments(isin) WHERE isin IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_instruments_nse_code ON instruments(nse_code) WHERE nse_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_instruments_bse_code ON instruments(bse_code) WHERE bse_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_instruments_active ON instruments(is_active);
CREATE INDEX IF NOT EXISTS idx_instruments_series ON instruments(series);
