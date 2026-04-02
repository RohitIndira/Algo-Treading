-- Migration 005: Data load tracking table
-- Tracks every bhavcopy download and ingestion for auditing and debugging.
-- Helps answer: "Did we load data for April 1?" and "When did it last fail?"

CREATE TABLE IF NOT EXISTS data_load_log (
    load_id SERIAL PRIMARY KEY,
    source VARCHAR(20) NOT NULL,              -- 'nse_bhavcopy', 'bse_bhavcopy', 'yahoo', 'b2c_api'
    trade_date DATE NOT NULL,                  -- The market date this data belongs to
    exchange VARCHAR(3) NOT NULL,              -- 'NSE' or 'BSE'
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    rows_skipped INTEGER NOT NULL DEFAULT 0,   -- Duplicates or invalid rows
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    error_message TEXT,
    file_name VARCHAR(255),                    -- Original bhavcopy filename
    file_size_bytes BIGINT,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER                        -- How long the load took
);

-- Prevent duplicate loads for the same date+source+exchange
CREATE UNIQUE INDEX IF NOT EXISTS idx_load_log_unique
    ON data_load_log(source, trade_date, exchange)
    WHERE status = 'SUCCESS';

-- Find recent loads quickly
CREATE INDEX IF NOT EXISTS idx_load_log_date ON data_load_log(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_load_log_status ON data_load_log(status)
    WHERE status IN ('RUNNING', 'FAILED');
