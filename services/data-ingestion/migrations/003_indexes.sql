-- Migration 003: Indexes for fast 52W and analytical queries
--
-- Primary query pattern (52W high/low):
--   SELECT MAX(high), MIN(low) FROM daily_ohlcv
--   WHERE instrument_id = $1 AND trade_date >= CURRENT_DATE - INTERVAL '52 weeks'
--
-- The composite PK (instrument_id, trade_date) already serves as the main B-tree index.
-- These additional indexes optimize specific access patterns.

-- Covering index for 52W query: enables index-only scans
-- PostgreSQL can answer MAX(high), MIN(low), close without touching the heap
CREATE INDEX IF NOT EXISTS idx_ohlcv_52w_cover ON daily_ohlcv
    (instrument_id, trade_date DESC)
    INCLUDE (high, low, close);

-- BRIN index on trade_date for bulk sequential scans across all stocks
-- Efficient for "give me all data for date X" type queries (bhavcopy loading)
CREATE INDEX IF NOT EXISTS idx_ohlcv_trade_date_brin ON daily_ohlcv
    USING BRIN (trade_date) WITH (pages_per_range = 32);

-- Source tracking: find all rows from a specific data source
CREATE INDEX IF NOT EXISTS idx_ohlcv_source ON daily_ohlcv(source);
