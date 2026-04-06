-- Migration 006: Breakout events table
-- Stores every 52W high/low breakout detected in real-time.
-- Used for:
--   1. Live screener: "these stocks broke 52W high TODAY" (via Redis)
--   2. Historical analysis: "show all RELIANCE breakouts in last 6 months" (via DB)

CREATE TABLE IF NOT EXISTS breakout_events (
    event_id BIGSERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(instrument_id),
    token VARCHAR(20) NOT NULL,
    symbol VARCHAR(30) NOT NULL,
    exchange VARCHAR(3) NOT NULL,

    -- Breakout details
    breakout_type VARCHAR(10) NOT NULL CHECK (breakout_type IN ('52W_HIGH', '52W_LOW')),
    breakout_price NUMERIC(12,2) NOT NULL,       -- LTP at breakout moment
    prev_52w_high NUMERIC(12,2),                  -- 52W high before breakout
    prev_52w_low NUMERIC(12,2),                   -- 52W low before breakout
    volume BIGINT,                                 -- Volume at breakout moment
    pct_change NUMERIC(8,4),                      -- % change from prev close

    -- Source of detection
    source VARCHAR(20) NOT NULL DEFAULT 'websocket', -- 'websocket', 'bhavcopy'

    -- Timestamps
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trade_date DATE NOT NULL DEFAULT CURRENT_DATE,

    -- One breakout per stock per type per day (ignore repeated ticks above 52W high)
    CONSTRAINT unique_breakout_per_day UNIQUE (instrument_id, breakout_type, trade_date)
);

-- "What broke out today?"
CREATE INDEX IF NOT EXISTS idx_breakout_trade_date ON breakout_events(trade_date DESC);

-- "Breakout history for this stock"
CREATE INDEX IF NOT EXISTS idx_breakout_instrument ON breakout_events(instrument_id, detected_at DESC);

-- "All 52W high breakouts today"
CREATE INDEX IF NOT EXISTS idx_breakout_type_date ON breakout_events(breakout_type, trade_date DESC);
