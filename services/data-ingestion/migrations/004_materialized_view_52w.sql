-- Migration 004: Materialized view for 52-week high/low
--
-- Pre-computes 52W high and low for every instrument.
-- Refreshed daily at ~18:45 IST after bhavcopy ingestion.
-- CONCURRENTLY refresh requires the UNIQUE INDEX on instrument_id.
--
-- During market hours, real-time 52W data comes from broker WebSocket.
-- This view is the authoritative source for pre-market / post-market / offline.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_52w_high_low AS
SELECT
    d.instrument_id,
    i.token,
    i.symbol,
    i.isin,
    i.exchange,
    MAX(d.high) AS week_52_high,
    MIN(d.low) AS week_52_low,
    -- Latest closing price (subquery for the most recent trade date per instrument)
    (SELECT d2.close
     FROM daily_ohlcv d2
     WHERE d2.instrument_id = d.instrument_id
     ORDER BY d2.trade_date DESC
     LIMIT 1) AS last_close,
    MAX(d.trade_date) AS last_trade_date,
    -- Distance from 52W high/low as percentage
    ROUND(
        ((SELECT d3.close FROM daily_ohlcv d3
          WHERE d3.instrument_id = d.instrument_id
          ORDER BY d3.trade_date DESC LIMIT 1) - MIN(d.low))
        / NULLIF(MAX(d.high) - MIN(d.low), 0) * 100, 2
    ) AS position_in_52w_range  -- 0% = at 52W low, 100% = at 52W high
FROM daily_ohlcv d
JOIN instruments i ON i.instrument_id = d.instrument_id
WHERE d.trade_date >= (CURRENT_DATE - INTERVAL '52 weeks')
GROUP BY d.instrument_id, i.token, i.symbol, i.isin, i.exchange
WITH DATA;

-- UNIQUE index required for REFRESH CONCURRENTLY (no read locks during refresh)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_52w_instrument
    ON mv_52w_high_low(instrument_id);

-- Lookup indexes
CREATE INDEX IF NOT EXISTS idx_mv_52w_token ON mv_52w_high_low(token);
CREATE INDEX IF NOT EXISTS idx_mv_52w_symbol ON mv_52w_high_low(symbol);
CREATE INDEX IF NOT EXISTS idx_mv_52w_isin ON mv_52w_high_low(isin);
