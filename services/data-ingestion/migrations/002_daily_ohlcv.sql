-- Migration 002: Partitioned daily OHLCV table
-- Stores daily Open/High/Low/Close/Volume data for all stocks
-- Partitioned by YEAR on trade_date for efficient 52W queries
--
-- At ~5000 stocks x 250 trading days x 20 years = ~25M rows total
-- Each yearly partition holds ~1.25M rows (sweet spot for PostgreSQL)
-- 52W queries only scan 2 partitions (current + previous year)

CREATE TABLE IF NOT EXISTS daily_ohlcv (
    instrument_id INTEGER NOT NULL REFERENCES instruments(instrument_id),
    trade_date DATE NOT NULL,
    open NUMERIC(12,2) NOT NULL,
    high NUMERIC(12,2) NOT NULL,
    low NUMERIC(12,2) NOT NULL,
    close NUMERIC(12,2) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    delivery_volume BIGINT,
    delivery_pct NUMERIC(5,2),
    vwap NUMERIC(12,2),
    turnover NUMERIC(18,2),
    prev_close NUMERIC(12,2),
    pct_change NUMERIC(8,4),
    source VARCHAR(20) NOT NULL DEFAULT 'bhavcopy',  -- bhavcopy, b2c_api, yahoo
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (instrument_id, trade_date)
) PARTITION BY RANGE (trade_date);

-- Create yearly partitions: 2006 through 2027
-- Each partition covers [start, end) — end is exclusive
CREATE TABLE IF NOT EXISTS daily_ohlcv_2006 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2007 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2007-01-01') TO ('2008-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2008 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2009 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2010 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2011 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2012 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2013 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2014 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2015 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2016 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2017 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2018 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2019 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2020 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2021 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2022 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2023 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2024 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2025 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2026 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS daily_ohlcv_2027 PARTITION OF daily_ohlcv
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
