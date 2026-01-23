-- Migration: Add trading_mode column to strategies
-- Version: 003
-- Description: Per-strategy trading mode (LIVE/PAPER) to support
--               user-specific paper/live settings for managed
--               strategies like CASH_52W_HIGH.

ALTER TABLE strategies
ADD COLUMN IF NOT EXISTS trading_mode VARCHAR(20) NOT NULL DEFAULT 'LIVE';

-- Optional comment for documentation
COMMENT ON COLUMN strategies.trading_mode IS 'Per-strategy trading mode: LIVE or PAPER';

