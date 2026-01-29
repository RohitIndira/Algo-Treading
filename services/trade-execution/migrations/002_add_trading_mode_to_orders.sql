-- Migration: Add trading_mode column to orders table for paper/live distinction
ALTER TABLE orders ADD COLUMN IF NOT EXISTS trading_mode VARCHAR(10) NOT NULL DEFAULT 'LIVE';
-- Optional: Add index for trading_mode
CREATE INDEX IF NOT EXISTS idx_orders_trading_mode ON orders(trading_mode);