-- Migration: Fix VARCHAR column sizes to prevent truncation errors
-- Date: 2026-01-02
-- Description: Expand VARCHAR(10) columns to VARCHAR(20) to accommodate proto enum formats like EXCHANGE_NSE

-- ===========================================================================
-- TRADE EXECUTION SERVICE: Expand orders table columns
-- ===========================================================================

-- Expand exchange column to handle EXCHANGE_NSE format
ALTER TABLE IF EXISTS orders
  ALTER COLUMN exchange TYPE VARCHAR(20);

-- Expand order_type column to handle ORDER_TYPE_MARKET format
ALTER TABLE IF EXISTS orders
  ALTER COLUMN order_type TYPE VARCHAR(20);

-- Expand order_side column to handle longer formats
ALTER TABLE IF EXISTS orders
  ALTER COLUMN order_side TYPE VARCHAR(20);

-- Expand validity column to VARCHAR(20) for consistency
ALTER TABLE IF EXISTS orders
  ALTER COLUMN validity TYPE VARCHAR(20);

-- Expand stop_loss_type column to VARCHAR(20)
ALTER TABLE IF EXISTS orders
  ALTER COLUMN stop_loss_type TYPE VARCHAR(20);

-- ===========================================================================
-- RULES ENGINE SERVICE: Expand trade_signals table columns
-- ===========================================================================

-- Expand order_side in trade_signals table
ALTER TABLE IF EXISTS trade_signals
  ALTER COLUMN order_side TYPE VARCHAR(20);

-- ===========================================================================
-- Validation queries (commented out - run manually if needed)
-- ===========================================================================
-- Check orders table columns
-- SELECT column_name, data_type, character_maximum_length 
-- FROM information_schema.columns 
-- WHERE table_name = 'orders' 
--   AND column_name IN ('exchange', 'order_type', 'order_side', 'validity', 'stop_loss_type')
-- ORDER BY ordinal_position;

-- Check trade_signals table columns
-- SELECT column_name, data_type, character_maximum_length 
-- FROM information_schema.columns 
-- WHERE table_name = 'trade_signals' 
--   AND column_name IN ('order_type', 'order_side')
-- ORDER BY ordinal_position;
