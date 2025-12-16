-- Migration: Add additional columns to trade_configs table
-- Version: 005
-- Description: Add stop_loss_type, trailing_sl_pct, and product_type columns for advanced order configuration

-- Add columns to trade_configs table
ALTER TABLE trade_configs 
ADD COLUMN IF NOT EXISTS stop_loss_type VARCHAR(20) DEFAULT 'FIXED',
ADD COLUMN IF NOT EXISTS trailing_sl_pct DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS product_type VARCHAR(20) DEFAULT 'INTRADAY';

-- Add comments explaining the columns
COMMENT ON COLUMN trade_configs.stop_loss_type IS 'Type of stop loss: FIXED or TRAILING';
COMMENT ON COLUMN trade_configs.trailing_sl_pct IS 'Trailing stop loss percentage (used when stop_loss_type is TRAILING)';
COMMENT ON COLUMN trade_configs.product_type IS 'Product type for order execution: INTRADAY, DELIVERY, etc.';
