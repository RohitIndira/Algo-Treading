-- Add limit_price column to trade_configs
ALTER TABLE trade_configs ADD COLUMN IF NOT EXISTS limit_price DECIMAL;
