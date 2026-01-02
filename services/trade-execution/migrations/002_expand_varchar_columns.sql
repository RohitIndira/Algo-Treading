-- Migration: Expand VARCHAR columns to prevent truncation errors
-- Date: 2026-01-02
-- Description: Expand VARCHAR(10) columns to VARCHAR(20) to handle proto enum formats

-- Expand orders table columns
ALTER TABLE IF EXISTS orders
  ALTER COLUMN exchange TYPE VARCHAR(20),
  ALTER COLUMN order_type TYPE VARCHAR(20),
  ALTER COLUMN order_side TYPE VARCHAR(20),
  ALTER COLUMN validity TYPE VARCHAR(20),
  ALTER COLUMN stop_loss_type TYPE VARCHAR(20);

-- Verify the columns were updated
-- SELECT column_name, data_type FROM information_schema.columns 
-- WHERE table_name = 'orders' AND column_name IN ('exchange', 'order_type', 'order_side', 'validity', 'stop_loss_type');
