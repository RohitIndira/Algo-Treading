-- Add strategy_name column to orders table for display in UI positions.
-- Populated from the trade signal when the order is created.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS strategy_name VARCHAR(255) DEFAULT '';
