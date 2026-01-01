-- Migration: Add depth-related fields to strategy_conditions
-- Version: 002
-- Adds min_bid_quantity, min_ask_quantity, max_spread_pct, depth_only, require_ltp_between_spread

ALTER TABLE strategy_conditions
ADD COLUMN IF NOT EXISTS min_bid_quantity BIGINT;

ALTER TABLE strategy_conditions
ADD COLUMN IF NOT EXISTS min_ask_quantity BIGINT;

ALTER TABLE strategy_conditions
ADD COLUMN IF NOT EXISTS max_spread_pct DECIMAL(10,4);

ALTER TABLE strategy_conditions
ADD COLUMN IF NOT EXISTS depth_only BOOLEAN DEFAULT false;

ALTER TABLE strategy_conditions
ADD COLUMN IF NOT EXISTS require_ltp_between_spread BOOLEAN DEFAULT false;

-- Add indexes if useful
CREATE INDEX IF NOT EXISTS idx_strategy_conditions_min_bid_quantity ON strategy_conditions(min_bid_quantity);
CREATE INDEX IF NOT EXISTS idx_strategy_conditions_min_ask_quantity ON strategy_conditions(min_ask_quantity);
