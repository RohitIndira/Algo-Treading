-- Migration: Add market cap range columns to strategy_conditions table
-- Version: 004
-- Description: Add min_market_cap and max_market_cap columns for market capitalization filtering

-- Add market cap columns to strategy_conditions table
ALTER TABLE strategy_conditions 
ADD COLUMN IF NOT EXISTS min_market_cap DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS max_market_cap DECIMAL(15,2);

-- Add comments explaining the columns
COMMENT ON COLUMN strategy_conditions.min_market_cap IS 'Minimum market capitalization filter for stocks (in crores)';
COMMENT ON COLUMN strategy_conditions.max_market_cap IS 'Maximum market capitalization filter for stocks (in crores)';

-- Add check constraint to ensure min is less than max when both are set
ALTER TABLE strategy_conditions 
ADD CONSTRAINT chk_market_cap_range 
CHECK (min_market_cap IS NULL OR max_market_cap IS NULL OR min_market_cap <= max_market_cap);
