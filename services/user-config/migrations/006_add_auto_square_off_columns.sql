-- Migration: Add auto square-off columns to risk_limits table
-- Version: 006
-- Description: Add enable_auto_square_off and auto_square_off_time columns for automatic position closing

-- Add columns to risk_limits table
ALTER TABLE risk_limits 
ADD COLUMN IF NOT EXISTS enable_auto_square_off BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS auto_square_off_time VARCHAR(20) DEFAULT '15:05';

-- Add comments explaining the columns
COMMENT ON COLUMN risk_limits.enable_auto_square_off IS 'Enable automatic square-off of positions at specified time';
COMMENT ON COLUMN risk_limits.auto_square_off_time IS 'Time for automatic square-off in HH:MM format (default: 15:05)';
