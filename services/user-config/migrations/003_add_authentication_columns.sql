-- Migration: Add authentication columns to strategies table
-- Version: 003
-- Description: Add bearer_token, app_id, and source columns for storing frontend authentication data

-- Add authentication columns to strategies table
ALTER TABLE strategies 
ADD COLUMN IF NOT EXISTS bearer_token TEXT,
ADD COLUMN IF NOT EXISTS app_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS source VARCHAR(50);

-- Add comments explaining the columns
COMMENT ON COLUMN strategies.bearer_token IS 'JWT bearer token from frontend for order execution authentication';
COMMENT ON COLUMN strategies.app_id IS 'Application ID from frontend for order execution authentication';
COMMENT ON COLUMN strategies.source IS 'Source identifier from frontend for order execution authentication';

-- Create index for faster queries on active strategies with authentication
CREATE INDEX IF NOT EXISTS idx_strategies_auth_active 
ON strategies(user_id, active) 
WHERE bearer_token IS NOT NULL;
