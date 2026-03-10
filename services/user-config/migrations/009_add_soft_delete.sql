-- Add deleted_at column to strategies table for soft delete
ALTER TABLE strategies ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NULL;

-- Create index on deleted_at for faster filtering
CREATE INDEX IF NOT EXISTS idx_strategies_deleted_at ON strategies(deleted_at);
