-- Migration 012: Add MANTHAN strategy type
-- Manthan = ATH breakout strategy. User provides only strategy_name + total_capital.
-- Backend fills all trade config (DELIVERY, TRAILING SL 20%, 2% trail, EMA allocation).

-- Update the strategy_type CHECK constraint to include MANTHAN
ALTER TABLE strategies DROP CONSTRAINT IF EXISTS strategies_strategy_type_check;
ALTER TABLE strategies ADD CONSTRAINT strategies_strategy_type_check
    CHECK (strategy_type IN ('NEWS', '52W_BREAKOUT', 'MANTHAN'));

-- Index for MANTHAN strategy lookup
CREATE INDEX IF NOT EXISTS idx_strategies_manthan
    ON strategies(strategy_type) WHERE strategy_type = 'MANTHAN' AND deleted_at IS NULL;
