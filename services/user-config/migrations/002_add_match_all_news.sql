-- Migration: Add match_all_news flag to strategies table
-- This allows users to create catch-all strategies that match ALL news events
-- Example: User sends "/all" to trade on every news regardless of stock/sentiment

-- Add match_all_news column
ALTER TABLE strategies 
ADD COLUMN match_all_news BOOLEAN NOT NULL DEFAULT FALSE;

-- Add comment explaining the column
COMMENT ON COLUMN strategies.match_all_news IS 
'When true, strategy matches ALL news events (overrides stock_codes, sentiments, categories filters). Only impact_score_threshold is still checked.';

-- Create index for match_all_news strategies (for faster queries)
CREATE INDEX idx_strategies_match_all_active 
ON strategies(match_all_news, active) 
WHERE match_all_news = TRUE AND active = TRUE;

-- Update existing strategies to set match_all_news based on empty conditions in strategy_conditions table
-- If a strategy has no stock_codes, no sentiments, and impact_score = 0 or 1, it's effectively a match-all
UPDATE strategies s
SET match_all_news = TRUE
FROM strategy_conditions sc
WHERE s.strategy_id = sc.strategy_id
AND s.active = TRUE 
AND (sc.stock_codes IS NULL OR sc.stock_codes = '{}')
AND (sc.sentiments IS NULL OR sc.sentiments = '{}')
AND (sc.categories IS NULL OR sc.categories = '{}')
AND (sc.impact_score_threshold IS NULL OR sc.impact_score_threshold <= 1);
