-- Migration 010: Add 52W Breakout strategy support (Manthan strategy)
--
-- Adds strategy_type to strategies table and 52W-specific fields to trade_configs.
-- The 52W strategy is simple: user provides investment + positions,
-- system auto-trades based on 52W breakouts with EMA-based position sizing.
--
-- User inputs: strategy_name, total_capital, max_positions, stop_loss_pct, take_profit_pct
-- System handles: which stocks (52W breakout), how much (EMA score), when (real-time)

-- Add strategy type to strategies table
-- 'NEWS' = existing news-based strategy
-- '52W_BREAKOUT' = new 52-week breakout strategy
ALTER TABLE strategies
    ADD COLUMN IF NOT EXISTS strategy_type VARCHAR(20) DEFAULT 'NEWS'
    CHECK (strategy_type IN ('NEWS', '52W_BREAKOUT'));

-- Add 52W-specific fields to trade_configs
-- total_capital: how much money the user wants to deploy (default ₹1,00,000)
-- max_positions: maximum number of stocks to hold at once (default 25)
-- per_stock_amount: auto-calculated (total_capital / max_positions)
-- position_sizing_mode: how to determine quantity per trade
--   'FIXED_QTY' = existing behavior (fixed number of shares)
--   'EMA_ALLOCATION' = Manthan strategy (EMA score determines allocation %)
ALTER TABLE trade_configs
    ADD COLUMN IF NOT EXISTS position_sizing_mode VARCHAR(20) DEFAULT 'FIXED_QTY'
    CHECK (position_sizing_mode IN ('FIXED_QTY', 'EMA_ALLOCATION'));

ALTER TABLE trade_configs
    ADD COLUMN IF NOT EXISTS total_capital DECIMAL DEFAULT 100000;

ALTER TABLE trade_configs
    ADD COLUMN IF NOT EXISTS max_positions INTEGER DEFAULT 25;

ALTER TABLE trade_configs
    ADD COLUMN IF NOT EXISTS per_stock_amount DECIMAL;

-- Index for quick lookup of 52W strategies
CREATE INDEX IF NOT EXISTS idx_strategies_type ON strategies(strategy_type)
    WHERE strategy_type = '52W_BREAKOUT';
