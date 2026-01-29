-- Migration: Create paper_positions table for paper trading
-- Version: 003
-- Description: Store paper trading positions with stop loss, take profit, and PnL tracking

CREATE TABLE IF NOT EXISTS paper_positions (
    position_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    
    -- Stock information
    stock_code BIGINT NOT NULL,
    token BIGINT NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    
    -- Position details
    quantity INT NOT NULL,
    entry_price DECIMAL(15,2) NOT NULL,
    current_price DECIMAL(15,2) NOT NULL,
    
    -- Stop loss and take profit
    stop_loss DECIMAL(15,2),
    take_profit DECIMAL(15,2),
    
    -- PnL tracking
    unrealized_pnl DECIMAL(15,2) DEFAULT 0,
    unrealized_pnl_pct DECIMAL(10,4) DEFAULT 0,
    
    -- Position status
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    -- OPEN, CLOSED_SL (stopped out), CLOSED_TP (take profit), CLOSED_MANUAL
    
    -- Order references
    entry_order_id UUID NOT NULL,
    exit_order_id UUID,
    
    -- Timestamps
    opened_at TIMESTAMP DEFAULT NOW(),
    closed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT fk_entry_order FOREIGN KEY (entry_order_id) REFERENCES orders(order_id),
    CONSTRAINT check_quantity_positive CHECK (quantity > 0),
    CONSTRAINT check_entry_price_positive CHECK (entry_price > 0)
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_paper_positions_user_id ON paper_positions(user_id);
CREATE INDEX IF NOT EXISTS idx_paper_positions_strategy_id ON paper_positions(strategy_id);
CREATE INDEX IF NOT EXISTS idx_paper_positions_status ON paper_positions(status);
CREATE INDEX IF NOT EXISTS idx_paper_positions_user_status ON paper_positions(user_id, status);
CREATE INDEX IF NOT EXISTS idx_paper_positions_token ON paper_positions(token);

-- Create table for realized PnL tracking
CREATE TABLE IF NOT EXISTS paper_pnl_history (
    pnl_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    position_id UUID NOT NULL,
    
    -- Stock information
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    
    -- Trade details
    quantity INT NOT NULL,
    entry_price DECIMAL(15,2) NOT NULL,
    exit_price DECIMAL(15,2) NOT NULL,
    
    -- PnL calculation
    realized_pnl DECIMAL(15,2) NOT NULL,
    realized_pnl_pct DECIMAL(10,4) NOT NULL,
    
    -- Exit reason
    exit_reason VARCHAR(50) NOT NULL,
    -- STOP_LOSS, TAKE_PROFIT, MANUAL, STRATEGY_DISABLED
    
    -- Timestamps
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT fk_position FOREIGN KEY (position_id) REFERENCES paper_positions(position_id)
);

-- Create indexes for PnL history
CREATE INDEX IF NOT EXISTS idx_paper_pnl_user_id ON paper_pnl_history(user_id);
CREATE INDEX IF NOT EXISTS idx_paper_pnl_strategy_id ON paper_pnl_history(strategy_id);
CREATE INDEX IF NOT EXISTS idx_paper_pnl_exit_time ON paper_pnl_history(exit_time);

-- Create view for easy querying of user's daily PnL
CREATE OR REPLACE VIEW user_daily_paper_pnl AS
SELECT 
    user_id,
    strategy_id,
    DATE(exit_time) as trade_date,
    COUNT(*) as num_trades,
    SUM(realized_pnl) as daily_pnl,
    AVG(realized_pnl_pct) as avg_pnl_pct,
    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
    SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades
FROM paper_pnl_history
GROUP BY user_id, strategy_id, DATE(exit_time);

COMMENT ON TABLE paper_positions IS 'Stores open and closed paper trading positions';
COMMENT ON TABLE paper_pnl_history IS 'Tracks realized PnL for closed paper positions';
COMMENT ON COLUMN paper_positions.status IS 'Position status: OPEN, CLOSED_SL, CLOSED_TP, CLOSED_MANUAL';
COMMENT ON COLUMN paper_pnl_history.exit_reason IS 'Why position was closed: STOP_LOSS, TAKE_PROFIT, MANUAL, STRATEGY_DISABLED';
