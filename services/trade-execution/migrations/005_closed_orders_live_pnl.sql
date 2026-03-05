-- Closed orders PnL tracking for live trades + dashboard support
-- Version: 5.0
-- Created: 2026-03-04

-- Live trade exit tracking (mirrors paper_exit_price / paper_pnl for live orders)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS live_exit_price DECIMAL(15,2);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS live_pnl        DECIMAL(15,2);

-- Index for efficiently querying closed paper positions (paper_exit_price set = position closed)
CREATE INDEX IF NOT EXISTS idx_orders_paper_closed ON orders(user_id, is_paper_trade, paper_exit_price)
    WHERE is_paper_trade = true AND paper_exit_price IS NOT NULL;

-- Index for efficiently querying closed live positions (live_exit_price set = position closed)
CREATE INDEX IF NOT EXISTS idx_orders_live_closed ON orders(user_id, is_paper_trade, live_exit_price)
    WHERE is_paper_trade = false AND live_exit_price IS NOT NULL;

-- Index for dashboard stats: open paper positions
CREATE INDEX IF NOT EXISTS idx_orders_paper_open ON orders(user_id, is_paper_trade, status)
    WHERE is_paper_trade = true AND status = 'FILLED';

-- Index for dashboard stats: open live positions
CREATE INDEX IF NOT EXISTS idx_orders_live_open ON orders(user_id, is_paper_trade, status)
    WHERE is_paper_trade = false AND status IN ('FILLED', 'PARTIALLY_FILLED');
