-- Paper Trading Migration
-- Version: 3.0
-- Created: 2026-03-02

-- Add paper trading columns to orders table
ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_paper_trade BOOLEAN DEFAULT false;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS trading_mode VARCHAR(10) DEFAULT 'LIVE';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paper_exit_price DECIMAL(15,2);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paper_pnl DECIMAL(15,2);

-- Index for efficiently querying active paper positions
CREATE INDEX IF NOT EXISTS idx_orders_paper_trade ON orders(user_id, is_paper_trade, status)
    WHERE is_paper_trade = true;

CREATE INDEX IF NOT EXISTS idx_orders_paper_symbol ON orders(symbol, is_paper_trade, status)
    WHERE is_paper_trade = true;
