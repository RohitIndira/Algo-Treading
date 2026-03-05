-- Add missing order fields
-- Version: 4.0
-- These columns exist in the Go Order model but were absent from migration 001.

-- Broker integration (Indira Securities / Odin)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS product_type     VARCHAR(20)     DEFAULT 'INTRADAY';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS indira_order_id  VARCHAR(50);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS indira_response  TEXT;

-- Frontend auth data (carried on signals from the rules-engine)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS bearer_token     TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS app_id           VARCHAR(100);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS source           VARCHAR(20);

-- Stop-loss configuration
ALTER TABLE orders ADD COLUMN IF NOT EXISTS stop_loss_type   VARCHAR(20)     DEFAULT 'FIXED';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS trailing_sl_pct  DECIMAL(10,4);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS highest_price    DECIMAL(15,2);

-- Bracket / square-off
ALTER TABLE orders ADD COLUMN IF NOT EXISTS target_price       DECIMAL(15,2);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_square_off_order BOOLEAN        DEFAULT false;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_orders_indira_id ON orders(indira_order_id)
    WHERE indira_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_product_type ON orders(product_type, status)
    WHERE is_paper_trade = false;
