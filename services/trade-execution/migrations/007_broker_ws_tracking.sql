-- Store raw broker WebSocket data for every order status update.
-- Version: 7.0
-- Created: 2026-03-18

-- Raw broker status string exactly as received from WS (e.g. "EXECUTED", "PENDING", "CANCELLED")
ALTER TABLE orders ADD COLUMN IF NOT EXISTS broker_status VARCHAR(50);

-- Full raw JSON payload from the broker WebSocket for the latest update
ALTER TABLE orders ADD COLUMN IF NOT EXISTS broker_ws_data JSONB;

-- Exchange order number from the broker (OrderNumber field in WS, e.g. "1100000103825867")
ALTER TABLE orders ADD COLUMN IF NOT EXISTS exchange_order_number VARCHAR(50);

-- Index for looking up orders by exchange order number
CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_number ON orders(exchange_order_number)
    WHERE exchange_order_number IS NOT NULL;
