-- Migration: Add Indira API fields to orders table
-- Description: Adds fields for Indira Securities API integration and frontend auth data
-- Date: 2025-12-12

-- Add Indira-specific columns to orders table
ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS indira_order_id VARCHAR(50);
ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS indira_response TEXT;
ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS product_type VARCHAR(20) DEFAULT 'INTRADAY';
ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS target_price DECIMAL(18, 2);

-- Add frontend auth columns (these are temporary and not persisted long-term for security)
ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS bearer_token TEXT;
ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS app_id VARCHAR(100);
ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS source VARCHAR(20);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_orders_indira_order_id ON orders(indira_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_product_type ON orders(product_type);

-- Add comments for documentation
COMMENT ON COLUMN orders.indira_order_id IS 'Order ID from Indira Securities API';
COMMENT ON COLUMN orders.indira_response IS 'Raw response from Indira Securities API';
COMMENT ON COLUMN orders.product_type IS 'Product type: INTRADAY, DELIVERY, CASH, MTF';
COMMENT ON COLUMN orders.target_price IS 'Target price for bracket orders';
COMMENT ON COLUMN orders.bearer_token IS 'JWT bearer token from frontend (temporary, for API calls only)';
COMMENT ON COLUMN orders.app_id IS 'Application ID from frontend';
COMMENT ON COLUMN orders.source IS 'Source platform: IOS, AND, WEB';

-- Note: The bearer_token, app_id, and source fields are for temporary storage during order processing.
-- They should not be used for long-term authentication and should be cleared or encrypted after use.
