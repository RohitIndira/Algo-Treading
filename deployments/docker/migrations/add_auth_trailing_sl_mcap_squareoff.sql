-- Migration: Add authentication fields, trailing stop loss, market cap filters, and auto square-off
-- Date: 2025-12-12
-- Description: Comprehensive update to support bearer token auth, trailing SL, market cap filters, and auto square-off

-- ===========================================================================
-- 1. USER_CONFIG SERVICE: Update strategies table
-- ===========================================================================

-- Add authentication fields to strategy_conditions table
ALTER TABLE IF EXISTS strategy_conditions 
ADD COLUMN IF NOT EXISTS min_market_cap DECIMAL(15,2),
ADD COLUMN IF NOT EXISTS max_market_cap DECIMAL(15,2);

COMMENT ON COLUMN strategy_conditions.min_market_cap IS 'Minimum market cap filter in crores';
COMMENT ON COLUMN strategy_conditions.max_market_cap IS 'Maximum market cap filter in crores';

-- Add stop loss type and trailing configuration to trade_config table
ALTER TABLE IF EXISTS trade_config
ADD COLUMN IF NOT EXISTS stop_loss_type VARCHAR(20) DEFAULT 'FIXED',
ADD COLUMN IF NOT EXISTS trailing_sl_pct DECIMAL(5,2) DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS product_type VARCHAR(20) DEFAULT 'INTRADAY';

COMMENT ON COLUMN trade_config.stop_loss_type IS 'Stop loss type: FIXED or TRAILING';
COMMENT ON COLUMN trade_config.trailing_sl_pct IS 'Trailing stop loss percentage (only for TRAILING type)';
COMMENT ON COLUMN trade_config.product_type IS 'Product type: INTRADAY, DELIVERY, CASH';

-- Add auto square-off configuration to risk_limits table
ALTER TABLE IF EXISTS risk_limits
ADD COLUMN IF NOT EXISTS enable_auto_square_off BOOLEAN DEFAULT true,
ADD COLUMN IF NOT EXISTS auto_square_off_time VARCHAR(10) DEFAULT '15:05';

COMMENT ON COLUMN risk_limits.enable_auto_square_off IS 'Enable automatic square-off at market close';
COMMENT ON COLUMN risk_limits.auto_square_off_time IS 'Auto square-off time in HH:MM format (e.g., 15:05 for 3:05 PM)';

-- ===========================================================================
-- 2. USER CREDENTIALS: Create new table for storing user authentication data
-- ===========================================================================

CREATE TABLE IF NOT EXISTS user_credentials (
    user_id VARCHAR(50) PRIMARY KEY,
    bearer_token TEXT NOT NULL,
    app_id VARCHAR(100) NOT NULL,
    source VARCHAR(20) NOT NULL, -- IOS, AND, WEB
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP, -- Optional: track token expiry
    
    CONSTRAINT chk_source CHECK (source IN ('IOS', 'AND', 'WEB'))
);

CREATE INDEX IF NOT EXISTS idx_user_credentials_user_id ON user_credentials(user_id);
CREATE INDEX IF NOT EXISTS idx_user_credentials_updated_at ON user_credentials(updated_at);

COMMENT ON TABLE user_credentials IS 'Stores user authentication credentials for broker API integration';
COMMENT ON COLUMN user_credentials.bearer_token IS 'JWT bearer token from frontend';
COMMENT ON COLUMN user_credentials.app_id IS 'Application ID from frontend';
COMMENT ON COLUMN user_credentials.source IS 'Source platform: IOS, AND, WEB';

-- ===========================================================================
-- 3. TRADE EXECUTION SERVICE: Update orders table
-- ===========================================================================

-- Add trailing stop loss fields to orders table
ALTER TABLE IF EXISTS orders
ADD COLUMN IF NOT EXISTS stop_loss_type VARCHAR(20) DEFAULT 'FIXED',
ADD COLUMN IF NOT EXISTS trailing_sl_pct DECIMAL(5,2) DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS highest_price DECIMAL(15,2), -- Track highest price for trailing SL
ADD COLUMN IF NOT EXISTS is_square_off_order BOOLEAN DEFAULT false; -- Mark square-off orders

COMMENT ON COLUMN orders.stop_loss_type IS 'Stop loss type: FIXED or TRAILING';
COMMENT ON COLUMN orders.trailing_sl_pct IS 'Trailing stop loss percentage';
COMMENT ON COLUMN orders.highest_price IS 'Highest price reached (for trailing stop loss calculation)';
COMMENT ON COLUMN orders.is_square_off_order IS 'Flag indicating if this is an auto square-off order';

-- Create index for efficient trailing SL monitoring
CREATE INDEX IF NOT EXISTS idx_orders_trailing_sl ON orders(status, stop_loss_type) 
WHERE stop_loss_type = 'TRAILING' AND status IN ('FILLED', 'PARTIALLY_FILLED');

-- Create index for auto square-off queries
CREATE INDEX IF NOT EXISTS idx_orders_square_off ON orders(product_type, status, created_at)
WHERE product_type = 'INTRADAY' AND status IN ('FILLED', 'PARTIALLY_FILLED');

-- ===========================================================================
-- 4. Update existing constraints and add new validations
-- ===========================================================================

-- Add constraint for stop loss type
ALTER TABLE IF EXISTS trade_config 
DROP CONSTRAINT IF EXISTS chk_stop_loss_type,
ADD CONSTRAINT chk_stop_loss_type CHECK (stop_loss_type IN ('FIXED', 'TRAILING'));

ALTER TABLE IF EXISTS orders
DROP CONSTRAINT IF EXISTS chk_stop_loss_type,
ADD CONSTRAINT chk_stop_loss_type CHECK (stop_loss_type IN ('FIXED', 'TRAILING'));

-- Add constraint for product type
ALTER TABLE IF EXISTS trade_config
DROP CONSTRAINT IF EXISTS chk_product_type,
ADD CONSTRAINT chk_product_type CHECK (product_type IN ('INTRADAY', 'DELIVERY', 'CASH'));

ALTER TABLE IF EXISTS orders
DROP CONSTRAINT IF EXISTS chk_product_type,
ADD CONSTRAINT chk_product_type CHECK (product_type IN ('INTRADAY', 'DELIVERY', 'CASH'));

-- ===========================================================================
-- 5. Create helper function for auto square-off time validation
-- ===========================================================================

CREATE OR REPLACE FUNCTION validate_square_off_time(time_str VARCHAR(10))
RETURNS BOOLEAN AS $$
DECLARE
    hour INT;
    minute INT;
BEGIN
    -- Parse time string in HH:MM format
    hour := CAST(SPLIT_PART(time_str, ':', 1) AS INT);
    minute := CAST(SPLIT_PART(time_str, ':', 2) AS INT);
    
    -- Validate hour (9-15) and minute (0-59)
    -- Market hours are typically 9:15 AM to 3:30 PM
    RETURN hour >= 9 AND hour <= 15 AND minute >= 0 AND minute <= 59;
EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- Add validation constraint for auto square-off time
ALTER TABLE IF EXISTS risk_limits
DROP CONSTRAINT IF EXISTS chk_auto_square_off_time,
ADD CONSTRAINT chk_auto_square_off_time 
CHECK (auto_square_off_time IS NULL OR validate_square_off_time(auto_square_off_time));

-- ===========================================================================
-- 6. Create view for monitoring orders requiring attention
-- ===========================================================================

CREATE OR REPLACE VIEW orders_requiring_monitoring AS
SELECT 
    o.order_id,
    o.user_id,
    o.strategy_id,
    o.symbol,
    o.order_side,
    o.quantity,
    o.filled_quantity,
    o.filled_price,
    o.stop_loss,
    o.stop_loss_type,
    o.trailing_sl_pct,
    o.highest_price,
    o.product_type,
    o.status,
    o.created_at,
    CASE 
        WHEN o.stop_loss_type = 'TRAILING' AND o.status IN ('FILLED', 'PARTIALLY_FILLED') 
        THEN 'TRAILING_SL_ACTIVE'
        WHEN o.product_type = 'INTRADAY' AND o.status IN ('FILLED', 'PARTIALLY_FILLED')
        THEN 'REQUIRES_SQUARE_OFF'
        ELSE 'NO_ACTION'
    END as monitoring_status
FROM orders o
WHERE 
    (o.stop_loss_type = 'TRAILING' AND o.status IN ('FILLED', 'PARTIALLY_FILLED'))
    OR 
    (o.product_type = 'INTRADAY' AND o.status IN ('FILLED', 'PARTIALLY_FILLED') AND o.is_square_off_order = false);

COMMENT ON VIEW orders_requiring_monitoring IS 'Orders that require monitoring for trailing SL or auto square-off';

-- ===========================================================================
-- 7. Update triggers for automatic timestamp updates
-- ===========================================================================

-- Update user_credentials updated_at trigger
CREATE OR REPLACE FUNCTION update_user_credentials_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_user_credentials_timestamp ON user_credentials;
CREATE TRIGGER trg_update_user_credentials_timestamp
    BEFORE UPDATE ON user_credentials
    FOR EACH ROW
    EXECUTE FUNCTION update_user_credentials_timestamp();

-- ===========================================================================
-- 8. Grant permissions (adjust as needed for your setup)
-- ===========================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON user_credentials TO trade_execution_user;
GRANT SELECT ON orders_requiring_monitoring TO trade_execution_user;

-- ===========================================================================
-- 9. Sample data for testing (optional - comment out in production)
-- ===========================================================================

-- Example: Insert default user credentials (replace with actual user data)
/*
INSERT INTO user_credentials (user_id, bearer_token, app_id, source)
VALUES 
    ('TEST_USER_001', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...', 'APP_12345', 'WEB')
ON CONFLICT (user_id) DO UPDATE 
SET 
    bearer_token = EXCLUDED.bearer_token,
    app_id = EXCLUDED.app_id,
    source = EXCLUDED.source,
    updated_at = CURRENT_TIMESTAMP;
*/

-- ===========================================================================
-- Migration complete
-- ===========================================================================
