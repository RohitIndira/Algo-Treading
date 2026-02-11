-- Migration: Create jobbing configuration tables
-- Version: 004
-- Description: Schema for jobbing strategy per-user, per-token configurations

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create jobbing_configs table for managed jobbing strategy
CREATE TABLE IF NOT EXISTS jobbing_configs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL,
    
    -- Strategy identification
    strategy_id VARCHAR(100) DEFAULT 'JOBBING' NOT NULL,
    strategy_name VARCHAR(255) DEFAULT 'Jobbing Strategy' NOT NULL,
    
    -- Stock/Token identification
    token VARCHAR(50) NOT NULL,
    symbol VARCHAR(50),
    exchange VARCHAR(20) DEFAULT 'NSE',
    
    -- Price range limits (absolute prices)
    lower_range DECIMAL(18, 4) NOT NULL CHECK (lower_range > 0),
    higher_range DECIMAL(18, 4) NOT NULL CHECK (higher_range > lower_range),
    
    -- Order placement parameters
    initial_buy_offset DECIMAL(18, 6) NOT NULL DEFAULT 0.01 CHECK (initial_buy_offset > 0),
    distance_continue DECIMAL(18, 6) NOT NULL DEFAULT 0.01 CHECK (distance_continue > 0),
    
    -- Quantity management
    quantity_per_order INTEGER NOT NULL DEFAULT 1 CHECK (quantity_per_order > 0),
    max_quantity INTEGER NOT NULL DEFAULT 10 CHECK (max_quantity >= quantity_per_order),
    
    -- Trading mode (LIVE or PAPER)
    trading_mode VARCHAR(10) DEFAULT 'LIVE' CHECK (trading_mode IN ('LIVE', 'PAPER')),
    
    -- Status tracking
    enabled BOOLEAN DEFAULT true NOT NULL,
    enabled_at TIMESTAMP WITH TIME ZONE,
    disabled_at TIMESTAMP WITH TIME ZONE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT unique_user_token_jobbing UNIQUE(user_id, token),
    CONSTRAINT check_price_range CHECK (higher_range > lower_range),
    CONSTRAINT check_quantities CHECK (max_quantity >= quantity_per_order)
);

-- Performance indexes
CREATE INDEX idx_jobbing_user_id ON jobbing_configs(user_id);
CREATE INDEX idx_jobbing_enabled_users ON jobbing_configs(user_id) WHERE enabled = true;
CREATE INDEX idx_jobbing_token ON jobbing_configs(token);
CREATE INDEX idx_jobbing_user_token ON jobbing_configs(user_id, token);
CREATE INDEX idx_jobbing_enabled_tokens ON jobbing_configs(token) WHERE enabled = true;

-- Function to auto-set enabled_at/disabled_at timestamps
CREATE OR REPLACE FUNCTION update_jobbing_enabled_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.enabled = true AND OLD.enabled = false THEN
        NEW.enabled_at = CURRENT_TIMESTAMP;
        NEW.disabled_at = NULL;
    ELSIF NEW.enabled = false AND OLD.enabled = true THEN
        NEW.disabled_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create or replace the update_updated_at_column function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update enabled timestamps
CREATE TRIGGER update_jobbing_enabled_at BEFORE UPDATE ON jobbing_configs
    FOR EACH ROW EXECUTE FUNCTION update_jobbing_enabled_timestamp();

-- Trigger to auto-update updated_at (reuse existing function)
CREATE TRIGGER update_jobbing_configs_updated_at BEFORE UPDATE ON jobbing_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE jobbing_configs IS 'Per-user, per-token jobbing strategy configurations';
COMMENT ON COLUMN jobbing_configs.lower_range IS 'Minimum price threshold for jobbing strategy';
COMMENT ON COLUMN jobbing_configs.higher_range IS 'Maximum price threshold for jobbing strategy';
COMMENT ON COLUMN jobbing_configs.initial_buy_offset IS 'Initial offset from LTP for first buy order (Initial B)';
COMMENT ON COLUMN jobbing_configs.distance_continue IS 'Distance between consecutive orders (Distance Continue S)';
COMMENT ON COLUMN jobbing_configs.quantity_per_order IS 'Quantity per individual order';
COMMENT ON COLUMN jobbing_configs.max_quantity IS 'Maximum total quantity allowed for this token';
COMMENT ON COLUMN jobbing_configs.trading_mode IS 'LIVE for real orders, PAPER for simulation';

-- Insert example configuration (commented out for production)
-- INSERT INTO jobbing_configs (
--     user_id, token, symbol, exchange,
--     lower_range, higher_range,
--     initial_buy_offset, distance_continue,
--     quantity_per_order, max_quantity,
--     trading_mode, enabled
-- ) VALUES (
--     'ISPL19027', '30274', 'SILVERCASE', 'NSE',
--     10.00, 15.00,
--     0.01, 0.01,
--     1, 10,
--     'PAPER', true
-- );
