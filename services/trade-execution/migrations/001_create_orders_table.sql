-- Trade Execution Service Database Schema
-- Version: 1.0
-- Created: 2025-11-13

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    event_id UUID NOT NULL,
    
    -- Stock information
    stock_code BIGINT NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    
    -- Order details
    order_type VARCHAR(10) NOT NULL,
    order_side VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(15,2),
    
    -- Stop loss and take profit
    stop_loss DECIMAL(15,2),
    take_profit DECIMAL(15,2),
    
    -- Order validity
    validity VARCHAR(10) DEFAULT 'DAY',
    
    -- Order status
    status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    
    -- Odin API integration
    odin_order_id VARCHAR(50),
    odin_response TEXT,
    
    -- Execution details
    filled_quantity INT DEFAULT 0,
    filled_price DECIMAL(15,2),
    commission DECIMAL(10,2),
    total_cost DECIMAL(15,2),
    
    -- Risk approval
    risk_approved BOOLEAN DEFAULT false,
    risk_score DECIMAL(5,2),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    submitted_at TIMESTAMP,
    executed_at TIMESTAMP,
    
    -- Error handling
    error_message TEXT,
    rejection_reason TEXT,
    retry_count INT DEFAULT 0,
    
    -- Constraints
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_price_positive CHECK (price IS NULL OR price > 0),
    CONSTRAINT chk_filled_quantity CHECK (filled_quantity >= 0 AND filled_quantity <= quantity)
);

-- Create execution_events table
CREATE TABLE IF NOT EXISTS execution_events (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(20) NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_event_id ON orders(event_id);
CREATE INDEX IF NOT EXISTS idx_orders_odin_id ON orders(odin_order_id) WHERE odin_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_stock_code ON orders(stock_code);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC);

-- Indexes for execution_events
CREATE INDEX IF NOT EXISTS idx_execution_events_order_id ON execution_events(order_id);
CREATE INDEX IF NOT EXISTS idx_execution_events_created_at ON execution_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_events_type ON execution_events(event_type);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert initial data for testing (optional)
-- Uncomment if you want test data
-- INSERT INTO orders (user_id, strategy_id, event_id, stock_code, exchange, symbol, order_type, order_side, quantity, price, status, risk_approved)
-- VALUES ('test-user-123', 'test-strategy-1', gen_random_uuid(), 517170, 'BSE', 'EDVENSWA', 'MARKET', 'BUY', 10, 47.75, 'RECEIVED', true);

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE ON orders TO trade_exec_svc;
-- GRANT SELECT, INSERT ON execution_events TO trade_exec_svc;
-- GRANT USAGE, SELECT ON SEQUENCE execution_events_id_seq TO trade_exec_svc;