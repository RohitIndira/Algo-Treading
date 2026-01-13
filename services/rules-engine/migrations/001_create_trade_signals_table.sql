-- Create trade_signals table to track all generated orders
CREATE TABLE IF NOT EXISTS trade_signals (
    -- Primary identifiers
    signal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id VARCHAR(255) NOT NULL UNIQUE,
    
    -- User and strategy info
    user_id VARCHAR(255) NOT NULL,
    strategy_id UUID NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    
    -- Event info
    event_id VARCHAR(255) NOT NULL,
    
    -- Stock details
    stock_code BIGINT NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    
    -- Order details
    order_type VARCHAR(20) NOT NULL, -- MARKET, LIMIT
    order_side VARCHAR(10) NOT NULL, -- BUY, SELL
    quantity INTEGER NOT NULL,
    price DECIMAL(15, 2) NOT NULL,
    stop_loss DECIMAL(15, 2),
    take_profit DECIMAL(15, 2),
    
    -- Matching info
    match_score DECIMAL(5, 2) NOT NULL,
    impact_score INTEGER NOT NULL,
    sentiment VARCHAR(50),
    news_category VARCHAR(255),
    
    -- Execution tracking
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING, SENT, EXECUTED, FAILED, CANCELLED
    execution_price DECIMAL(15, 2),
    execution_time TIMESTAMP,
    broker_order_id VARCHAR(255),
    error_message TEXT,
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    metadata JSONB
);

-- Create indexes for common queries
CREATE INDEX idx_trade_signals_user_id ON trade_signals(user_id);
CREATE INDEX idx_trade_signals_strategy_id ON trade_signals(strategy_id);
CREATE INDEX idx_trade_signals_status ON trade_signals(status);
CREATE INDEX idx_trade_signals_stock_code ON trade_signals(stock_code);
CREATE INDEX idx_trade_signals_created_at ON trade_signals(created_at DESC);
CREATE INDEX idx_trade_signals_event_id ON trade_signals(event_id);

-- Create composite index for user + status queries
CREATE INDEX idx_trade_signals_user_status ON trade_signals(user_id, status, created_at DESC);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_trade_signals_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trade_signals_updated_at
    BEFORE UPDATE ON trade_signals
    FOR EACH ROW
    EXECUTE FUNCTION update_trade_signals_updated_at();

-- Add comments
COMMENT ON TABLE trade_signals IS 'Tracks all generated trade signals from rules engine';
COMMENT ON COLUMN trade_signals.status IS 'Order execution status: PENDING, SENT, EXECUTED, FAILED, CANCELLED';
COMMENT ON COLUMN trade_signals.match_score IS 'Strategy match score (0-100)';
COMMENT ON COLUMN trade_signals.broker_order_id IS 'Order ID from broker after execution';