-- ============================================================================
-- Algo Trading System - Complete Database Schema
-- Database: trading_system
-- Description: Creates all required tables, indexes, triggers, and functions
-- ============================================================================

-- Connect to the trading_system database
\c trading_system;

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- USER LOGIN SERVICE SCHEMA
-- ============================================================================

-- User Sessions Table
CREATE TABLE IF NOT EXISTS user_sessions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(255) UNIQUE NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT,
    broadcast_token TEXT,
    
    -- Authentication details
    login_type VARCHAR(20) NOT NULL, -- PASSWORD, TOKEN, MPIN, TP_TOKEN
    second_auth_type VARCHAR(20), -- OTP, TOTP, FINGERPRINT, REGISTER
    source VARCHAR(50) DEFAULT 'MOBILEAPI',
    
    -- User details from ODIN response
    user_name VARCHAR(255),
    email VARCHAR(255),
    mobile_no VARCHAR(20),
    user_code VARCHAR(50),
    group_id VARCHAR(50),
    
    -- Session metadata
    exchanges TEXT[], -- Array of allowed exchanges
    product_types TEXT[], -- Array of allowed product types
    
    -- Device information
    device_udid VARCHAR(255),
    device_model VARCHAR(100),
    device_platform VARCHAR(50),
    ip_address VARCHAR(50),
    
    -- Session status
    is_active BOOLEAN DEFAULT TRUE,
    login_time TIMESTAMP NOT NULL DEFAULT NOW(),
    last_activity TIMESTAMP NOT NULL DEFAULT NOW(),
    logout_time TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    
    -- ODIN API details
    odin_api_url VARCHAR(500),
    odin_oc_token VARCHAR(255),
    
    -- Additional ODIN response data (JSON)
    other_details JSONB,
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- User Authentication Credentials Table
CREATE TABLE IF NOT EXISTS user_credentials (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL UNIQUE,
    indira_user_id VARCHAR(50),
    indira_app_id VARCHAR(100),
    indira_source VARCHAR(10),
    indira_bearer_token TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_user
        FOREIGN KEY(user_id) 
        REFERENCES users(id)
        ON DELETE CASCADE
);

-- Login History Table
CREATE TABLE IF NOT EXISTS login_history (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(255),
    
    -- Login attempt details
    login_type VARCHAR(20),
    second_auth_type VARCHAR(20),
    status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, ERROR
    error_message TEXT,
    
    -- Device/Network info
    device_udid VARCHAR(255),
    device_platform VARCHAR(50),
    ip_address VARCHAR(50),
    user_agent TEXT,
    
    -- Timestamps
    attempt_time TIMESTAMP NOT NULL DEFAULT NOW(),
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- USER CONFIG SERVICE SCHEMA
-- ============================================================================

-- Strategies table
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    description TEXT,
    active BOOLEAN DEFAULT false,
    version INTEGER DEFAULT 1,
    match_all_news BOOLEAN NOT NULL DEFAULT FALSE,
    -- Frontend authentication data (stored with strategy for order execution)
    bearer_token TEXT,
    app_id VARCHAR(100),
    source VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_user_strategy_name UNIQUE(user_id, strategy_name)
);

-- Strategy conditions table
CREATE TABLE IF NOT EXISTS strategy_conditions (
    condition_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    impact_score_threshold INTEGER CHECK (impact_score_threshold BETWEEN 1 AND 10),
    sentiments VARCHAR[] DEFAULT '{}',
    categories VARCHAR[] DEFAULT '{}',
    stock_codes BIGINT[] DEFAULT '{}',
    price_range_min DECIMAL(15,2),
    price_range_max DECIMAL(15,2),
    volume_threshold BIGINT,
    pct_change_threshold DECIMAL(10,2),
    exchanges VARCHAR[] DEFAULT '{}',
    min_market_cap DECIMAL(15,2),
    max_market_cap DECIMAL(15,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Trade configuration table
CREATE TABLE IF NOT EXISTS trade_configs (
    trade_config_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    order_type VARCHAR(50) NOT NULL DEFAULT 'MARKET',
    quantity INTEGER NOT NULL,
    max_position_size DECIMAL(15,2),
    stop_loss_pct DECIMAL(10,2),
    take_profit_pct DECIMAL(10,2),
    exchange VARCHAR(50) NOT NULL,
    order_side VARCHAR(50) NOT NULL DEFAULT 'BUY',
    limit_price DECIMAL(15,2),
    validity VARCHAR(50) DEFAULT 'DAY',
    stop_loss_type VARCHAR(20) DEFAULT 'FIXED',
    trailing_sl_pct DECIMAL(10,2),
    product_type VARCHAR(20) DEFAULT 'INTRADAY',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Risk limits table
CREATE TABLE IF NOT EXISTS risk_limits (
    risk_limit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    max_daily_trades INTEGER,
    max_loss_per_day DECIMAL(15,2),
    position_sizing VARCHAR(50) DEFAULT 'FIXED',
    max_portfolio_exposure_pct DECIMAL(10,2),
    max_per_trade_risk DECIMAL(15,2),
    enable_risk_checks BOOLEAN DEFAULT true,
    enable_auto_square_off BOOLEAN DEFAULT false,
    auto_square_off_time VARCHAR(10) DEFAULT '15:05',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TRADE EXECUTION SERVICE SCHEMA
-- ============================================================================

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

-- ============================================================================
-- RULES ENGINE SERVICE SCHEMA
-- ============================================================================

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

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- User Sessions Indexes
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_session_id ON user_sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_is_active ON user_sessions(is_active);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_user_sessions_login_time ON user_sessions(login_time);

-- User Credentials Indexes
CREATE INDEX IF NOT EXISTS idx_user_credentials_user_id ON user_credentials(user_id);
CREATE INDEX IF NOT EXISTS idx_user_credentials_is_active ON user_credentials(is_active);

-- Login History Indexes
CREATE INDEX IF NOT EXISTS idx_login_history_user_id ON login_history(user_id);
CREATE INDEX IF NOT EXISTS idx_login_history_status ON login_history(status);
CREATE INDEX IF NOT EXISTS idx_login_history_attempt_time ON login_history(attempt_time DESC);

-- Strategies Indexes
CREATE INDEX IF NOT EXISTS idx_strategies_user_id ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_strategies_active ON strategies(active);
CREATE INDEX IF NOT EXISTS idx_strategies_user_active ON strategies(user_id, active);
CREATE INDEX IF NOT EXISTS idx_strategies_match_all_active ON strategies(match_all_news, active) WHERE match_all_news = TRUE AND active = TRUE;

-- Strategy Conditions Indexes
CREATE INDEX IF NOT EXISTS idx_strategy_conditions_strategy_id ON strategy_conditions(strategy_id);

-- Trade Configs Indexes
CREATE INDEX IF NOT EXISTS idx_trade_configs_strategy_id ON trade_configs(strategy_id);

-- Risk Limits Indexes
CREATE INDEX IF NOT EXISTS idx_risk_limits_strategy_id ON risk_limits(strategy_id);

-- Orders Indexes
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_event_id ON orders(event_id);
CREATE INDEX IF NOT EXISTS idx_orders_odin_id ON orders(odin_order_id) WHERE odin_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_stock_code ON orders(stock_code);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC);

-- Execution Events Indexes
CREATE INDEX IF NOT EXISTS idx_execution_events_order_id ON execution_events(order_id);
CREATE INDEX IF NOT EXISTS idx_execution_events_created_at ON execution_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_events_type ON execution_events(event_type);

-- Trade Signals Indexes
CREATE INDEX IF NOT EXISTS idx_trade_signals_user_id ON trade_signals(user_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_strategy_id ON trade_signals(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_status ON trade_signals(status);
CREATE INDEX IF NOT EXISTS idx_trade_signals_stock_code ON trade_signals(stock_code);
CREATE INDEX IF NOT EXISTS idx_trade_signals_created_at ON trade_signals(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_signals_event_id ON trade_signals(event_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_user_status ON trade_signals(user_id, status, created_at DESC);

-- ============================================================================
-- FUNCTIONS AND TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp (generic)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to relevant tables
DROP TRIGGER IF EXISTS update_strategies_updated_at ON strategies;
CREATE TRIGGER update_strategies_updated_at BEFORE UPDATE ON strategies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_orders_updated_at ON orders;
CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS trade_signals_updated_at ON trade_signals;
CREATE TRIGGER trade_signals_updated_at BEFORE UPDATE ON trade_signals
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_user_sessions_updated_at ON user_sessions;
CREATE TRIGGER update_user_sessions_updated_at BEFORE UPDATE ON user_sessions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_user_credentials_updated_at ON user_credentials;
CREATE TRIGGER update_user_credentials_updated_at BEFORE UPDATE ON user_credentials
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

-- User Login Service
COMMENT ON TABLE user_sessions IS 'Active user sessions with authentication tokens';
-- COMMENT ON TABLE user_credentials IS 'User authentication credentials and ODIN API configuration';
COMMENT ON TABLE login_history IS 'Login attempt history for analytics and security';

-- Create an index on user_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_user_credentials_user_id ON user_credentials(user_id);

-- User Config Service
COMMENT ON TABLE strategies IS 'User trading strategies configuration';
COMMENT ON TABLE strategy_conditions IS 'Conditions/filters for strategy triggers';
COMMENT ON TABLE trade_configs IS 'Trade execution configuration for strategies';
COMMENT ON TABLE risk_limits IS 'Risk management limits for strategies';
COMMENT ON COLUMN strategies.match_all_news IS 'When true, strategy matches ALL news events (overrides filters). Only impact_score_threshold is checked.';

-- Trade Execution Service
COMMENT ON TABLE orders IS 'Order records submitted to broker';
COMMENT ON TABLE execution_events IS 'Event log for order lifecycle tracking';

-- Rules Engine Service
COMMENT ON TABLE trade_signals IS 'Tracks all generated trade signals from rules engine';
COMMENT ON COLUMN trade_signals.status IS 'Order execution status: PENDING, SENT, EXECUTED, FAILED, CANCELLED';
COMMENT ON COLUMN trade_signals.match_score IS 'Strategy match score (0-100)';
COMMENT ON COLUMN trade_signals.broker_order_id IS 'Order ID from broker after execution';

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

\echo '============================================================================'
\echo 'Schema creation completed successfully!'
\echo 'Database: trading_system'
\echo ''
\echo 'Tables created:'
\echo '  User Login Service: user_sessions, user_credentials, login_history'
\echo '  User Config Service: strategies, strategy_conditions, trade_configs, risk_limits'
\echo '  Trade Execution Service: orders, execution_events'
\echo '  Rules Engine Service: trade_signals'
\echo ''
\echo 'All indexes, triggers, and functions have been created.'
\echo '============================================================================'
