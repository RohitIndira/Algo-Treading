-- Migration: Create strategies and related tables
-- Version: 001
-- Description: Initial schema for user trading strategies

-- Create extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Strategies table
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    description TEXT,
    active BOOLEAN DEFAULT false,
    version INTEGER DEFAULT 1,
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_strategies_user_id ON strategies(user_id);
CREATE INDEX idx_strategies_active ON strategies(active);
CREATE INDEX idx_strategies_user_active ON strategies(user_id, active);
CREATE INDEX idx_strategy_conditions_strategy_id ON strategy_conditions(strategy_id);
CREATE INDEX idx_trade_configs_strategy_id ON trade_configs(strategy_id);
CREATE INDEX idx_risk_limits_strategy_id ON risk_limits(strategy_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update updated_at
CREATE TRIGGER update_strategies_updated_at BEFORE UPDATE ON strategies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments
COMMENT ON TABLE strategies IS 'User trading strategies configuration';
COMMENT ON TABLE strategy_conditions IS 'Conditions/filters for strategy triggers';
COMMENT ON TABLE trade_configs IS 'Trade execution configuration for strategies';
COMMENT ON TABLE risk_limits IS 'Risk management limits for strategies';
