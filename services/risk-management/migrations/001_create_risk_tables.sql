-- Migration: Create risk management tables (risk_limits, risk_audit, position_history)
-- Database: trading_db
-- Safe to run multiple times

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Risk Limits Configuration
CREATE TABLE IF NOT EXISTS risk_limits (
    risk_limit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    strategy_id UUID NOT NULL,
    max_daily_trades INT,
    max_loss_per_day DECIMAL(15,2),
    max_position_size DECIMAL(15,2),
    max_per_trade_risk DECIMAL(15,2),
    max_portfolio_exposure_pct DECIMAL(5,2),
    max_concentration_pct DECIMAL(5,2),
    circuit_breaker_loss_pct DECIMAL(5,2),
    enable_risk_checks BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Risk Audit Trail
CREATE TABLE IF NOT EXISTS risk_audit (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    order_id UUID NOT NULL,
    check_type VARCHAR(50),        -- PRE_TRADE, POST_TRADE
    violations JSONB,              -- JSON array of violations/details
    risk_score DECIMAL(5,2),
    approved BOOLEAN,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Position History
CREATE TABLE IF NOT EXISTS position_history (
    position_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    stock_code BIGINT,
    quantity INT,
    avg_price DECIMAL(15,2),
    current_value DECIMAL(15,2),
    unrealized_pnl DECIMAL(15,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_risk_limits_user_id ON risk_limits(user_id);
CREATE INDEX IF NOT EXISTS idx_risk_audit_user_id ON risk_audit(user_id);
CREATE INDEX IF NOT EXISTS idx_risk_audit_order_id ON risk_audit(order_id);
CREATE INDEX IF NOT EXISTS idx_position_history_user_id ON position_history(user_id);

-- Trigger to update updated_at on change
CREATE OR REPLACE FUNCTION rm_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_risk_limits_updated_at ON risk_limits;
CREATE TRIGGER update_risk_limits_updated_at BEFORE UPDATE ON risk_limits
    FOR EACH ROW EXECUTE FUNCTION rm_update_updated_at();

DROP TRIGGER IF EXISTS update_position_history_updated_at ON position_history;
CREATE TRIGGER update_position_history_updated_at BEFORE UPDATE ON position_history
    FOR EACH ROW EXECUTE FUNCTION rm_update_updated_at();
