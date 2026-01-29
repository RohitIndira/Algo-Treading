# PowerShell script to setup all databases for the trading platform
# This script creates databases and tables if they don't exist.

$ContainerName = "trading-postgres"

# Helper function to run SQL commands inside Docker container
function Run-Sql {
    param(
        [string]$Database,
        [string]$SqlCommand,
        [bool]$IgnoreError = $false
    )
    
    # Use temporary file to avoid quoting issues with complex SQL
    $TempFile = [System.IO.Path]::GetTempFileName()
    $SqlCommand | Set-Content $TempFile
    
    $DestFile = "/tmp/sql_cmd.sql"
    docker cp $TempFile "${ContainerName}:${DestFile}" | Out-Null
    
    if ($IgnoreError) {
        docker exec $ContainerName psql -U postgres -d $Database -f $DestFile 2>$null
    } else {
        docker exec $ContainerName psql -U postgres -d $Database -f $DestFile
    }
    
    Remove-Item $TempFile
}

# 1. Create Databases
Write-Host "Creating databases..." -ForegroundColor Cyan
Run-Sql "postgres" "CREATE DATABASE trading_db;" $true
Run-Sql "postgres" "CREATE DATABASE trading_execution;" $true

# 2. Setup trading_db
Write-Host "Setting up trading_db..." -ForegroundColor Cyan

# Extensions
Run-Sql "trading_db" 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";' $true
Run-Sql "trading_db" 'CREATE EXTENSION IF NOT EXISTS "pgcrypto";' $true

# user-login-service schema
Write-Host "  -> user-login-service"
Run-Sql "trading_db" @"
CREATE TABLE IF NOT EXISTS user_credentials (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    api_key TEXT NOT NULL,
    x_api_key TEXT NOT NULL,
    api_url VARCHAR(500) NOT NULL,
    password_encrypted TEXT,
    totp_secret VARCHAR(100),
    mpin_encrypted TEXT,
    client_id VARCHAR(50),
    pan VARCHAR(10),
    email VARCHAR(255),
    mobile_no VARCHAR(20),
    source VARCHAR(50) DEFAULT 'MOBILEAPI',
    preferred_login_type VARCHAR(20) DEFAULT 'PASSWORD',
    preferred_second_auth VARCHAR(20) DEFAULT 'TOTP',
    is_active BOOLEAN DEFAULT TRUE,
    last_login TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS user_sessions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(255) UNIQUE NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT,
    broadcast_token TEXT,
    login_type VARCHAR(20) NOT NULL,
    second_auth_type VARCHAR(20),
    source VARCHAR(50) DEFAULT 'MOBILEAPI',
    user_name VARCHAR(255),
    email VARCHAR(255),
    mobile_no VARCHAR(20),
    user_code VARCHAR(50),
    group_id VARCHAR(50),
    exchanges TEXT[],
    product_types TEXT[],
    device_udid VARCHAR(255),
    device_model VARCHAR(100),
    device_platform VARCHAR(50),
    ip_address VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    login_time TIMESTAMP NOT NULL DEFAULT NOW(),
    last_activity TIMESTAMP NOT NULL DEFAULT NOW(),
    logout_time TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    odin_api_url VARCHAR(500),
    odin_oc_token VARCHAR(255),
    other_details JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS login_history (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(255),
    login_type VARCHAR(20),
    second_auth_type VARCHAR(20),
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    device_udid VARCHAR(255),
    device_platform VARCHAR(50),
    ip_address VARCHAR(50),
    user_agent TEXT,
    attempt_time TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_session_id ON user_sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_is_active ON user_sessions(is_active);
CREATE INDEX IF NOT EXISTS idx_user_credentials_user_id ON user_credentials(user_id);
"@

# rules-engine schema
Write-Host "  -> rules-engine"
Run-Sql "trading_db" @"
CREATE TABLE IF NOT EXISTS trade_signals (
    signal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id VARCHAR(255) NOT NULL UNIQUE,
    user_id VARCHAR(255) NOT NULL,
    strategy_id UUID NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    stock_code BIGINT NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    token BIGINT NOT NULL,
    order_type VARCHAR(20) NOT NULL,
    order_side VARCHAR(10) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(15, 2) NOT NULL,
    stop_loss DECIMAL(15, 2),
    take_profit DECIMAL(15, 2),
    match_score DECIMAL(5, 2) NOT NULL,
    impact_score INTEGER NOT NULL,
    sentiment VARCHAR(50),
    news_category VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    execution_price DECIMAL(15, 2),
    execution_time TIMESTAMP,
    broker_order_id VARCHAR(255),
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);
CREATE INDEX IF NOT EXISTS idx_trade_signals_user_id ON trade_signals(user_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_strategy_id ON trade_signals(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trade_signals_status ON trade_signals(status);
"@

# user-config schema
Write-Host "  -> user-config"
Run-Sql "trading_db" @"
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    description TEXT,
    active BOOLEAN DEFAULT false,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    match_all_news BOOLEAN NOT NULL DEFAULT FALSE,
    trading_mode VARCHAR(20) NOT NULL DEFAULT 'LIVE',
    CONSTRAINT unique_user_strategy_name UNIQUE(user_id, strategy_name)
);
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
CREATE INDEX IF NOT EXISTS idx_strategies_user_id ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_strategy_conditions_strategy_id ON strategy_conditions(strategy_id);
"@

# risk-management schema
Write-Host "  -> risk-management"
Run-Sql "trading_db" @"
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
CREATE TABLE IF NOT EXISTS risk_audit (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    order_id UUID NOT NULL,
    check_type VARCHAR(50),
    violations JSONB,
    risk_score DECIMAL(5,2),
    approved BOOLEAN,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
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
CREATE INDEX IF NOT EXISTS idx_risk_limits_user_id ON risk_limits(user_id);
CREATE INDEX IF NOT EXISTS idx_risk_audit_user_id ON risk_audit(user_id);
"@

# 3. Setup trading_execution
Write-Host "Setting up trading_execution..." -ForegroundColor Cyan
Run-Sql "trading_execution" 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";' $true
Run-Sql "trading_execution" @"
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    event_id UUID NOT NULL,
    stock_code BIGINT NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    trading_mode VARCHAR(10) NOT NULL DEFAULT 'LIVE',
    order_type VARCHAR(10) NOT NULL,
    order_side VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(15,2),
    stop_loss DECIMAL(15,2),
    take_profit DECIMAL(15,2),
    validity VARCHAR(10) DEFAULT 'DAY',
    status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    odin_order_id VARCHAR(50),
    odin_response TEXT,
    filled_quantity INT DEFAULT 0,
    filled_price DECIMAL(15,2),
    commission DECIMAL(10,2),
    total_cost DECIMAL(15,2),
    risk_approved BOOLEAN DEFAULT false,
    risk_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    submitted_at TIMESTAMP,
    executed_at TIMESTAMP,
    error_message TEXT,
    rejection_reason TEXT,
    retry_count INT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS execution_events (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(20) NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_trading_mode ON orders(trading_mode);

-- Paper Trading Tables
CREATE TABLE IF NOT EXISTS paper_positions (
    position_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    stock_code BIGINT NOT NULL,
    token BIGINT NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    entry_price DECIMAL(15,2) NOT NULL,
    current_price DECIMAL(15,2) NOT NULL,
    stop_loss DECIMAL(15,2),
    take_profit DECIMAL(15,2),
    unrealized_pnl DECIMAL(15,2) DEFAULT 0,
    unrealized_pnl_pct DECIMAL(10,4) DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    entry_order_id UUID NOT NULL,
    exit_order_id UUID,
    opened_at TIMESTAMP DEFAULT NOW(),
    closed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_entry_order FOREIGN KEY (entry_order_id) REFERENCES orders(order_id),
    CONSTRAINT check_quantity_positive CHECK (quantity > 0),
    CONSTRAINT check_entry_price_positive CHECK (entry_price > 0)
);
CREATE INDEX IF NOT EXISTS idx_paper_positions_user_id ON paper_positions(user_id);
CREATE INDEX IF NOT EXISTS idx_paper_positions_strategy_id ON paper_positions(strategy_id);
CREATE INDEX IF NOT EXISTS idx_paper_positions_status ON paper_positions(status);
CREATE INDEX IF NOT EXISTS idx_paper_positions_user_status ON paper_positions(user_id, status);
CREATE INDEX IF NOT EXISTS idx_paper_positions_token ON paper_positions(token);

CREATE TABLE IF NOT EXISTS paper_pnl_history (
    pnl_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(50) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    position_id UUID NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    entry_price DECIMAL(15,2) NOT NULL,
    exit_price DECIMAL(15,2) NOT NULL,
    realized_pnl DECIMAL(15,2) NOT NULL,
    realized_pnl_pct DECIMAL(10,4) NOT NULL,
    exit_reason VARCHAR(50) NOT NULL,
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_position FOREIGN KEY (position_id) REFERENCES paper_positions(position_id)
);
CREATE INDEX IF NOT EXISTS idx_paper_pnl_user_id ON paper_pnl_history(user_id);
CREATE INDEX IF NOT EXISTS idx_paper_pnl_strategy_id ON paper_pnl_history(strategy_id);
CREATE INDEX IF NOT EXISTS idx_paper_pnl_exit_time ON paper_pnl_history(exit_time);
"@

# Create view for daily PnL summary
Write-Host "  -> Creating paper trading views..."
Run-Sql "trading_execution" @"
CREATE OR REPLACE VIEW user_daily_paper_pnl AS
SELECT 
    user_id,
    strategy_id,
    DATE(exit_time) as trade_date,
    COUNT(*) as num_trades,
    SUM(realized_pnl) as daily_pnl,
    AVG(realized_pnl_pct) as avg_pnl_pct,
    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
    SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades
FROM paper_pnl_history
GROUP BY user_id, strategy_id, DATE(exit_time);
"@ $true

Write-Host ""
Write-Host "Verifying tables in trading_db:" -ForegroundColor Yellow
Run-Sql "trading_db" "\dt"

Write-Host ""
Write-Host "Verifying tables in trading_execution:" -ForegroundColor Yellow
Run-Sql "trading_execution" "\dt"

Write-Host ""
Write-Host "Database setup completed successfully!" -ForegroundColor Green
