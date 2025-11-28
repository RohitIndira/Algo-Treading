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

-- Indexes for performance
CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_session_id ON user_sessions(session_id);
CREATE INDEX idx_user_sessions_is_active ON user_sessions(is_active);
CREATE INDEX idx_user_sessions_expires_at ON user_sessions(expires_at);
CREATE INDEX idx_user_sessions_login_time ON user_sessions(login_time);

-- User Authentication Credentials Table
CREATE TABLE IF NOT EXISTS user_credentials (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- ODIN API Configuration
    api_key TEXT NOT NULL,
    x_api_key TEXT NOT NULL,
    api_url VARCHAR(500) NOT NULL,
    
    -- User authentication details
    password_encrypted TEXT, -- Encrypted password (optional - for auto-login scenarios)
    totp_secret VARCHAR(100), -- TOTP secret for generating codes
    mpin_encrypted TEXT, -- Encrypted MPIN
    
    -- User profile
    client_id VARCHAR(50),
    pan VARCHAR(10),
    email VARCHAR(255),
    mobile_no VARCHAR(20),
    
    -- Configuration
    source VARCHAR(50) DEFAULT 'MOBILEAPI',
    preferred_login_type VARCHAR(20) DEFAULT 'PASSWORD',
    preferred_second_auth VARCHAR(20) DEFAULT 'TOTP',
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    last_login TIMESTAMP,
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for quick lookups
CREATE INDEX idx_user_credentials_user_id ON user_credentials(user_id);
CREATE INDEX idx_user_credentials_is_active ON user_credentials(is_active);

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

-- Indexes for analytics
CREATE INDEX idx_login_history_user_id ON login_history(user_id);
CREATE INDEX idx_login_history_status ON login_history(status);
CREATE INDEX idx_login_history_attempt_time ON login_history(attempt_time);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers for updated_at
CREATE TRIGGER update_user_sessions_updated_at
    BEFORE UPDATE ON user_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_credentials_updated_at
    BEFORE UPDATE ON user_credentials
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Function to clean expired sessions (can be called by a cron job)
CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    UPDATE user_sessions
    SET is_active = FALSE,
        logout_time = NOW()
    WHERE is_active = TRUE
    AND expires_at < NOW();
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
