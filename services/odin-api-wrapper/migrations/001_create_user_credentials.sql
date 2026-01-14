-- Migration: Create user_credentials table for odin-api-wrapper
-- Database: trading_system

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS user_credentials (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(50) UNIQUE NOT NULL,
    api_key TEXT NOT NULL,
    x_api_key TEXT,
    api_url VARCHAR(500),
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_credentials_user_id ON user_credentials(user_id);

-- Trigger to update updated_at
CREATE OR REPLACE FUNCTION odin_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_user_credentials_updated_at ON user_credentials;
CREATE TRIGGER update_user_credentials_updated_at BEFORE UPDATE ON user_credentials
<<<<<<< HEAD
    FOR EACH ROW EXECUTE FUNCTION odin_update_updated_at();
=======
    FOR EACH ROW EXECUTE FUNCTION odin_update_updated_at();
>>>>>>> dev
