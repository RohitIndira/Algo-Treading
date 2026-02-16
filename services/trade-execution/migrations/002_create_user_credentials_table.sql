-- User Credentials Table for Broker Authentication
-- Used by trade-execution to fetch Indira Securities credentials per user

CREATE TABLE IF NOT EXISTS user_credentials (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL UNIQUE,
    indira_user_id VARCHAR(100) NOT NULL,
    indira_app_id VARCHAR(100) NOT NULL,
    indira_source VARCHAR(20) NOT NULL DEFAULT 'WEB',
    indira_bearer_token TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for fast lookup by user_id
CREATE INDEX IF NOT EXISTS idx_user_credentials_user_id ON user_credentials(user_id);

-- Trigger to auto-update updated_at
CREATE TRIGGER update_user_credentials_updated_at BEFORE UPDATE ON user_credentials
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
