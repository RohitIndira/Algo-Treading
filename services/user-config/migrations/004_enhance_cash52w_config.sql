-- Migration: Enhance cash52w_configs table for Phase 1 features
-- Date: 2026-02-10
-- Description: Add multi-level profit/SL, portfolio config, and manual controls

-- Add new columns to cash52w_configs table
ALTER TABLE cash52w_configs 
    -- Portfolio configuration
    ADD COLUMN IF NOT EXISTS total_capital DECIMAL(15,2) DEFAULT 500000 CHECK (total_capital >= 50000),
    ADD COLUMN IF NOT EXISTS max_stocks INT DEFAULT 25 CHECK (max_stocks >= 5 AND max_stocks <= 100),
    ADD COLUMN IF NOT EXISTS auto_rebalance BOOLEAN DEFAULT TRUE,
    
    -- Stop-Loss Levels (JSONB for flexibility)
    ADD COLUMN IF NOT EXISTS stop_loss_levels JSONB DEFAULT '{
        "level_1": {
            "trigger_percent": -10,
            "exit_quantity_percent": 50,
            "type": "fixed",
            "enabled": true
        },
        "level_2": {
            "trigger_percent": -20,
            "exit_quantity_percent": 100,
            "type": "trailing",
            "enabled": true
        }
    }'::jsonb,
    
    -- Profit Target Levels (JSONB for flexibility)
    ADD COLUMN IF NOT EXISTS profit_levels JSONB DEFAULT '{
        "level_1": {
            "trigger_percent": 15,
            "exit_quantity_percent": 33,
            "type": "fixed",
            "enabled": true
        },
        "level_2": {
            "trigger_percent": 30,
            "exit_quantity_percent": 50,
            "type": "fixed",
            "enabled": true
        },
        "level_3": {
            "trigger_percent": 50,
            "exit_quantity_percent": 100,
            "type": "trailing",
            "trail_percent": 10,
            "enabled": true
        }
    }'::jsonb,
    
    -- Manual Controls (Emergency actions)
    ADD COLUMN IF NOT EXISTS force_exit_all BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS force_exit_stocks TEXT[] DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS pause_new_entries BOOLEAN DEFAULT FALSE,
    
    -- Version for optimistic locking
    ADD COLUMN IF NOT EXISTS version INT DEFAULT 1;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_cash52w_configs_enabled 
    ON cash52w_configs(enabled) 
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_cash52w_configs_force_exit 
    ON cash52w_configs(force_exit_all) 
    WHERE force_exit_all = TRUE;

CREATE INDEX IF NOT EXISTS idx_cash52w_configs_pause 
    ON cash52w_configs(pause_new_entries) 
    WHERE pause_new_entries = TRUE;

-- Add comments for documentation
COMMENT ON COLUMN cash52w_configs.total_capital IS 'Total capital allocated for 52W strategy (₹)';
COMMENT ON COLUMN cash52w_configs.capital_per_stock IS 'Capital per stock position (₹)';
COMMENT ON COLUMN cash52w_configs.max_stocks IS 'Maximum number of stocks in portfolio';
COMMENT ON COLUMN cash52w_configs.auto_rebalance IS 'Automatically buy new stocks when SL hit';
COMMENT ON COLUMN cash52w_configs.stop_loss_levels IS 'Multi-level stop-loss configuration (JSONB)';
COMMENT ON COLUMN cash52w_configs.profit_levels IS 'Multi-level profit target configuration (JSONB)';
COMMENT ON COLUMN cash52w_configs.force_exit_all IS 'Emergency: Exit all positions immediately';
COMMENT ON COLUMN cash52w_configs.force_exit_stocks IS 'Emergency: Exit specific stocks [NSE:RELIANCE, ...]';
COMMENT ON COLUMN cash52w_configs.pause_new_entries IS 'Pause new stock entries (keep existing positions)';
COMMENT ON COLUMN cash52w_configs.version IS 'Version for optimistic locking';

-- Migration complete
-- To apply: psql -U postgres -d user_config_db -f migrations/004_enhance_cash52w_config.sql
