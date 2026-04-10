-- ============================================================================
-- Migration 013: Drop Legacy V1 Tables
--
-- These tables were replaced by Migration 010 (normalized_schema_v2).
-- Data was migrated to V2 by Migration 011.
--
-- Drops:
--   - orders           → replaced by orders_v2
--   - execution_events → replaced by order_status_history
--   - user_credentials → replaced by broker_accounts
--
-- Also drops the old trigger function update_updated_at_column()
-- that was only used by the legacy `orders` table.
-- ============================================================================

DROP TABLE IF EXISTS execution_events CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS user_credentials CASCADE;

-- Drop legacy trigger function (was only used by the old orders table)
DROP FUNCTION IF EXISTS update_updated_at_column();

SELECT 'Legacy V1 tables dropped successfully.' AS result;
