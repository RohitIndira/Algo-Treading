-- ============================================================================
-- Migration 012: ROLLBACK — Drop V2 tables if migration needs to be undone
--
-- WARNING: This drops ALL V2 tables and their data.
-- The old tables (orders, execution_events, user_credentials) are untouched.
-- Run ONLY if you need to revert to the old schema.
-- ============================================================================

-- Order matters due to foreign key dependencies
DROP TABLE IF EXISTS position_fills CASCADE;
DROP TABLE IF EXISTS positions CASCADE;
DROP TABLE IF EXISTS daily_pnl_summary CASCADE;
DROP TABLE IF EXISTS signal_metrics CASCADE;
DROP TABLE IF EXISTS signal_metrics_default CASCADE;
DROP TABLE IF EXISTS order_group_legs CASCADE;
DROP TABLE IF EXISTS order_groups CASCADE;
DROP TABLE IF EXISTS stop_loss_config CASCADE;
DROP TABLE IF EXISTS broker_requests CASCADE;
DROP TABLE IF EXISTS broker_requests_default CASCADE;
DROP TABLE IF EXISTS fills CASCADE;
DROP TABLE IF EXISTS fills_default CASCADE;
DROP TABLE IF EXISTS order_status_history CASCADE;
DROP TABLE IF EXISTS order_status_history_default CASCADE;
DROP TABLE IF EXISTS orders_v2 CASCADE;
DROP TABLE IF EXISTS broker_accounts CASCADE;
DROP TABLE IF EXISTS instruments CASCADE;

-- Drop partition maintenance function
DROP FUNCTION IF EXISTS create_monthly_partitions();
-- Drop shared trigger function (only if old tables don't use it)
-- DROP FUNCTION IF EXISTS update_v2_updated_at();

SELECT 'V2 schema rolled back. Old tables (orders, execution_events, user_credentials) are intact.' AS result;