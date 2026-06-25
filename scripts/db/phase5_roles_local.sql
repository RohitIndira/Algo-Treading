-- ────────────────────────────────────────────────────────────────────
-- Phase 5 — Per-service Postgres roles (LOCAL DEV, full table coverage)
-- ────────────────────────────────────────────────────────────────────
-- Run as superuser:
--   PGPASSWORD=postgres psql -h localhost -U postgres -f scripts/db/phase5_roles_local.sql
--
-- LOCAL convention: password = role name (e.g. user_config_svc / user_config_svc).
-- Staging / prod must replace these with vault-provided secrets.
--
-- COVERAGE: every table in stockk_auth / stockk_trading / stockk_market has an
-- explicit owner + readers. No table is orphaned.
--
-- EXCLUSIONS:
--   • hft_engine_svc role is intentionally NOT created (hft-engine is frozen
--     per 2026-06-24 audit). hft_audit_orders becomes a superuser-only table
--     until hft-engine is unfrozen.
--
-- Idempotent: re-running drops + recreates roles. Connections must be closed.
-- ────────────────────────────────────────────────────────────────────

-- Drop existing roles before recreating (idempotent).
-- For repeat runs, REVOKE/REASSIGN their grants first so DROP ROLE succeeds.
-- The DO blocks below silently no-op on first run (when roles don't exist).
\set ON_ERROR_STOP off
\c stockk_auth
DO $$ BEGIN
    EXECUTE 'REASSIGN OWNED BY user_config_svc, trade_execution_svc, risk_management_svc TO postgres';
    EXECUTE 'DROP OWNED BY user_config_svc, trade_execution_svc, risk_management_svc';
EXCEPTION WHEN undefined_object THEN NULL; END $$;

\c stockk_trading
DO $$ BEGIN
    EXECUTE 'REASSIGN OWNED BY user_config_svc, trade_execution_svc, rules_engine_svc, rebalancer_svc, api_gateway_reader, risk_management_svc TO postgres';
    EXECUTE 'DROP OWNED BY user_config_svc, trade_execution_svc, rules_engine_svc, rebalancer_svc, api_gateway_reader, risk_management_svc';
EXCEPTION WHEN undefined_object THEN NULL; END $$;

\c stockk_market
DO $$ BEGIN
    EXECUTE 'REASSIGN OWNED BY data_ingestion_svc, rules_engine_svc, rebalancer_svc, api_gateway_reader, risk_management_svc TO postgres';
    EXECUTE 'DROP OWNED BY data_ingestion_svc, rules_engine_svc, rebalancer_svc, api_gateway_reader, risk_management_svc';
EXCEPTION WHEN undefined_object THEN NULL; END $$;

\c postgres
DROP ROLE IF EXISTS user_config_svc;
DROP ROLE IF EXISTS trade_execution_svc;
DROP ROLE IF EXISTS rules_engine_svc;
DROP ROLE IF EXISTS rebalancer_svc;
DROP ROLE IF EXISTS api_gateway_reader;
DROP ROLE IF EXISTS data_ingestion_svc;
DROP ROLE IF EXISTS risk_management_svc;
\set ON_ERROR_STOP on

-- ════════════════════════════════════════════════════════════════════
-- ROLE DEFINITIONS
-- ════════════════════════════════════════════════════════════════════

CREATE ROLE user_config_svc     LOGIN PASSWORD 'user_config_svc';
CREATE ROLE trade_execution_svc LOGIN PASSWORD 'trade_execution_svc';
CREATE ROLE rules_engine_svc    LOGIN PASSWORD 'rules_engine_svc';
CREATE ROLE rebalancer_svc      LOGIN PASSWORD 'rebalancer_svc';
CREATE ROLE api_gateway_reader  LOGIN PASSWORD 'api_gateway_reader';
CREATE ROLE data_ingestion_svc  LOGIN PASSWORD 'data_ingestion_svc';
CREATE ROLE risk_management_svc LOGIN PASSWORD 'risk_management_svc';

ALTER ROLE user_config_svc     CONNECTION LIMIT 25;
ALTER ROLE trade_execution_svc CONNECTION LIMIT 30;
ALTER ROLE rules_engine_svc    CONNECTION LIMIT 30;
ALTER ROLE rebalancer_svc      CONNECTION LIMIT 5;
ALTER ROLE api_gateway_reader  CONNECTION LIMIT 50;
ALTER ROLE data_ingestion_svc  CONNECTION LIMIT 20;
ALTER ROLE risk_management_svc CONNECTION LIMIT 10;

ALTER ROLE user_config_svc     SET statement_timeout = '10s';
ALTER ROLE trade_execution_svc SET statement_timeout = '10s';
ALTER ROLE rules_engine_svc    SET statement_timeout = '15s';
ALTER ROLE rebalancer_svc      SET statement_timeout = '30s';
ALTER ROLE api_gateway_reader  SET statement_timeout = '5s';
ALTER ROLE data_ingestion_svc  SET statement_timeout = '10s';
ALTER ROLE risk_management_svc SET statement_timeout = '5s';

-- ════════════════════════════════════════════════════════════════════
-- LOCKDOWN: revoke default PUBLIC CONNECT on all 3 DBs.
-- Without this, ANY role (including data_ingestion_svc trying to touch
-- stockk_trading) can connect — the boundary is only conceptual.
-- After REVOKE FROM PUBLIC, only explicitly-granted roles can connect.
-- ════════════════════════════════════════════════════════════════════
REVOKE CONNECT ON DATABASE stockk_auth    FROM PUBLIC;
REVOKE CONNECT ON DATABASE stockk_trading FROM PUBLIC;
REVOKE CONNECT ON DATABASE stockk_market  FROM PUBLIC;

-- ════════════════════════════════════════════════════════════════════
-- stockk_auth  (1 table: user_credentials)
-- ════════════════════════════════════════════════════════════════════
\c stockk_auth

-- Owner: user_config_svc (RW, owns it)
GRANT CONNECT, TEMPORARY ON DATABASE stockk_auth TO user_config_svc;
GRANT USAGE ON SCHEMA public TO user_config_svc;
GRANT ALL ON ALL TABLES IN SCHEMA public TO user_config_svc;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO user_config_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON TABLES TO user_config_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON SEQUENCES TO user_config_svc;

-- Reader: trade_execution_svc (reads user_credentials to call broker)
GRANT CONNECT ON DATABASE stockk_auth TO trade_execution_svc;
GRANT USAGE ON SCHEMA public TO trade_execution_svc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trade_execution_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO trade_execution_svc;

-- Reader: risk_management_svc (audit reads)
GRANT CONNECT ON DATABASE stockk_auth TO risk_management_svc;
GRANT USAGE ON SCHEMA public TO risk_management_svc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO risk_management_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO risk_management_svc;

-- ════════════════════════════════════════════════════════════════════
-- stockk_trading  (18 entries = 17 tables + 1 view)
--
-- Per-table writer mapping (audit findings):
--   execution_events              → trade_execution_svc (W)
--   execution_outbox              → rules_engine_svc (W)
--   hft_audit_orders              → SUPERUSER ONLY (hft frozen)
--   manthan_cooldown              → rules_engine_svc (W)
--   manthan_order_events          → trade_execution_svc (W)
--   manthan_orders                → trade_execution_svc (W)
--   manthan_portfolio_state       → rules_engine_svc (W)
--   manthan_position_events       → rules_engine_svc (W)
--   manthan_positions             → rules_engine_svc (W)
--   manthan_positions_with_intent → VIEW (read-only by nature)
--   manthan_signal_decisions      → rules_engine_svc (RW lifecycle owner)
--                                   + rebalancer_svc (INSERT-only co-writer)
--   orders                        → trade_execution_svc (W)
--   risk_limits                   → risk_management_svc (W)
--   signal_inbox                  → rules_engine_svc (W) → trade_execution_svc (R)
--   strategies                    → user_config_svc (RW) + rules_engine_svc (UPDATE)
--   strategy_conditions           → user_config_svc (RW)
--   trade_configs                 → user_config_svc (RW)
--   trade_signals                 → rules_engine_svc (W) → trade_execution_svc (R)
-- ════════════════════════════════════════════════════════════════════
\c stockk_trading

-- ─── user_config_svc: writes strategies / strategy_conditions / trade_configs ───
GRANT CONNECT ON DATABASE stockk_trading TO user_config_svc;
GRANT USAGE ON SCHEMA public TO user_config_svc;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    strategies, strategy_conditions, trade_configs
TO user_config_svc;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO user_config_svc;

-- ─── trade_execution_svc: writes orders + events; reads strategies/configs ───
GRANT CONNECT ON DATABASE stockk_trading TO trade_execution_svc;
GRANT USAGE ON SCHEMA public TO trade_execution_svc;
GRANT SELECT, INSERT, UPDATE ON
    orders, manthan_orders, manthan_order_events,
    execution_events
TO trade_execution_svc;
GRANT SELECT ON
    signal_inbox, trade_signals,
    strategies, strategy_conditions, trade_configs, risk_limits,
    manthan_positions, manthan_positions_with_intent, manthan_signal_decisions,
    execution_outbox
TO trade_execution_svc;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trade_execution_svc;

-- ─── rules_engine_svc: owns Manthan state machine ───
GRANT CONNECT ON DATABASE stockk_trading TO rules_engine_svc;
GRANT USAGE ON SCHEMA public TO rules_engine_svc;
GRANT SELECT, INSERT, UPDATE ON
    manthan_positions, manthan_position_events, manthan_portfolio_state,
    manthan_signal_decisions, manthan_cooldown,
    trade_signals, signal_inbox, execution_outbox,
    strategies, strategy_conditions
TO rules_engine_svc;
GRANT SELECT ON
    manthan_positions_with_intent,
    trade_configs, risk_limits,
    orders, manthan_orders, manthan_order_events, execution_events
TO rules_engine_svc;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO rules_engine_svc;

-- ─── rebalancer_svc: INSERT-only on co-write table, SELECT elsewhere ───
GRANT CONNECT ON DATABASE stockk_trading TO rebalancer_svc;
GRANT USAGE ON SCHEMA public TO rebalancer_svc;
-- INSERT-ONLY on the co-write table (deliberately NO UPDATE/DELETE — lifecycle owned by rules-engine):
GRANT SELECT, INSERT ON manthan_signal_decisions TO rebalancer_svc;
-- SELECT-only on the rest the rebalancer needs:
GRANT SELECT ON
    manthan_positions, manthan_positions_with_intent,
    manthan_cooldown, manthan_portfolio_state,
    strategies, strategy_conditions, trade_configs,
    risk_limits, trade_signals
TO rebalancer_svc;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO rebalancer_svc;

-- ─── risk_management_svc: writes risk_limits; reads everything else ───
GRANT CONNECT ON DATABASE stockk_trading TO risk_management_svc;
GRANT USAGE ON SCHEMA public TO risk_management_svc;
GRANT SELECT, INSERT, UPDATE ON risk_limits TO risk_management_svc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO risk_management_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO risk_management_svc;

-- ─── api_gateway_reader: SELECT-only across the entire DB ───
GRANT CONNECT ON DATABASE stockk_trading TO api_gateway_reader;
GRANT USAGE ON SCHEMA public TO api_gateway_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_gateway_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO api_gateway_reader;

-- ─── hft_audit_orders: NO service writes (hft frozen). Superuser-only.
-- Already covered: no role has been granted write on this table.

-- ════════════════════════════════════════════════════════════════════
-- stockk_market  (27 entries: 22 OHLCV partitions + 5 main tables)
--
-- Owner: data_ingestion_svc (full RW)
-- Readers: rules_engine_svc, rebalancer_svc (read manthan_signals only),
--          api_gateway_reader, risk_management_svc
-- ════════════════════════════════════════════════════════════════════
\c stockk_market

-- ─── data_ingestion_svc: owns it all ───
GRANT CONNECT, TEMPORARY ON DATABASE stockk_market TO data_ingestion_svc;
GRANT USAGE ON SCHEMA public TO data_ingestion_svc;
GRANT ALL ON ALL TABLES IN SCHEMA public TO data_ingestion_svc;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO data_ingestion_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON TABLES TO data_ingestion_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON SEQUENCES TO data_ingestion_svc;

-- ─── rules_engine_svc: SELECT-only on market ───
GRANT CONNECT ON DATABASE stockk_market TO rules_engine_svc;
GRANT USAGE ON SCHEMA public TO rules_engine_svc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rules_engine_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO rules_engine_svc;

-- ─── rebalancer_svc: SELECT manthan_signals only ───
GRANT CONNECT ON DATABASE stockk_market TO rebalancer_svc;
GRANT USAGE ON SCHEMA public TO rebalancer_svc;
GRANT SELECT ON manthan_signals, manthan_stocks, instruments TO rebalancer_svc;

-- ─── api_gateway_reader: SELECT-only across everything ───
GRANT CONNECT ON DATABASE stockk_market TO api_gateway_reader;
GRANT USAGE ON SCHEMA public TO api_gateway_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_gateway_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO api_gateway_reader;

-- ─── risk_management_svc: SELECT-only ───
GRANT CONNECT ON DATABASE stockk_market TO risk_management_svc;
GRANT USAGE ON SCHEMA public TO risk_management_svc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO risk_management_svc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO risk_management_svc;
