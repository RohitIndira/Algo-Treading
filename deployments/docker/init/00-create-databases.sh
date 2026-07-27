#!/bin/bash
# ============================================================================
# Creates the second database and required extensions.
#
# The system uses TWO Postgres databases on the same instance:
#   trading_db        -> strategies, conditions, trade_configs, risk_limits,
#                        AMN activations, trade_signals   (user-config, rules-engine)
#   trading_execution -> orders, fills, positions, broker_accounts
#                        (trade-execution; user-config reads credentials from here)
#
# trading_db is created by the postgres entrypoint from POSTGRES_DB.
# This script adds trading_execution and enables extensions on both.
#
# Runs only on a fresh data volume (empty PGDATA).
# ============================================================================
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-'EOSQL'
    CREATE DATABASE trading_execution;
EOSQL

for db in "$POSTGRES_DB" trading_execution; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<-'EOSQL'
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "pgcrypto";
EOSQL
    echo "==> extensions ready on ${db}"
done
