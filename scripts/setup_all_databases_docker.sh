#!/bin/bash

echo "================================================"
echo "PostgreSQL Database Setup for All Services (Docker client)"
echo "================================================"
echo ""

# Config
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_PORT="55432"

# Helper: run psql inside a temporary postgres client container
psql_cmd() {
    # Usage: psql_cmd [psql-args...]
    docker run --rm -e PGPASSWORD="$DB_PASSWORD" -v "$(pwd)":/work -w /work postgres:15 psql -h host.docker.internal -p "$DB_PORT" -U "$DB_USER" "$@"
}

echo "Using Dockerized psql to connect to host.docker.internal:$DB_PORT as $DB_USER"
echo ""

USER_CONFIG_DB="trading_db"
TRADE_EXECUTION_DB="trading_execution"
RULES_ENGINE_DB="trading_db"
USER_LOGIN_DB="trading_db"

echo "Step: Creating databases and user (if not exists)"

echo "Creating database: $USER_CONFIG_DB"
psql_cmd -d postgres -c "CREATE DATABASE \"$USER_CONFIG_DB\";" 2>/dev/null || echo "! Database '$USER_CONFIG_DB' might already exist"

echo "Creating database: $TRADE_EXECUTION_DB"
psql_cmd -d postgres -c "CREATE DATABASE \"$TRADE_EXECUTION_DB\";" 2>/dev/null || echo "! Database '$TRADE_EXECUTION_DB' might already exist"

echo "Creating user: trading_user"
psql_cmd -d postgres -c "CREATE USER trading_user WITH PASSWORD 'your_secure_password';" 2>/dev/null || echo "! User 'trading_user' might already exist"

echo "Granting privileges to trading_user on $TRADE_EXECUTION_DB"
psql_cmd -d $TRADE_EXECUTION_DB -c "GRANT ALL PRIVILEGES ON DATABASE \"$TRADE_EXECUTION_DB\" TO trading_user;" 2>/dev/null || true

echo ""
echo "Running migrations from repository (if present)"

# Helper to apply migrations in a given directory to a database
apply_migrations() {
    local dir="$1"; shift
    local db="$1"; shift
    if [ -d "$dir" ]; then
        for migration in $(ls "$dir"/*.sql 2>/dev/null | sort); do
            echo "Running migration: $(basename "$migration") on $db"
            psql_cmd -d "$db" -f "$migration" || echo "Migration failed: $(basename "$migration")"
        done
    else
        echo "! No migrations found at $dir"
    fi
}

apply_migrations "services/user-config/migrations" "$USER_CONFIG_DB"
apply_migrations "services/trade-execution/migrations" "$TRADE_EXECUTION_DB"
apply_migrations "services/user-login-service/migrations" "$USER_LOGIN_DB"
apply_migrations "services/rules-engine/migrations" "$RULES_ENGINE_DB"

echo ""
echo "Verifying tables in $USER_CONFIG_DB"
psql_cmd -d "$USER_CONFIG_DB" -c "\dt" || true

echo "Verifying tables in $TRADE_EXECUTION_DB"
psql_cmd -d "$TRADE_EXECUTION_DB" -c "\dt" || true

echo ""
echo "Database setup (docker client) complete."
