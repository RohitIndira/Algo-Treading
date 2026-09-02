#!/bin/bash
# Init script for the trading-postgres container.
#
# Runs ONCE on first container boot via
# postgres:15-alpine's /docker-entrypoint-initdb.d mechanism.
# Creates the 5 canonical databases each service owns per
# docs/db_ownership.md. Per-service schema migrations are run
# separately by each service (or their `go-migrate` step) — this
# script only ensures the databases *exist* so a `docker-compose up`
# on a fresh clone doesn't crash every service at ping time.
#
# Idempotent: uses `CREATE DATABASE ... WITH OWNER ...` inside an
# EXCEPTION-swallowed DO block so re-runs (or restarts on a persistent
# volume) are safe.
#
# POSTGRES_DB=trading_db is already created by the image's entrypoint
# before this script runs, so it's not in the list.

set -e

# Canonical DBs — every service using the code's env-var default DB name
# must have that name in this list, else its ping fails at boot.
#
# Owned-by (writer) column below is authoritative; readers may open the
# DB too (positions svc's read-only 'positions_reader' role, api-gateway,
# etc.) but only ONE service writes.
DBS=(
    # dbname             # owner service (writer)
    "execution_db   trade-execution"
    "signals_db         data-ingestion"
    "positions_db        positions"
    "order_status_db     orderstatus"
    "stockk_market       data-ingestion"
)

for row in "${DBS[@]}"; do
    dbname=$(echo "$row" | awk '{print $1}')
    owner=$(echo  "$row" | awk '{print $2}')
    echo "init_databases.sh: creating $dbname (owner=$owner) if missing..."

    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $dbname'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$dbname')
        \gexec
        COMMENT ON DATABASE $dbname IS 'Owned by: $owner. See docs/db_ownership.md.';
EOSQL
done

echo "init_databases.sh: all canonical DBs present."
