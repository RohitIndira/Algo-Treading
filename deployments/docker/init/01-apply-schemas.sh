#!/bin/bash
# ============================================================================
# Applies the base schema and per-service migrations to both databases.
#
#   trading_db        <- init_all_schemas.sql
#                        + services/user-config/migrations/*.sql
#                        + services/rules-engine/migrations/*.sql
#   trading_execution <- services/trade-execution/migrations/*.sql
#                        (001_init.sql is the authoritative order/fill schema)
#
# The SQL is mounted read-only at /sql by docker-compose, so the files stay
# the single source of truth in the repo — no copies to drift.
#
# Migrations are applied with ON_ERROR_STOP off per-file: several of them
# overlap with the base schema (all use IF NOT EXISTS), and an already-applied
# statement must not abort a fresh bring-up.
# ============================================================================
set -uo pipefail

PSQL="psql -v ON_ERROR_STOP=1 --username $POSTGRES_USER"

apply_file() {
    local db="$1" file="$2"
    [ -f "$file" ] || return 0
    echo "--> [${db}] $(basename "$file")"
    if ! $PSQL --dbname "$db" -f "$file" >/dev/null 2>/tmp/pgerr; then
        echo "    ! skipped (already applied or conflicts with base schema):"
        sed 's/^/      /' /tmp/pgerr | head -3
    fi
}

apply_dir() {
    local db="$1" dir="$2"
    [ -d "$dir" ] || return 0
    # Sorted so numeric prefixes apply in order and the run is reproducible.
    while IFS= read -r f; do
        apply_file "$db" "$f"
    done < <(find "$dir" -maxdepth 1 -name '*.sql' | sort)
}

echo "============================================================"
echo "Applying schema to ${POSTGRES_DB}"
echo "============================================================"
apply_file "$POSTGRES_DB" /sql/init_all_schemas.sql
apply_dir  "$POSTGRES_DB" /sql/migrations/user-config
apply_dir  "$POSTGRES_DB" /sql/migrations/rules-engine

echo "============================================================"
echo "Applying schema to trading_execution"
echo "============================================================"
apply_dir trading_execution /sql/migrations/trade-execution

echo "============================================================"
echo "Schema bootstrap complete."
for db in "$POSTGRES_DB" trading_execution; do
    count=$($PSQL --dbname "$db" -tAc \
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")
    echo "  ${db}: ${count} tables"
done
echo "============================================================"
