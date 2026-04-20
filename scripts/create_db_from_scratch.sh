#!/usr/bin/env bash
set -euo pipefail

# Creates a fresh PostgreSQL database and applies all SQL migrations
# Usage: PGHOST=host PGPORT=5432 PGUSER=user PGPASSWORD=pass ./scripts/create_db_from_scratch.sh

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-}"
DBNAME="${DBNAME:-trading_db}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found in PATH. Please install PostgreSQL client tools." >&2
  exit 1
fi

if [ -z "$PGPASSWORD" ]; then
  echo -n "Postgres password for user $PGUSER (will not echo): "
  read -s PGPASSWORD
  echo
fi

export PGPASSWORD

echo "Creating fresh database '$DBNAME' on $PGHOST:$PGPORT as $PGUSER"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \"$DBNAME\";"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"$DBNAME\";"

echo "Applying base schema: deployments/docker/init_all_schemas.sql"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DBNAME" -v ON_ERROR_STOP=1 -f "$REPO_ROOT/deployments/docker/init_all_schemas.sql"

echo "Applying service migrations..."

# Collect migration files in deterministic order
mapfile -t MIG_FILES < <(find "$REPO_ROOT/services" -type f -path "*/migrations/*.sql" 2>/dev/null | sort)

for sql in "${MIG_FILES[@]}"; do
  base="$(basename "$sql")"
  # Skip rollback/cleanup migrations that conflict with the base schema
  if [[ "$base" == "012_rollback_v2.sql" || "$base" == "013_drop_legacy_tables.sql" ]]; then
    echo "-> Skipping (rollback/cleanup): $sql"
    continue
  fi
  echo "-> Applying: $sql"
  psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DBNAME" -v ON_ERROR_STOP=1 -f "$sql" || true
done

echo "All migrations applied successfully. Database '$DBNAME' is ready."
