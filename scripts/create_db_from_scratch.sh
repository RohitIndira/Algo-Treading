#!/usr/bin/env bash
set -euo pipefail

# Creates a fresh PostgreSQL database and applies all SQL migrations
# Usage: PGHOST=host PGPORT=5432 PGUSER=user PGPASSWORD=pass ./scripts/create_db_from_scratch.sh

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-}"
DBNAME="${DBNAME:-trading_system}"

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
echo "Applying pre-init SQL: scripts/pre_create_users.sql"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DBNAME" -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/pre_create_users.sql"

echo "Applying base schema: deployments/docker/init_all_schemas.sql"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DBNAME" -v ON_ERROR_STOP=1 -f "$REPO_ROOT/deployments/docker/init_all_schemas.sql"

echo "Searching and applying other migration SQL files..."

# Collect migration files in deterministic order
mapfile -t MIG_FILES < <(find "$REPO_ROOT" -type f \( -path "$REPO_ROOT/services/*/migrations/*.sql" -o -path "$REPO_ROOT/scripts/migrations/*.sql" -o -path "$REPO_ROOT/deployments/docker/migrations/*.sql" \) | sort)

for sql in "${MIG_FILES[@]}"; do
  echo "-> Applying: $sql"
  psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DBNAME" -v ON_ERROR_STOP=1 -f "$sql"
done

echo "All migrations applied successfully. Database '$DBNAME' is ready."
