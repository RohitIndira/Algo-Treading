#!/usr/bin/env bash
set -euo pipefail

# Single-entry script to setup all service databases.
# This wrapper delegates to scripts/setup_all_databases.sh which contains
# the per-service migrations and DB creation logic.
#
# Usage:
#   ./setup_all_services_db.sh      # runs the full setup
#   sudo ./setup_all_services_db.sh # if your system requires sudo to start postgres

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$ROOT_DIR/scripts/setup_all_databases.sh"

if [ ! -f "$SCRIPT" ]; then
  echo "ERROR: central setup script not found: $SCRIPT"
  echo "Make sure you're in the repository root and that scripts/setup_all_databases.sh exists."
  exit 1
fi

echo "Running database setup using: $SCRIPT"
echo "Tip: run with sudo if the script needs to start the postgres service."

bash "$SCRIPT"

echo "Done. If any migrations failed, re-run the script to see errors and fix them."
