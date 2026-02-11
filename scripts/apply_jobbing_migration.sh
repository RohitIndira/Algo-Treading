#!/bin/bash
# Bash script to apply Jobbing strategy database migration

set -e

echo "====================================="
echo "Jobbing Strategy Database Migration"
echo "====================================="
echo ""

# Load environment variables
ENV_FILE="./services/user-config/.env"
if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from: $ENV_FILE"
    export $(cat "$ENV_FILE" | grep -v '^#' | xargs)
else
    echo "Warning: .env file not found at $ENV_FILE"
    echo "Using environment variables or defaults"
fi

# Database connection parameters
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-user_config_db}"

echo ""
echo "Database Connection Parameters:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""

# Set PGPASSWORD for psql
export PGPASSWORD="$DB_PASSWORD"

# Migration file path
MIGRATION_FILE="./services/user-config/migrations/004_create_jobbing_configs.sql"

if [ ! -f "$MIGRATION_FILE" ]; then
    echo "Error: Migration file not found: $MIGRATION_FILE"
    exit 1
fi

echo "Migration file: $MIGRATION_FILE"
echo ""

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "✗ PostgreSQL client (psql) not found in PATH"
    echo "Please install PostgreSQL client tools"
    exit 1
fi

echo "✓ PostgreSQL client (psql) found"
echo ""

echo "Applying migration..."

# Test database connection first
echo "Testing database connection..."
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT version();" > /dev/null 2>&1; then
    echo "✗ Database connection failed!"
    exit 1
fi

echo "✓ Database connection successful"
echo ""

# Apply migration
echo "Applying Jobbing strategy migration..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$MIGRATION_FILE"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Migration applied successfully!"
    echo ""
    
    # Verify table creation
    echo "Verifying table creation..."
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            COUNT(*) as table_count,
            (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'jobbing_configs') as column_count,
            (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_name = 'jobbing_configs') as constraint_count
        FROM information_schema.tables 
        WHERE table_name = 'jobbing_configs';
    "
    echo ""
    
    # Show table structure
    echo "Table structure:"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\d jobbing_configs"
    
    echo ""
    echo "====================================="
    echo "Migration completed successfully!"
    echo "====================================="
else
    echo ""
    echo "✗ Migration failed!"
    echo "Check the error messages above for details"
    exit 1
fi

# Cleanup
unset PGPASSWORD
