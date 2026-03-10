#!/bin/bash

# Setup script for trade_signals table in rules-engine service
# This table tracks all generated trade signals with execution status

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up trade_signals table...${NC}"

# Load environment variables (use same DB as user-config)
if [ -f "../user-config/.env" ]; then
    # Source the .env file to load variables
    set -a
    source ../user-config/.env
    set +a
    echo -e "${GREEN}✓ Loaded database config from user-config/.env${NC}"
else
    echo -e "${YELLOW}Warning: ../user-config/.env not found, using defaults${NC}"
    POSTGRES_HOST=${POSTGRES_HOST:-localhost}
    POSTGRES_PORT=${POSTGRES_PORT:-5432}
    POSTGRES_USER=${POSTGRES_USER:-postgres}
    POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-postgres}
    POSTGRES_DB=${POSTGRES_DB:-trading_db}
fi

# Set defaults if any variable is still empty
POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-postgres}
POSTGRES_DB=${POSTGRES_DB:-trading_db}
POSTGRES_SSLMODE=${POSTGRES_SSLMODE:-disable}

echo -e "${YELLOW}Database Configuration:${NC}"
echo "  Host: $POSTGRES_HOST"
echo "  Port: $POSTGRES_PORT"
echo "  Database: $POSTGRES_DB"
echo "  User: $POSTGRES_USER"
echo ""

# Run migration
echo -e "${GREEN}Running migration...${NC}"
PGPASSWORD=$POSTGRES_PASSWORD psql \
    -h $POSTGRES_HOST \
    -p $POSTGRES_PORT \
    -U $POSTGRES_USER \
    -d $POSTGRES_DB \
    -f migrations/001_create_trade_signals_table.sql

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Migration completed successfully!${NC}"
    echo ""
    echo -e "${GREEN}Trade signals table created with:${NC}"
    echo "  - Order tracking (order_id, user_id, strategy_id)"
    echo "  - Stock details (stock_code, symbol, exchange)"
    echo "  - Execution status (PENDING → SENT → EXECUTED/FAILED)"
    echo "  - Timestamps and metadata"
    echo ""
    echo -e "${YELLOW}Next steps:${NC}"
    echo "1. Restart rules-engine service"
    echo "2. Orders will be automatically tracked in PostgreSQL"
    echo "3. Query orders: SELECT * FROM trade_signals WHERE user_id = 'user-123';"
else
    echo -e "${RED}✗ Migration failed!${NC}"
    exit 1
fi
