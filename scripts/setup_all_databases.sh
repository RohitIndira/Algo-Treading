#!/bin/bash

echo "================================================"
echo "PostgreSQL Database Setup for All Services"
echo "================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Database configurations
DB_USER="postgres"
DB_PASSWORD="postgres"

# Database names for different services
USER_CONFIG_DB="trading_db"
TRADE_EXECUTION_DB="trading_execution"
RULES_ENGINE_DB="trading_db"  # Uses same as user-config
USER_LOGIN_DB="trading_db"    # Uses same as user-config

# Check if PostgreSQL is installed
echo "${BLUE}Checking PostgreSQL installation...${NC}"
if ! command -v psql &> /dev/null; then
    echo "${RED}✗ PostgreSQL is not installed${NC}"
    echo "Please install PostgreSQL first:"
    echo "  sudo apt-get update"
    echo "  sudo apt-get install postgresql postgresql-contrib"
    exit 1
fi
echo "${GREEN}✓ PostgreSQL is installed${NC}"
echo ""

# Step 1: Check if PostgreSQL is running
echo "${YELLOW}Step 1: Checking if PostgreSQL is running...${NC}"
if sudo systemctl is-active --quiet postgresql; then
    echo "${GREEN}✓ PostgreSQL is running${NC}"
else
    echo "${YELLOW}Starting PostgreSQL...${NC}"
    sudo systemctl start postgresql
    sleep 2
    if sudo systemctl is-active --quiet postgresql; then
        echo "${GREEN}✓ PostgreSQL started successfully${NC}"
    else
        echo "${RED}✗ Failed to start PostgreSQL${NC}"
        exit 1
    fi
fi
echo ""

# Step 2: Set postgres user password
echo "${YELLOW}Step 2: Setting up postgres user password...${NC}"
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "${GREEN}✓ Password set successfully${NC}"
else
    echo "${YELLOW}! Password might already be set${NC}"
fi
echo ""

# Step 3: Create databases
echo "${YELLOW}Step 3: Creating databases...${NC}"

# Create trading_db (used by user-config, user-login-service, rules-engine)
echo "Creating database: $USER_CONFIG_DB"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d postgres -c "CREATE DATABASE $USER_CONFIG_DB;" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "${GREEN}✓ Database '$USER_CONFIG_DB' created${NC}"
else
    echo "${YELLOW}! Database '$USER_CONFIG_DB' might already exist${NC}"
fi

# Create trading_execution (used by trade-execution service)
echo "Creating database: $TRADE_EXECUTION_DB"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d postgres -c "CREATE DATABASE $TRADE_EXECUTION_DB;" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "${GREEN}✓ Database '$TRADE_EXECUTION_DB' created${NC}"
else
    echo "${YELLOW}! Database '$TRADE_EXECUTION_DB' might already exist${NC}"
fi

# Create trading_user for trade-execution service
echo "Creating user: trading_user"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d postgres -c "CREATE USER trading_user WITH PASSWORD 'your_secure_password';" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "${GREEN}✓ User 'trading_user' created${NC}"
else
    echo "${YELLOW}! User 'trading_user' might already exist${NC}"
fi

# Grant privileges to trading_user
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE $TRADE_EXECUTION_DB TO trading_user;" 2>/dev/null
echo ""

# Step 4: Run migrations for each service
echo "${YELLOW}Step 4: Running database migrations...${NC}"
echo ""

# 4.1: User Config Service
echo "${BLUE}=== User Config Service ===${NC}"
MIGRATIONS_DIR="../services/user-config/migrations"
if [ -d "$MIGRATIONS_DIR" ]; then
    for migration in $(ls $MIGRATIONS_DIR/*.sql 2>/dev/null | sort); do
        echo "Running migration: $(basename $migration)"
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $USER_CONFIG_DB -f "$migration"
        if [ $? -eq 0 ]; then
            echo "${GREEN}✓ Migration completed: $(basename $migration)${NC}"
        else
            echo "${RED}✗ Migration failed: $(basename $migration)${NC}"
        fi
    done
else
    echo "${YELLOW}! No migrations found for user-config service${NC}"
fi
echo ""

# 4.2: Trade Execution Service
echo "${BLUE}=== Trade Execution Service ===${NC}"
MIGRATIONS_DIR="../services/trade-execution/migrations"
if [ -d "$MIGRATIONS_DIR" ]; then
    for migration in $(ls $MIGRATIONS_DIR/*.sql 2>/dev/null | sort); do
        echo "Running migration: $(basename $migration)"
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $TRADE_EXECUTION_DB -f "$migration"
        if [ $? -eq 0 ]; then
            echo "${GREEN}✓ Migration completed: $(basename $migration)${NC}"
        else
            echo "${RED}✗ Migration failed: $(basename $migration)${NC}"
        fi
    done
    
    # Grant permissions to trading_user
    echo "Granting permissions to trading_user..."
    PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $TRADE_EXECUTION_DB -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading_user;" 2>/dev/null
    PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $TRADE_EXECUTION_DB -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading_user;" 2>/dev/null
    echo "${GREEN}✓ Permissions granted${NC}"
else
    echo "${YELLOW}! No migrations found for trade-execution service${NC}"
fi
echo ""

# 4.3: User Login Service
echo "${BLUE}=== User Login Service ===${NC}"
MIGRATIONS_DIR="../services/user-login-service/migrations"
if [ -d "$MIGRATIONS_DIR" ]; then
    for migration in $(ls $MIGRATIONS_DIR/*.sql 2>/dev/null | sort); do
        echo "Running migration: $(basename $migration)"
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $USER_LOGIN_DB -f "$migration"
        if [ $? -eq 0 ]; then
            echo "${GREEN}✓ Migration completed: $(basename $migration)${NC}"
        else
            echo "${RED}✗ Migration failed: $(basename $migration)${NC}"
        fi
    done
else
    echo "${YELLOW}! No migrations found for user-login-service${NC}"
fi
echo ""

# 4.4: Rules Engine Service
echo "${BLUE}=== Rules Engine Service ===${NC}"
MIGRATIONS_DIR="../services/rules-engine/migrations"
if [ -d "$MIGRATIONS_DIR" ]; then
    for migration in $(ls $MIGRATIONS_DIR/*.sql 2>/dev/null | sort); do
        echo "Running migration: $(basename $migration)"
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $RULES_ENGINE_DB -f "$migration"
        if [ $? -eq 0 ]; then
            echo "${GREEN}✓ Migration completed: $(basename $migration)${NC}"
        else
            echo "${RED}✗ Migration failed: $(basename $migration)${NC}"
        fi
    done
else
    echo "${YELLOW}! No migrations found for rules-engine service${NC}"
fi
echo ""

# 4.5: Odin API Wrapper Service
echo "${BLUE}=== Odin API Wrapper Service ===${NC}"
MIGRATIONS_DIR="../services/odin-api-wrapper/migrations"
if [ -d "$MIGRATIONS_DIR" ]; then
    for migration in $(ls $MIGRATIONS_DIR/*.sql 2>/dev/null | sort); do
        echo "Running migration: $(basename $migration)"
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d trading_system -f "$migration"
        if [ $? -eq 0 ]; then
            echo "${GREEN}✓ Migration completed: $(basename $migration)${NC}"
        else
            echo "${RED}✗ Migration failed: $(basename $migration)${NC}"
        fi
    done
else
    echo "${YELLOW}! No migrations found for odin-api-wrapper service${NC}"
fi
echo ""

# 4.6: Risk Management Service
echo "${BLUE}=== Risk Management Service ===${NC}"
MIGRATIONS_DIR="../services/risk-management/migrations"
if [ -d "$MIGRATIONS_DIR" ]; then
    for migration in $(ls $MIGRATIONS_DIR/*.sql 2>/dev/null | sort); do
        echo "Running migration: $(basename $migration)"
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $USER_CONFIG_DB -f "$migration"
        if [ $? -eq 0 ]; then
            echo "${GREEN}✓ Migration completed: $(basename $migration)${NC}"
        else
            echo "${RED}✗ Migration failed: $(basename $migration)${NC}"
        fi
    done
else
    echo "${YELLOW}! No migrations found for risk-management service${NC}"
fi
echo ""

# Step 5: Verify tables in each database
echo "${YELLOW}Step 5: Verifying created tables...${NC}"
echo ""

echo "${BLUE}Tables in $USER_CONFIG_DB:${NC}"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $USER_CONFIG_DB -c "\dt"
echo ""

echo "${BLUE}Tables in $TRADE_EXECUTION_DB:${NC}"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $TRADE_EXECUTION_DB -c "\dt"
echo ""

# Step 6: Summary
echo "${GREEN}================================================${NC}"
echo "${GREEN}Database Setup Complete!${NC}"
echo "${GREEN}================================================${NC}"
echo ""
echo "Databases created:"
echo "  - $USER_CONFIG_DB (user-config, user-login-service, rules-engine)"
echo "  - $TRADE_EXECUTION_DB (trade-execution)"
echo ""
echo "Users created:"
echo "  - postgres (password: postgres)"
echo "  - trading_user (password: your_secure_password)"
echo ""
echo "Connection strings:"
echo "  User Config:     postgresql://postgres:postgres@localhost:5432/$USER_CONFIG_DB"
echo "  Trade Execution: postgresql://trading_user:your_secure_password@localhost:5432/$TRADE_EXECUTION_DB"
echo ""
echo "${YELLOW}Note: Change default passwords in production!${NC}"
