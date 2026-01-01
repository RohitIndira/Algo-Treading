#!/bin/bash

echo "================================================"
echo "PostgreSQL Database Setup for User Config Service"
echo "================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Read configuration from .env
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_NAME="trading_db"

echo "Configuration:"
echo "- Database: $DB_NAME"
echo "- User: $DB_USER"
echo ""

# Step 1: Check if PostgreSQL is running
echo "${YELLOW}Step 1: Checking if PostgreSQL is running...${NC}"
if sudo systemctl is-active --quiet postgresql; then
    echo "${GREEN}✓ PostgreSQL is running${NC}"
else
    echo "${RED}✗ PostgreSQL is not running${NC}"
    echo "Starting PostgreSQL..."
    sudo systemctl start postgresql
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

# Step 3: Create database
echo "${YELLOW}Step 3: Creating database '$DB_NAME'...${NC}"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME;" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "${GREEN}✓ Database created successfully${NC}"
else
    echo "${YELLOW}! Database might already exist${NC}"
fi
echo ""

# Step 4: Run migrations
echo "${YELLOW}Step 4: Running database migrations...${NC}"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -f migrations/001_create_strategies_table.sql
if [ $? -eq 0 ]; then
    echo "${GREEN}✓ Migrations completed successfully${NC}"
else
    echo "${RED}✗ Migration failed${NC}"
    exit 1
fi
echo ""

# Step 5: Check tables
echo "${YELLOW}Step 5: Checking created tables...${NC}"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -c "\dt"
echo ""

# Step 6: Insert sample data
echo "${YELLOW}Step 6: Inserting sample test data...${NC}"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME <<EOF
-- Insert a test strategy
INSERT INTO strategies (strategy_id, user_id, strategy_name, description, active)
VALUES 
    ('123e4567-e89b-12d3-a456-426614174000', 'IS14415', 'Test Strategy 1', 'This is a test strategy', true),
    ('223e4567-e89b-12d3-a456-426614174001', 'IS14415', 'Test Strategy 2', 'Another test strategy', false)
ON CONFLICT DO NOTHING;

-- Insert strategy conditions
INSERT INTO strategy_conditions (condition_id, strategy_id, impact_score_threshold, sentiments, stock_codes)
VALUES 
    ('323e4567-e89b-12d3-a456-426614174002', '123e4567-e89b-12d3-a456-426614174000', 5, ARRAY['positive', 'neutral'], ARRAY[12345::BIGINT, 67890::BIGINT])
ON CONFLICT DO NOTHING;

-- Insert trade config
INSERT INTO trade_configs (trade_config_id, strategy_id, order_type, quantity, exchange, order_side, validity)
VALUES 
    ('423e4567-e89b-12d3-a456-426614174003', '123e4567-e89b-12d3-a456-426614174000', 'LIMIT', 100, 'NSE', 'BUY', 'DAY')
ON CONFLICT DO NOTHING;

-- Insert risk limits
INSERT INTO risk_limits (risk_limit_id, strategy_id, max_daily_trades, position_sizing, enable_risk_checks)
VALUES 
    ('523e4567-e89b-12d3-a456-426614174004', '123e4567-e89b-12d3-a456-426614174000', 10, 'FIXED', true)
ON CONFLICT DO NOTHING;
EOF

if [ $? -eq 0 ]; then
    echo "${GREEN}✓ Sample data inserted successfully${NC}"
else
    echo "${YELLOW}! Some data might already exist${NC}"
fi
echo ""

# Step 7: Verify data
echo "${YELLOW}Step 7: Verifying inserted data...${NC}"
echo ""
echo "Strategies:"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -c "SELECT strategy_id, user_id, strategy_name, active, created_at FROM strategies;"
echo ""

echo "Record counts:"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME <<EOF
SELECT 
    (SELECT COUNT(*) FROM strategies) as strategies,
    (SELECT COUNT(*) FROM strategy_conditions) as conditions,
    (SELECT COUNT(*) FROM trade_configs) as configs,
    (SELECT COUNT(*) FROM risk_limits) as limits;
EOF

echo ""
echo "${GREEN}================================================${NC}"
echo "${GREEN}Setup Complete!${NC}"
echo "${GREEN}================================================${NC}"
echo ""
echo "You can now:"
echo "1. Open pgAdmin"
echo "2. Connect to server with:"
echo "   - Host: localhost"
echo "   - Port: 5432"
echo "   - Database: $DB_NAME"
echo "   - User: $DB_USER"
echo "   - Password: $DB_PASSWORD"
echo ""
echo "3. Navigate to: Servers → [Your Server] → Databases → $DB_NAME → Schemas → public → Tables"
echo "4. Right-click any table → 'View/Edit Data' → 'All Rows'"
echo ""
