# PostgreSQL Setup Guide for Trading System

This guide will help you set up PostgreSQL for the trading system.

## Prerequisites

- Ubuntu/Linux system
- Root or sudo access

## Installation

### 1. Install PostgreSQL

```bash
# Update package lists
sudo apt update

# Install PostgreSQL and contrib package
sudo apt install postgresql postgresql-contrib -y

# Check PostgreSQL status
sudo systemctl status postgresql

# Enable PostgreSQL to start on boot
sudo systemctl enable postgresql
```

### 2. Configure PostgreSQL

```bash
# Switch to postgres user
sudo -i -u postgres

# Access PostgreSQL prompt
psql

# Or directly as your user
sudo -u postgres psql
```

### 3. Create Database and User

```sql
-- Create database for trading system
CREATE DATABASE trading_system;

-- Create user with password
CREATE USER trading_user WITH PASSWORD 'your_secure_password';

-- Grant all privileges on database
GRANT ALL PRIVILEGES ON DATABASE trading_system TO trading_user;

-- Connect to the database
\c trading_system

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO trading_user;

-- Exit psql
\q
```

### 4. Configure PostgreSQL Authentication

Edit the PostgreSQL configuration file:

```bash
# Open pg_hba.conf
sudo nano /etc/postgresql/*/main/pg_hba.conf
```

Add or modify the following line to allow password authentication:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     md5
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
```

Restart PostgreSQL:

```bash
sudo systemctl restart postgresql
```

## Database Setup for User Config Service

### 1. Update Environment Variables

Update `services/user-config/.env`:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=trading_user
DB_PASSWORD=your_secure_password
DB_NAME=trading_system
DB_SSLMODE=disable

# Server Configuration
GRPC_PORT=50051

# Logging
LOG_LEVEL=debug
```

### 2. Run Database Migrations

```bash
# Connect to PostgreSQL
psql -U trading_user -d trading_system -h localhost

# Or if you get authentication errors, use sudo
sudo -u postgres psql -d trading_system
```

Then run the migration SQL:

```sql
-- Create strategies table
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id UUID PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    description TEXT,
    active BOOLEAN DEFAULT FALSE,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, strategy_name)
);

-- Create index on user_id
CREATE INDEX IF NOT EXISTS idx_strategies_user_id ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_strategies_active ON strategies(active);

-- Create strategy_conditions table
CREATE TABLE IF NOT EXISTS strategy_conditions (
    condition_id UUID PRIMARY KEY,
    strategy_id UUID NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    impact_score_threshold INTEGER,
    sentiments TEXT[],
    categories TEXT[],
    stock_codes BIGINT[],
    price_range_min NUMERIC(10, 2),
    price_range_max NUMERIC(10, 2),
    volume_threshold BIGINT,
    pct_change_threshold NUMERIC(5, 2),
    exchanges TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on strategy_id
CREATE INDEX IF NOT EXISTS idx_strategy_conditions_strategy_id ON strategy_conditions(strategy_id);

-- Create trade_configs table
CREATE TABLE IF NOT EXISTS trade_configs (
    trade_config_id UUID PRIMARY KEY,
    strategy_id UUID NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    order_type VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    max_position_size NUMERIC(15, 2),
    stop_loss_pct NUMERIC(5, 2),
    take_profit_pct NUMERIC(5, 2),
    exchange VARCHAR(50) NOT NULL,
    order_side VARCHAR(10) NOT NULL,
    limit_price NUMERIC(10, 2),
    validity VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on strategy_id
CREATE INDEX IF NOT EXISTS idx_trade_configs_strategy_id ON trade_configs(strategy_id);

-- Create risk_limits table
CREATE TABLE IF NOT EXISTS risk_limits (
    risk_limit_id UUID PRIMARY KEY,
    strategy_id UUID NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    max_daily_trades INTEGER,
    max_loss_per_day NUMERIC(15, 2),
    position_sizing VARCHAR(50) NOT NULL,
    max_portfolio_exposure_pct NUMERIC(5, 2),
    max_per_trade_risk NUMERIC(5, 2),
    enable_risk_checks BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on strategy_id
CREATE INDEX IF NOT EXISTS idx_risk_limits_strategy_id ON risk_limits(strategy_id);
```

### 3. Verify Tables

```sql
-- List all tables
\dt

-- Describe strategies table
\d strategies

-- Describe strategy_conditions table
\d strategy_conditions

-- Describe trade_configs table
\d trade_configs

-- Describe risk_limits table
\d risk_limits

-- Check data
SELECT * FROM strategies;
```

## Using pgAdmin

### 1. Install pgAdmin (if not installed)

```bash
# Install pgAdmin
sudo apt install pgadmin4 -y

# Or use the web version
sudo apt install pgadmin4-web -y
```

### 2. Connect to PostgreSQL using pgAdmin

1. **Open pgAdmin** (you can find it in your applications menu or launch from terminal: `pgadmin4`)

2. **Add New Server**:
   - Click on "Add New Server" or right-click on "Servers" → "Register" → "Server"

3. **General Tab**:
   - **Name**: `Trading System Local` (or any name you prefer)

4. **Connection Tab**:
   - **Host name/address**: `localhost` or `127.0.0.1`
   - **Port**: `5432` (default PostgreSQL port)
   - **Maintenance database**: `postgres` (default)
   - **Username**: `postgres` (default superuser) or `trading_user` (if created)
   - **Password**: Your PostgreSQL password
   - **Save password**: Check this box for convenience

5. **Click "Save"**

### 3. Default PostgreSQL Credentials

If you just installed PostgreSQL, the default settings are:
- **Username**: `postgres`
- **Password**: You need to set this (see below)
- **Port**: `5432`
- **Host**: `localhost`

### 4. Set/Reset PostgreSQL Password

If you don't know the `postgres` user password:

```bash
# Switch to postgres user
sudo -i -u postgres

# Open psql
psql

# Set password for postgres user
ALTER USER postgres PASSWORD 'your_new_password';

# Exit
\q
exit
```

### 5. Connection String Format

For application configuration, use:
```
postgresql://username:password@localhost:5432/database_name
```

Example:
```
postgresql://trading_user:your_secure_password@localhost:5432/trading_system
```

### 6. Quick Connection Test

Test your connection from terminal:

```bash
# Using psql command
psql -h localhost -p 5432 -U postgres -d postgres

# Or for trading_user
psql -h localhost -p 5432 -U trading_user -d trading_system
```

If you get "password authentication failed", make sure:
1. You've set the password correctly
2. pg_hba.conf allows md5 authentication
3. PostgreSQL service is running: `sudo systemctl status postgresql`

## Quick Start Guide

### Complete Setup from Scratch

```bash
# 1. Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib -y

# 2. Set postgres user password
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres123';"

# 3. Create trading database and user
sudo -u postgres psql << EOF
CREATE DATABASE trading_system;
CREATE USER trading_user WITH PASSWORD 'trading123';
GRANT ALL PRIVILEGES ON DATABASE trading_system TO trading_user;
\c trading_system
GRANT ALL ON SCHEMA public TO trading_user;
EOF

# 4. Run migrations
sudo -u postgres psql -d trading_system -f services/user-config/migrations/001_create_strategies_table.sql

# 5. Verify setup
psql -h localhost -p 5432 -U trading_user -d trading_system -c "\dt"
```

### pgAdmin Quick Connection

1. **Open pgAdmin**
2. **Right-click "Servers"** → **"Register"** → **"Server"**
3. **Fill in**:
   - Name: `Trading System`
   - Host: `localhost`
   - Port: `5432`
   - Username: `postgres` or `trading_user`
   - Password: `postgres123` or `trading123`
4. **Click "Save"**

## Troubleshooting

### Issue 1: Cannot connect to PostgreSQL

**Error**: `psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: No such file or directory`

**Solution**:
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Start PostgreSQL
sudo systemctl start postgresql

# Enable on boot
sudo systemctl enable postgresql
```

### Issue 2: Password authentication failed

**Error**: `psql: error: FATAL: password authentication failed for user "postgres"`

**Solution**:
```bash
# Reset password for postgres user
sudo -u postgres psql
ALTER USER postgres PASSWORD 'your_new_password';
\q

# Or if you can't access psql at all, edit pg_hba.conf temporarily
sudo nano /etc/postgresql/*/main/pg_hba.conf
# Change 'md5' to 'trust' temporarily for local connections
# Restart PostgreSQL
sudo systemctl restart postgresql
# Now you can connect without password and reset it
psql -U postgres
ALTER USER postgres PASSWORD 'your_new_password';
\q
# Change 'trust' back to 'md5' in pg_hba.conf
# Restart PostgreSQL again
sudo systemctl restart postgresql
```

### Issue 3: Permission denied for schema public

**Error**: `ERROR: permission denied for schema public`

**Solution**:
```bash
# Connect as postgres superuser
sudo -u postgres psql -d trading_system

# Grant permissions
GRANT ALL ON SCHEMA public TO trading_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading_user;
\q
```

### Issue 4: Database does not exist

**Error**: `psql: error: FATAL: database "trading_system" does not exist`

**Solution**:
```bash
# Create the database
sudo -u postgres psql -c "CREATE DATABASE trading_system;"

# Grant permissions
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE trading_system TO trading_user;"
```

### Issue 5: Port 5432 already in use

**Error**: `could not bind IPv4 address "0.0.0.0": Address already in use`

**Solution**:
```bash
# Check what's using port 5432
sudo lsof -i :5432

# Or check PostgreSQL processes
ps aux | grep postgres

# Kill any stray PostgreSQL processes if needed
sudo pkill -9 postgres

# Restart PostgreSQL
sudo systemctl restart postgresql
```

### Issue 6: Cannot find pg_hba.conf

**Solution**:
```bash
# Find pg_hba.conf location
sudo -u postgres psql -c "SHOW hba_file;"

# Or search for it
sudo find / -name pg_hba.conf 2>/dev/null
```

## Useful PostgreSQL Commands

### Database Management

```sql
-- List all databases
\l

-- Connect to a database
\c database_name

-- List all tables in current database
\dt

-- Describe a table
\d table_name

-- Show table size
\d+ table_name

-- Drop database (be careful!)
DROP DATABASE database_name;
```

### User Management

```sql
-- List all users
\du

-- Create user
CREATE USER username WITH PASSWORD 'password';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE database_name TO username;

-- Revoke privileges
REVOKE ALL PRIVILEGES ON DATABASE database_name FROM username;

-- Drop user
DROP USER username;
```

### Data Operations

```sql
-- Count rows
SELECT COUNT(*) FROM table_name;

-- View recent entries
SELECT * FROM table_name ORDER BY created_at DESC LIMIT 10;

-- Delete all data from table (keep structure)
TRUNCATE TABLE table_name CASCADE;

-- Check database size
SELECT pg_size_pretty(pg_database_size('trading_system'));
```

## Next Steps

After setting up PostgreSQL:

1. Update `services/user-config/.env` with your database credentials
2. Run the user-config service: `cd services/user-config && go run cmd/main.go`
3. Test the service with gRPC calls (see `services/user-config/TESTING.md`)
4. Monitor logs for any connection issues
5. Use pgAdmin to inspect data and run queries

## Security Best Practices

1. **Change default passwords** immediately in production
2. **Use strong passwords** (mix of letters, numbers, symbols)
3. **Limit network access** - only allow connections from trusted IPs
4. **Use SSL/TLS** in production (set `DB_SSLMODE=require`)
5. **Regular backups**:
   ```bash
   # Backup database
   pg_dump -U trading_user trading_system > backup.sql
   
   # Restore database
   psql -U trading_user trading_system < backup.sql
   ```
6. **Keep PostgreSQL updated**:
   ```bash
   sudo apt update
   sudo apt upgrade postgresql postgresql-contrib
   ```
