# ✅ Setup Complete: Live + Staging Environment Configuration

## 📌 What Was Done

### 1. **Docker Compose Configuration**
- ✅ Updated `docker-compose.yml` (LIVE) - now uses `.env.live` files
- ✅ Created `docker-compose.staging.yml` (STAGING) - uses `.env.staging` files
- ✅ Both can run simultaneously on the same server with **zero conflicts**

### 2. **Environment Variables (All Services)**

#### Root Level Files
```
✅ .env.live          → LIVE global configuration
✅ .env.staging       → STAGING global configuration
```

#### API Gateway
```
✅ api/gateway/.env.live      → LIVE API Gateway (port 8081)
✅ api/gateway/.env.staging   → STAGING API Gateway (port 8181)
```

#### User Config Service
```
✅ services/user-config/.env.live      → LIVE (port 50051)
✅ services/user-config/.env.staging   → STAGING (port 50151)
```

#### Data Ingestion Service
```
✅ services/data-ingestion/.env.live      → LIVE (port 50052)
✅ services/data-ingestion/.env.staging   → STAGING (port 50152)
```

#### Rules Engine Service
```
✅ services/rules-engine/.env.live      → LIVE (port 50053)
✅ services/rules-engine/.env.staging   → STAGING (port 50153)
```

#### Trade Execution Service
```
✅ services/trade-execution/.env.live      → LIVE (port 50054)
✅ services/trade-execution/.env.staging   → STAGING (port 50154)
```

#### Risk Management Service
```
✅ services/risk-management/.env.live      → LIVE (port 50055)
✅ services/risk-management/.env.staging   → STAGING (port 50155)
```

### 3. **Management & Documentation**
```
✅ manage-environments.sh         → Interactive menu to manage both environments
✅ LIVE_STAGING_SETUP.md          → Quick start & troubleshooting guide
✅ ENV_CONFIGURATION_GUIDE.md     → Detailed environment configuration docs
✅ SETUP_SUMMARY.md               → This file
```

---

## 🎯 Key Differences: LIVE vs STAGING

| Component | LIVE | STAGING |
|-----------|------|---------|
| **Compose File** | `docker-compose.yml` | `docker-compose.staging.yml` |
| **Env Files** | `.env.live` | `.env.staging` |
| **Environment Mode** | `production` | `staging` |
| **Debug Mode** | `false` | `true` |
| **Container Names** | No suffix | `-staging` suffix |
| **Database Names** | `trading_db` | `trading_db_staging` |
| **Network** | `trading-network` | `trading-network-staging` |
| **Log Level** | `info` | `info` (DEBUG=true) |

### Port Mapping

| Service | LIVE | STAGING | Conflict |
|---------|------|---------|----------|
| PostgreSQL | 5432 | 5433 | ✅ None |
| MongoDB | 27017 | 27018 | ✅ None |
| Redis | 6379 | 6380 | ✅ None |
| Zookeeper | 2181 | 2182 | ✅ None |
| Kafka (Internal) | 9092 | 9093 | ✅ None |
| Kafka (External) | 29092 | 29093 | ✅ None |
| RabbitMQ (AMQP) | 5672 | 5673 | ✅ None |
| RabbitMQ (UI) | 15672 | 15673 | ✅ None |
| API Gateway (REST) | 8081 | 8181 | ✅ None |
| gRPC Services | 50051-55 | 50151-55 | ✅ None |

---

## 🚀 Quick Start

### Using Interactive Menu (Recommended)
```bash
cd /home/ubuntu/staging-algo-news/Algo-Treading
./manage-environments.sh

# Then choose:
# Option 1: Start LIVE
# Option 2: Start STAGING
# Option 3: Start BOTH
```

### Manual Commands

#### Start LIVE Only
```bash
docker-compose up -d
# API Gateway accessible at: http://localhost:8081
```

#### Start STAGING Only
```bash
docker-compose -f docker-compose.staging.yml up -d
# API Gateway accessible at: http://localhost:8181
```

#### Start BOTH
```bash
# Terminal 1: Start LIVE
docker-compose up -d

# Terminal 2: Start STAGING
docker-compose -f docker-compose.staging.yml up -d

# Check status
docker ps | grep trading
```

---

## 📊 Configuration Examples

### LIVE - PostgreSQL Connection
```
Host: localhost    (or postgres from within container)
Port: 5432
Database: trading_db
User: postgres
Password: postgres
```

### STAGING - PostgreSQL Connection
```
Host: localhost    (or postgres-staging from within container)
Port: 5433
Database: trading_db_staging
User: postgres
Password: postgres
```

### LIVE - API Gateway
```
URL: http://localhost:8081
Health Check: http://localhost:8081/api/v1/health
Environment: production
Debug: false
```

### STAGING - API Gateway
```
URL: http://localhost:8181
Health Check: http://localhost:8181/api/v1/health
Environment: staging
Debug: true
```

---

## 📝 Environment Variable Locations

### Search for all env files
```bash
find . -name ".env*" -not -path "*/node_modules/*" | sort
```

### View all LIVE configurations
```bash
grep -r "ENVIRONMENT=production" . --include=".env.live" | head -10
```

### View all STAGING configurations
```bash
grep -r "ENVIRONMENT=staging" . --include=".env.staging" | head -10
```

### Compare LIVE vs STAGING for a service
```bash
diff api/gateway/.env.live api/gateway/.env.staging
```

---

## 🔍 Verification

### Verify File Structure
```bash
# Check all .env files exist
ls -la .env.live .env.staging
ls -la api/gateway/.env.live api/gateway/.env.staging
ls -la services/*/\.env.live services/*/\.env.staging

# Count files
find . -name ".env.live" | wc -l  # Should be 8 files
find . -name ".env.staging" | wc -l  # Should be 8 files
```

### Verify Docker Compose Syntax
```bash
# Check LIVE compose file
docker-compose config > /dev/null && echo "✅ LIVE docker-compose.yml is valid"

# Check STAGING compose file
docker-compose -f docker-compose.staging.yml config > /dev/null && echo "✅ STAGING docker-compose.staging.yml is valid"
```

### Verify Port Mapping
```bash
# After starting both environments:
docker ps --format "table {{.Names}}\t{{.Ports}}" | grep "8081\|8181\|5432\|5433"

# Should show:
# trading-api-gateway      0.0.0.0:8081->8081/tcp
# trading-api-gateway-staging  0.0.0.0:8181->8081/tcp
# etc.
```

---

## 📚 Documentation Files

1. **[LIVE_STAGING_SETUP.md](LIVE_STAGING_SETUP.md)**
   - How to start/stop environments
   - Port allocation details
   - Database access instructions
   - Troubleshooting guide

2. **[ENV_CONFIGURATION_GUIDE.md](ENV_CONFIGURATION_GUIDE.md)**
   - Detailed environment variable management
   - How to add new variables
   - Best practices
   - Configuration verification

3. **[manage-environments.sh](manage-environments.sh)**
   - Interactive menu for managing both environments
   - View logs, check status, start/stop services
   - Executable script for easy management

---

## 🛠️ Management Commands

### Start/Stop (LIVE)
```bash
docker-compose up -d       # Start all LIVE services
docker-compose down         # Stop all LIVE services
docker-compose restart      # Restart all LIVE services
docker-compose logs -f      # View LIVE logs (Ctrl+C to stop)
```

### Start/Stop (STAGING)
```bash
docker-compose -f docker-compose.staging.yml up -d       # Start
docker-compose -f docker-compose.staging.yml down        # Stop
docker-compose -f docker-compose.staging.yml restart     # Restart
docker-compose -f docker-compose.staging.yml logs -f     # Logs
```

### View Configuration
```bash
# What env file is a service using?
docker inspect trading-api-gateway | grep -A10 "Env"
docker inspect trading-api-gateway-staging | grep -A10 "Env"

# Check current environment
docker exec trading-api-gateway env | grep ENVIRONMENT
docker exec trading-api-gateway-staging env | grep ENVIRONMENT
```

### Health Checks
```bash
# LIVE API Gateway
curl http://localhost:8081/api/v1/health

# STAGING API Gateway
curl http://localhost:8181/api/v1/health

# PostgreSQL
docker exec trading-postgres pg_isready -U postgres
docker exec trading-postgres-staging pg_isready -U postgres
```

---

## ⚠️ Important Notes

1. **Complete Isolation**: LIVE and STAGING are completely isolated
   - Separate databases
   - Separate volumes
   - Separate networks
   - Separate port ranges

2. **No Data Sharing**: Data created in LIVE won't appear in STAGING and vice versa

3. **Resource Usage**: Both environments consume CPU and RAM
   - Monitor with: `docker stats`
   - Consider server capacity before running both

4. **Persistent Data**: Docker volumes persist even after containers stop
   - Location: `/var/lib/docker/volumes/`
   - Backup before major changes

5. **Environment Variables**: Each service loads from its .env.* file
   - Override at runtime with: `MY_VAR=value docker-compose up -d`

---

## ✨ Next Steps

1. **Review the configuration**:
   ```bash
   cat LIVE_STAGING_SETUP.md
   cat ENV_CONFIGURATION_GUIDE.md
   ```

2. **Start the environments**:
   ```bash
   ./manage-environments.sh
   # Choose option 3 to start BOTH
   ```

3. **Verify they're running**:
   ```bash
   docker ps | grep trading | head -3
   curl http://localhost:8081/api/v1/health
   curl http://localhost:8181/api/v1/health
   ```

4. **Test the setup**:
   ```bash
   # Check LIVE databases
   psql -h localhost -p 5432 -U postgres -d trading_db -c "SELECT 1;"
   
   # Check STAGING databases
   psql -h localhost -p 5433 -U postgres -d trading_db_staging -c "SELECT 1;"
   ```

---

## 📞 Support

For issues:
1. Check [LIVE_STAGING_SETUP.md](LIVE_STAGING_SETUP.md#troubleshooting)
2. Check [ENV_CONFIGURATION_GUIDE.md](ENV_CONFIGURATION_GUIDE.md#troubleshooting)
3. Review logs: `docker-compose logs -f [service-name]`
4. Verify ports: `netstat -tuln | grep -E "8081|8181|5432|5433"`

---

## 📋 File Checklist

```
✅ Root Configuration
   ✅ .env.live
   ✅ .env.staging
   ✅ docker-compose.yml (updated)
   ✅ docker-compose.staging.yml (created)

✅ API Gateway
   ✅ api/gateway/.env.live
   ✅ api/gateway/.env.staging

✅ User Config Service
   ✅ services/user-config/.env.live
   ✅ services/user-config/.env.staging

✅ Data Ingestion Service
   ✅ services/data-ingestion/.env.live
   ✅ services/data-ingestion/.env.staging

✅ Rules Engine Service
   ✅ services/rules-engine/.env.live
   ✅ services/rules-engine/.env.staging

✅ Trade Execution Service
   ✅ services/trade-execution/.env.live
   ✅ services/trade-execution/.env.staging

✅ Risk Management Service
   ✅ services/risk-management/.env.live
   ✅ services/risk-management/.env.staging

✅ Management & Documentation
   ✅ manage-environments.sh
   ✅ LIVE_STAGING_SETUP.md
   ✅ ENV_CONFIGURATION_GUIDE.md
   ✅ SETUP_SUMMARY.md (this file)
```

---

**Setup completed successfully! 🎉**

You can now run both LIVE and STAGING environments on the same server without any conflicts.
