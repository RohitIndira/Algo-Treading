# 🔧 Environment Configuration Guide

This document explains how environment variables are managed for LIVE and STAGING environments.

## 📁 File Structure

### Root Environment Files
```
Algo-Treading/
├── .env.live           # Global LIVE environment variables
├── .env.staging        # Global STAGING environment variables
└── docker-compose*.yml # Compose files reference .env.* files
```

### Service-Specific Environment Files
```
api/gateway/
├── .env.live           # API Gateway LIVE configuration
├── .env.staging        # API Gateway STAGING configuration
└── .env.example        # Template

services/
├── user-config/
│   ├── .env.live       # User Config LIVE
│   ├── .env.staging    # User Config STAGING
│   └── .env.example
├── data-ingestion/
│   ├── .env.live
│   ├── .env.staging
│   └── .env.example
├── rules-engine/
│   ├── .env.live
│   ├── .env.staging
│   └── .env.example
├── trade-execution/
│   ├── .env.live
│   ├── .env.staging
│   └── .env.example
└── risk-management/
    ├── .env.live
    ├── .env.staging
    └── .env.example
```

---

## 🚀 How It Works

### LIVE Environment (`docker-compose.yml`)
```bash
# Starts using .env.live files for each service
docker-compose up -d

# Each service loads its .env.live configuration:
# - api/gateway/.env.live
# - services/user-config/.env.live
# - services/data-ingestion/.env.live
# - services/rules-engine/.env.live
# - services/trade-execution/.env.live
# - services/risk-management/.env.live
```

### STAGING Environment (`docker-compose.staging.yml`)
```bash
# Starts using .env.staging files for each service
docker-compose -f docker-compose.staging.yml up -d

# Each service loads its .env.staging configuration:
# - api/gateway/.env.staging
# - services/user-config/.env.staging
# - services/data-ingestion/.env.staging
# - services/rules-engine/.env.staging
# - services/trade-execution/.env.staging
# - services/risk-management/.env.staging
```

---

## 🔑 Key Configuration Differences

### LIVE vs STAGING

| Variable | LIVE | STAGING |
|----------|------|---------|
| **ENVIRONMENT** | `production` | `staging` |
| **DEBUG** | `false` | `true` |
| **POSTGRES_HOST** | `postgres` | `postgres-staging` |
| **POSTGRES_PORT** | `5432` | `5432` (internal) |
| **MONGO_URI** | `mongodb://...@mongodb:...` | `mongodb://...@mongodb-staging:...` |
| **REDIS_HOST** | `redis` | `redis-staging` |
| **KAFKA_BROKERS** | `kafka:9092` | `kafka-staging:9092` |
| **RABBITMQ_URL** | `...@rabbitmq:...` | `...@rabbitmq-staging:...` |

### External Port Mapping

| Service | LIVE Port | STAGING Port |
|---------|-----------|--------------|
| PostgreSQL | 5432 | 5433 |
| MongoDB | 27017 | 27018 |
| Redis | 6379 | 6380 |
| API Gateway | 8081 | 8181 |
| Kafka | 29092 | 29093 |
| RabbitMQ AMQP | 5672 | 5673 |
| RabbitMQ UI | 15672 | 15673 |

---

## 📝 Modifying Configuration

### Add a new environment variable

1. **Update the .env files:**
   ```bash
   # For LIVE
   echo "NEW_VAR=value_live" >> .env.live
   
   # For STAGING
   echo "NEW_VAR=value_staging" >> .env.staging
   ```

2. **Update service .env files:**
   ```bash
   # Example: Add to api/gateway
   echo "NEW_VAR=api_gateway_value" >> api/gateway/.env.live
   echo "NEW_VAR=api_gateway_value_staging" >> api/gateway/.env.staging
   ```

3. **Restart the service:**
   ```bash
   # LIVE
   docker-compose up -d --build api-gateway
   
   # STAGING
   docker-compose -f docker-compose.staging.yml up -d --build api-gateway
   ```

### Override environment variable at runtime

```bash
# LIVE - override via environment
DATABASE_HOST=custom-host docker-compose up -d api-gateway

# STAGING - override via environment
DATABASE_HOST=custom-host docker-compose -f docker-compose.staging.yml up -d api-gateway
```

---

## 🔍 Viewing Configuration

### Check what environment variables a container is using

```bash
# LIVE
docker exec trading-api-gateway env | grep -E "DATABASE|REDIS|KAFKA"

# STAGING
docker exec trading-api-gateway-staging env | grep -E "DATABASE|REDIS|KAFKA"
```

### View .env file for a service

```bash
# LIVE API Gateway config
cat api/gateway/.env.live

# STAGING API Gateway config
cat api/gateway/.env.staging
```

---

## 🛠️ Best Practices

### 1. Never commit sensitive data
```bash
# .gitignore should include:
.env
.env.live
.env.staging
.env.local
**/api/gateway/.env
**/services/**/.env
```

### 2. Use .env.example as template
```bash
# Keep .env.example updated with non-sensitive defaults
# Developers copy it to create their .env files
```

### 3. Document all environment variables
```bash
# In .env.example, provide comments for each variable:
# DATABASE_HOST=localhost
# ^ The hostname or IP of PostgreSQL server
```

### 4. Use consistent naming
```bash
# Good:
REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

# Avoid:
redis_host, REDIS_ip, redis_port_number
```

### 5. Keep LIVE and STAGING synchronized
```bash
# When adding new variables, update both:
1. Update .env.live and .env.staging
2. Update all service .env.live files
3. Update all service .env.staging files
4. Document in .env.example
5. Commit changes
```

---

## 🚨 Troubleshooting

### Environment variable not being picked up

```bash
# Check the docker-compose file is using env_file
cat docker-compose.yml | grep -A2 "env_file"

# Check the .env file exists and is readable
ls -la api/gateway/.env.live

# View what the service actually has
docker exec trading-api-gateway printenv | grep YOUR_VAR
```

### Different behavior between LIVE and STAGING

1. Check .env.live vs .env.staging:
   ```bash
   diff api/gateway/.env.live api/gateway/.env.staging
   ```

2. Verify no override in docker-compose:
   ```bash
   grep -A10 "api-gateway:" docker-compose.yml
   ```

3. Check container logs:
   ```bash
   docker-compose logs api-gateway
   docker-compose -f docker-compose.staging.yml logs api-gateway
   ```

### Port conflicts

```bash
# Check if port is in use
netstat -tuln | grep 8081  # LIVE
netstat -tuln | grep 8181  # STAGING

# Kill process using port
lsof -i :8081
kill -9 <PID>
```

---

## 📊 Configuration Summary

### LIVE Setup
- **File**: `docker-compose.yml`
- **Env Prefix**: `.env.live`
- **Network**: `trading-network`
- **Database**: `trading_db`
- **Mode**: Production (DEBUG=false)

### STAGING Setup
- **File**: `docker-compose.staging.yml`
- **Env Prefix**: `.env.staging`
- **Network**: `trading-network-staging`
- **Database**: `trading_db_staging`
- **Mode**: Staging (DEBUG=true)

---

## ✅ Verification Checklist

```bash
# Verify LIVE setup
✓ docker-compose.yml loads .env.live files
✓ Ports: 5432, 27017, 6379, 8081, 9092, 5672 are live
✓ Environment=production
✓ Debug=false

# Verify STAGING setup
✓ docker-compose.staging.yml loads .env.staging files
✓ Ports: 5433, 27018, 6380, 8181, 9093, 5673 are staging
✓ Environment=staging
✓ Debug=true

# Verify both can run simultaneously
✓ No port conflicts
✓ No database conflicts
✓ No network conflicts
✓ No container name conflicts
```

---

## 🔄 Quick Commands

```bash
# View all env files
find . -name ".env*" -not -path "*/node_modules/*" | sort

# Compare configurations
diff api/gateway/.env.live api/gateway/.env.staging

# Update all staging databases to different values
sed -i 's/trading_db/trading_db_staging/g' services/**/.env.staging

# Check all services are using correct hostnames
grep -r "postgres:" **/.*env.live   # Should all say "postgres"
grep -r "postgres:" **/.*env.staging # Should all say "postgres-staging"
```
