# 🚀 Algo-Trading: Live + Staging on Single Server

This guide explains how to run both **LIVE** and **STAGING** environments on the same server without conflicts.

## 📋 Overview

| Aspect | LIVE | STAGING |
|--------|------|---------|
| **Docker Compose** | `docker-compose.yml` | `docker-compose.staging.yml` |
| **Container Suffix** | None | `-staging` |
| **Volume Suffix** | None | `-staging` |
| **Network** | `trading-network` | `trading-network-staging` |
| **Database** | `trading_db` | `trading_db_staging` |

---

## 🔌 Port Allocation

### Infrastructure Services

| Service | LIVE | STAGING |
|---------|------|---------|
| **PostgreSQL** | 5432 | 5433 |
| **MongoDB** | 27017 | 27018 |
| **Redis** | 6379 | 6380 |
| **Zookeeper** | 2181 | 2182 |
| **Kafka** (internal) | 9092 | 9093 |
| **Kafka** (external) | 29092 | 29093 |
| **RabbitMQ** (AMQP) | 5672 | 5673 |
| **RabbitMQ** (UI) | 15672 | 15673 |

### Application Services

| Service | LIVE | STAGING |
|---------|------|---------|
| **API Gateway** | 8081 | 8181 |
| **User Config** (gRPC) | 50051 | 50151 |
| **Data Ingestion** (gRPC) | 50052 | 50152 |
| **Rules Engine** (gRPC) | 50053 | 50153 |
| **Trade Execution** (gRPC) | 50054 | 50154 |
| **Risk Management** (gRPC) | 50055 | 50155 |

---

## 🎯 Quick Start

### Option 1: Using Interactive Manager Script (Recommended)

```bash
cd /home/ubuntu/staging-algo-news/Algo-Treading

# Make script executable
chmod +x manage-environments.sh

# Run the interactive menu
./manage-environments.sh
```

### Option 2: Manual Commands

#### Start LIVE Only
```bash
cd /home/ubuntu/staging-algo-news/Algo-Treading
docker-compose up -d
```

#### Start STAGING Only
```bash
cd /home/ubuntu/staging-algo-news/Algo-Treading
docker-compose -f docker-compose.staging.yml up -d
```

#### Start BOTH
```bash
cd /home/ubuntu/staging-algo-news/Algo-Treading

# Start LIVE
docker-compose up -d

# Start STAGING
docker-compose -f docker-compose.staging.yml up -d
```

---

## 🛑 Stopping Environments

### Stop LIVE
```bash
docker-compose down
```

### Stop STAGING
```bash
docker-compose -f docker-compose.staging.yml down
```

### Stop ALL
```bash
docker-compose down
docker-compose -f docker-compose.staging.yml down
```

---

## 📊 Checking Status

### View Running Containers

```bash
# All containers
docker ps

# LIVE only (no -staging suffix)
docker ps | grep "^.*trading-" | grep -v staging

# STAGING only (-staging suffix)
docker ps | grep staging
```

### View Logs

```bash
# LIVE logs
docker-compose logs -f

# STAGING logs
docker-compose -f docker-compose.staging.yml logs -f

# Specific service (LIVE)
docker-compose logs -f api-gateway

# Specific service (STAGING)
docker-compose -f docker-compose.staging.yml logs -f api-gateway
```

### Health Checks

```bash
# LIVE API Gateway
curl http://localhost:8081/api/v1/health

# STAGING API Gateway
curl http://localhost:8181/api/v1/health

# LIVE PostgreSQL
docker exec trading-postgres pg_isready -U postgres

# STAGING PostgreSQL
docker exec trading-postgres-staging pg_isready -U postgres
```

---

## 🗄️ Database Access

### LIVE Database

```bash
# PostgreSQL
psql -h localhost -p 5432 -U postgres -d trading_db

# MongoDB
mongosh --host localhost:27017 -u admin -p admin --authenticationDatabase admin

# Redis
redis-cli -p 6379
```

### STAGING Database

```bash
# PostgreSQL
psql -h localhost -p 5433 -U postgres -d trading_db_staging

# MongoDB
mongosh --host localhost:27018 -u admin -p admin --authenticationDatabase admin

# Redis
redis-cli -p 6380
```

---

## 🐛 Troubleshooting

### Port Already in Use

If a port is already in use:

```bash
# Find process using the port
lsof -i :8081  # Replace with your port

# Kill the process
kill -9 <PID>
```

### Container Won't Start

```bash
# Check logs
docker-compose logs -f api-gateway

# Or with staging
docker-compose -f docker-compose.staging.yml logs -f api-gateway

# Restart container
docker-compose restart api-gateway
```

### Database Connection Issues

```bash
# Check if database container is healthy
docker-compose ps

# Restart the database
docker-compose restart postgres
docker-compose restart mongodb
```

### Containers Keep Restarting

1. Check logs for errors
2. Verify all dependencies are healthy
3. Wait for healthchecks to pass (usually 30-60 seconds)

---

## 🔄 Updating Containers

### Rebuild and Restart LIVE

```bash
docker-compose up -d --build
```

### Rebuild and Restart STAGING

```bash
docker-compose -f docker-compose.staging.yml up -d --build
```

### Rebuild Specific Service

```bash
# LIVE
docker-compose up -d --build api-gateway

# STAGING
docker-compose -f docker-compose.staging.yml up -d --build api-gateway
```

---

## 📝 Notes

- ✅ **Completely Isolated**: Each environment has separate containers, volumes, and networks
- ✅ **No Port Conflicts**: All ports are offset by 100+ to avoid collisions
- ✅ **Independent Databases**: Separate `trading_db` and `trading_db_staging`
- ✅ **Easy Management**: Use the manager script for simple operations
- ⚠️ **Resource Usage**: Both environments use CPU and RAM - monitor server resources
- ⚠️ **Persistent Data**: Docker volumes persist data even after containers stop

---

## 🔗 Service Communication

### LIVE - Internal Service Communication
Services communicate via internal Docker network `trading-network`:
- `api-gateway` → `postgres:5432`, `redis:6379`, `user-config:50051`, etc.

### STAGING - Internal Service Communication
Services communicate via internal Docker network `trading-network-staging`:
- `api-gateway-staging` → `postgres-staging:5432`, `redis-staging:6379`, `user-config-staging:50051`, etc.

---

## ✨ Best Practices

1. **Start STAGING first for testing** before switching LIVE traffic
2. **Use separate terminals** to monitor both environments:
   ```bash
   # Terminal 1: LIVE logs
   docker-compose logs -f

   # Terminal 2: STAGING logs
   docker-compose -f docker-compose.staging.yml logs -f
   ```

3. **Monitor resource usage** with `docker stats`
4. **Regular backups** of database volumes before major changes
5. **Test thoroughly** in STAGING before deploying to LIVE

---

## 🆘 Support

For issues or questions:
1. Check logs: `docker-compose logs -f`
2. Verify container status: `docker ps`
3. Check port availability: `netstat -tuln`
4. Review healthchecks: `docker-compose ps`
