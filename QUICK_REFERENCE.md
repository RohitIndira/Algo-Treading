# 🚀 Quick Reference Card: LIVE vs STAGING

## 📍 Start Services

```bash
# Start LIVE
docker-compose up -d

# Start STAGING
docker-compose -f docker-compose.staging.yml up -d

# Start BOTH
docker-compose up -d && docker-compose -f docker-compose.staging.yml up -d
```

## 🛑 Stop Services

```bash
# Stop LIVE
docker-compose down

# Stop STAGING
docker-compose -f docker-compose.staging.yml down

# Stop ALL
docker-compose down && docker-compose -f docker-compose.staging.yml down
```

## 📋 View Logs

```bash
# LIVE - all services
docker-compose logs -f

# STAGING - all services
docker-compose -f docker-compose.staging.yml logs -f

# LIVE - specific service
docker-compose logs -f api-gateway

# STAGING - specific service
docker-compose -f docker-compose.staging.yml logs -f api-gateway
```

## 🔍 Check Status

```bash
# All containers
docker ps

# LIVE only
docker ps | grep -v staging

# STAGING only
docker ps | grep staging

# Detailed container info
docker-compose ps
docker-compose -f docker-compose.staging.yml ps
```

## 🌐 API Gateway Endpoints

```bash
# LIVE
curl http://localhost:8081/api/v1/health

# STAGING
curl http://localhost:8181/api/v1/health
```

## 📦 Database Connections

### LIVE
```
PostgreSQL: psql -h localhost -p 5432 -U postgres
MongoDB:   mongosh --host localhost:27017 -u admin -p admin
Redis:     redis-cli -p 6379
```

### STAGING
```
PostgreSQL: psql -h localhost -p 5433 -U postgres
MongoDB:   mongosh --host localhost:27018 -u admin -p admin
Redis:     redis-cli -p 6380
```

## 🔌 Port Mapping

| Service | LIVE | STAGING |
|---------|------|---------|
| API Gateway | 8081 | 8181 |
| PostgreSQL | 5432 | 5433 |
| MongoDB | 27017 | 27018 |
| Redis | 6379 | 6380 |
| Kafka | 29092 | 29093 |
| RabbitMQ AMQP | 5672 | 5673 |
| RabbitMQ UI | 15672 | 15673 |

## 🔄 Rebuild & Restart

```bash
# LIVE - rebuild specific service
docker-compose up -d --build api-gateway

# STAGING - rebuild specific service
docker-compose -f docker-compose.staging.yml up -d --build api-gateway

# LIVE - rebuild all
docker-compose up -d --build

# STAGING - rebuild all
docker-compose -f docker-compose.staging.yml up -d --build
```

## 📊 Resource Usage

```bash
# View resource usage (CPU, Memory, Network)
docker stats

# View specific container
docker stats trading-api-gateway
docker stats trading-api-gateway-staging
```

## 🔐 Access Containers

```bash
# LIVE container shell
docker exec -it trading-api-gateway sh

# STAGING container shell
docker exec -it trading-api-gateway-staging sh

# View container environment
docker exec trading-api-gateway env
docker exec trading-api-gateway-staging env
```

## 🛠️ Troubleshoot

```bash
# Check port availability
netstat -tuln | grep -E "8081|8181"

# View container logs with error filtering
docker-compose logs api-gateway | grep -i error
docker-compose -f docker-compose.staging.yml logs api-gateway | grep -i error

# Inspect container
docker inspect trading-api-gateway
docker inspect trading-api-gateway-staging

# Restart single service
docker-compose restart api-gateway
docker-compose -f docker-compose.staging.yml restart api-gateway
```

## 📝 Environment Variables

```bash
# View all env files
find . -name ".env.live" -o -name ".env.staging" | sort

# Compare LIVE vs STAGING
diff api/gateway/.env.live api/gateway/.env.staging

# Check actual environment in running container
docker exec trading-api-gateway printenv | sort
docker exec trading-api-gateway-staging printenv | sort
```

## 🎯 Common Tasks

### Update environment variable

```bash
# Edit .env file
nano api/gateway/.env.live        # LIVE
nano api/gateway/.env.staging     # STAGING

# Restart service to apply changes
docker-compose restart api-gateway
docker-compose -f docker-compose.staging.yml restart api-gateway
```

### Check service is healthy

```bash
# LIVE PostgreSQL
docker exec trading-postgres pg_isready -U postgres

# STAGING PostgreSQL
docker exec trading-postgres-staging pg_isready -U postgres

# LIVE Redis
docker exec trading-redis redis-cli ping

# STAGING Redis
docker exec trading-redis-staging redis-cli ping
```

### Verify network connectivity

```bash
# LIVE - test connection between services
docker exec trading-api-gateway curl http://user-config:50051

# STAGING - test connection between services
docker exec trading-api-gateway-staging curl http://user-config-staging:50051
```

### Clean up unused resources

```bash
# Remove stopped containers
docker container prune

# Remove unused volumes
docker volume prune

# Remove unused images
docker image prune

# Full cleanup
docker system prune -a
```

## 📚 Full Guides

- **Setup Guide**: `LIVE_STAGING_SETUP.md`
- **Config Guide**: `ENV_CONFIGURATION_GUIDE.md`
- **Setup Summary**: `SETUP_SUMMARY.md`
- **Manager Script**: `./manage-environments.sh`

## ⚡ One-Liners

```bash
# Stop all Algo-Trading containers
docker ps -q --filter "ancestor=*algo*" | xargs docker stop

# Remove all Algo-Trading containers
docker ps -a -q --filter "ancestor=*algo*" | xargs docker rm

# View real-time stats for all services
watch -n 1 'docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep trading'

# Compare database sizes
du -sh /var/lib/docker/volumes/*trading*

# Quick health check for LIVE
for svc in postgres mongodb redis kafka rabbitmq api-gateway; do echo "=== $svc ==="; docker exec trading-$svc curl -s http://localhost/health 2>/dev/null || echo "Container specific health check"; done
```

---

**Need help?** Run: `./manage-environments.sh`
