# Docker Deployment Guide

## Quick Start - Run the Complete System

### Prerequisites
- Docker Desktop (v20.10+) or Docker Engine + Docker Compose
- 8GB RAM minimum
- 20GB disk space

### Option 1: Start Everything with One Command

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

### Option 2: Start Services Individually

```bash
# Start infrastructure only
docker-compose up -d postgres mongodb redis elasticsearch kafka rabbitmq

# Wait for services to be healthy (check logs)
docker-compose logs

# Start application services
docker-compose up -d api-gateway user-config data-ingestion rules-engine trade-execution risk-management
```

---

## Service Ports and Access

| Service | Port | URL |
|---------|------|-----|
| API Gateway | 8081 | http://localhost:8081 |
| User Config (gRPC) | 50051 | localhost:50051 |
| Data Ingestion (gRPC) | 50052 | localhost:50052 |
| Rules Engine (gRPC) | 50053 | localhost:50053 |
| Trade Execution (gRPC) | 50054 | localhost:50054 |
| Risk Management (gRPC) | 50055 | localhost:50055 |
| PostgreSQL | 5432 | postgres://postgres:postgres@localhost:5432/trading_db |
| MongoDB | 27017 | mongodb://admin:admin@localhost:27017 |
| Redis | 6379 | redis://localhost:6379 |
| Elasticsearch | 9200 | http://localhost:9200 |
| Kafka | 9092 | localhost:9092 |
| RabbitMQ | 5672 | amqp://guest:guest@localhost:5672 |
| RabbitMQ Management | 15672 | http://localhost:15672 |

---

## Common Commands

### Check Service Status
```bash
# List all running containers
docker-compose ps

# Check health of services
docker-compose ps --status=running

# View logs for specific service
docker-compose logs api-gateway
docker-compose logs -f rules-engine  # Follow logs
```

### Access Services

```bash
# PostgreSQL CLI
docker-compose exec postgres psql -U postgres -d trading_db

# MongoDB CLI
docker-compose exec mongodb mongosh -u admin -p admin --authenticationDatabase admin

# Redis CLI
docker-compose exec redis redis-cli

# RabbitMQ Management UI
# Open: http://localhost:15672
# Username: guest
# Password: guest
```

### Rebuild Services

```bash
# Rebuild specific service
docker-compose build api-gateway

# Rebuild all services
docker-compose build

# Rebuild and restart
docker-compose up -d --build api-gateway
```

### Database Management

```bash
# Run migrations
docker-compose exec user-config ./migrate.sh

# Reset database
docker-compose down -v  # Remove volumes
docker-compose up -d    # Restart

# Backup PostgreSQL
docker-compose exec postgres pg_dump -U postgres trading_db > backup.sql

# Restore PostgreSQL
docker-compose exec -T postgres psql -U postgres trading_db < backup.sql
```

### Monitoring & Debugging

```bash
# View real-time logs
docker-compose logs -f

# View logs for all services
docker-compose logs --tail=100

# Inspect container
docker-compose exec api-gateway sh

# Check resource usage
docker stats

# View container details
docker-compose config

# Validate compose file
docker-compose config --quiet
```

---

## Environment Configuration

Edit the following in `docker-compose.yml` environment sections:

### Database Credentials
```yaml
POSTGRES_USER: postgres
POSTGRES_PASSWORD: postgres  # Change this!
```

### Logging Levels
```yaml
LOG_LEVEL: info  # Options: debug, info, warn, error
```

### gRPC Endpoints
Update in `api-gateway` service if services use different hosts/ports:
```yaml
USER_CONFIG_SERVICE: user-config:50051
DATA_INGESTION_SERVICE: data-ingestion:50052
```

---

## Production Deployment Tips

### 1. Use External Configuration
```bash
# Override compose file for production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### 2. Resource Limits
```yaml
# Add to services in docker-compose.yml
resources:
  limits:
    cpus: '2'
    memory: 2G
  reservations:
    cpus: '1'
    memory: 1G
```

### 3. Restart Policies
```yaml
# Already configured as 'unless-stopped'
restart: unless-stopped
```

### 4. Data Persistence
- Volumes are created automatically
- Data persists across container restarts
- Back up volumes regularly

### 5. Networking
- All services on same `trading-network`
- Services communicate via hostname (not localhost)
- External clients access via published ports

---

## Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs [service-name]

# Rebuild images
docker-compose build --no-cache

# Remove dangling resources
docker system prune -a
```

### Database connection errors
```bash
# Wait for database to be ready
docker-compose logs postgres
# Check if it says "database system is ready to accept connections"

# If stuck, restart
docker-compose restart postgres
```

### gRPC connection errors
```bash
# Verify services are up
docker-compose ps

# Check if services can communicate
docker-compose exec api-gateway ping user-config

# View gRPC port logs
docker-compose logs user-config
```

### Out of memory
```bash
# Increase Docker memory allocation
# Docker Desktop: Settings → Resources → Memory (increase to 8GB+)

# Or use resource limits in compose file
```

### Port conflicts
```bash
# If ports already in use, change in docker-compose.yml
# Example: "8081:8081" → "8082:8081"

# Find what's using a port
# Windows: netstat -ano | findstr :8081
# Linux/Mac: lsof -i :8081
```

---

## Scaling Services

Scale specific service instances:
```bash
# Run 3 instances of rules-engine
docker-compose up -d --scale rules-engine=3

# View all instances
docker-compose ps
```

---

## Cleanup

```bash
# Stop all containers
docker-compose stop

# Remove containers
docker-compose down

# Remove containers and volumes (WARNING: deletes data)
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Full cleanup (careful!)
docker system prune -a -v
```

---

## Next Steps

1. **Customize Configuration**: Edit `docker-compose.yml` for your environment
2. **Set Credentials**: Update default passwords in production
3. **Database Initialization**: Run migrations after first startup
4. **API Testing**: Use provided Postman collection
5. **Monitor Logs**: Set up centralized logging (ELK Stack, Splunk, etc.)

---

## Support

For issues, check:
- Service logs: `docker-compose logs [service]`
- Health checks: `docker-compose ps`
- Network connectivity: `docker network inspect trading-network`
