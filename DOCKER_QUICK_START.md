# Quick Start - Docker Commands

## Build & Run

```bash
# Build all images
docker-compose build

# Start all services
docker-compose up -d

# View status
docker-compose ps

# View logs
docker-compose logs -f api-gateway
```

## Share with Others

### Option 1: Docker Hub (Public)
```bash
# Login
docker login

# Push images
docker push yourusername/trading-api-gateway:v1.0.0
docker push yourusername/trading-user-config:v1.0.0
# ... share link

# Recipients run
docker-compose up -d
```

### Option 2: Save as File
```bash
# Export all images
docker save $(docker images --format '{{.Repository}}:{{.Tag}}' | grep trading-) | gzip > trading.tar.gz

# Recipient loads
gunzip < trading.tar.gz | docker load
docker-compose up -d
```

### Option 3: Share Compose File
```bash
# Share these files:
# - docker-compose.yml
# - .env.example

# Recipients run:
docker-compose pull
docker-compose up -d
```

## Stop & Cleanup

```bash
# Stop services
docker-compose stop

# Remove containers
docker-compose down

# Remove everything including volumes
docker-compose down -v

# Clean up images
docker image prune -a
```

## Troubleshoot

```bash
# Check service logs
docker-compose logs service-name

# Test connectivity
docker-compose exec api-gateway ping user-config

# Access database
docker-compose exec postgres psql -U postgres -d trading_db

# View resource usage
docker stats
```

## Service URLs

- API Gateway: http://localhost:8081
- RabbitMQ UI: http://localhost:15672 (guest/guest)
- PostgreSQL: localhost:5432
- MongoDB: localhost:27017
- Redis: localhost:6379
- Elasticsearch: http://localhost:9200
