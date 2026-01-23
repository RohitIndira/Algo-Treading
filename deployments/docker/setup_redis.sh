#!/bin/bash

# Redis Setup Script for Trading System
# This script sets up Redis with Redis Commander (Web UI)

set -e

echo "=========================================="
echo "Redis Setup for Trading System"
echo "=========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Prefer modern Docker Compose V2 (`docker compose`) but fall back to legacy `docker-compose` if needed
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo ""
echo "📦 Starting Redis containers..."

# Navigate to the docker directory
cd "$(dirname "$0")"

# Stop and remove existing containers if any
${COMPOSE_CMD} -f docker-compose-redis.yml down 2>/dev/null || true

# Start Redis and Redis Commander
${COMPOSE_CMD} -f docker-compose-redis.yml up -d

echo ""
echo "⏳ Waiting for Redis to be ready..."
sleep 5

# Check if Redis is running
if docker exec trading-redis redis-cli ping &> /dev/null; then
    echo "✅ Redis is running successfully!"
else
    echo "❌ Redis failed to start properly"
    exit 1
fi

echo ""
echo "=========================================="
echo "✅ Redis Setup Complete!"
echo "=========================================="
echo ""
echo "📊 Connection Details:"
echo "   Redis Server (host): localhost:6380 -> container redis:6379"
echo "   Password: (none - no password required)"
echo "   Database: 0 (default)"
echo ""
echo "🌐 Redis Commander (Web UI):"
echo "   URL: http://localhost:8081"
echo "   Open in browser to manage Redis visually"
echo ""
echo "💡 Useful Commands:"
echo "   Connect via CLI (host): redis-cli -h localhost -p 6380"
echo "   In Docker: docker exec -it trading-redis redis-cli"
echo "   View logs: docker logs trading-redis"
echo "   Stop Redis: ${COMPOSE_CMD} -f docker-compose-redis.yml down"
echo ""
echo "🔍 Test Connection:"
echo "   Run: redis-cli -h localhost -p 6380 ping"
echo "   Expected output: PONG"
echo ""
echo "=========================================="
