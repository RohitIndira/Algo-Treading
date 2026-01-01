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

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo ""
echo "📦 Starting Redis containers..."

# Navigate to the docker directory
cd "$(dirname "$0")"

# Stop and remove existing containers if any
docker-compose -f docker-compose-redis.yml down 2>/dev/null || true

# Start Redis and Redis Commander
docker-compose -f docker-compose-redis.yml up -d

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
echo "   Redis Server: localhost:6379"
echo "   Password: (none - no password required)"
echo "   Database: 0 (default)"
echo ""
echo "🌐 Redis Commander (Web UI):"
echo "   URL: http://localhost:8081"
echo "   Open in browser to manage Redis visually"
echo ""
echo "💡 Useful Commands:"
echo "   Connect via CLI: redis-cli -h localhost -p 6379"
echo "   In Docker: docker exec -it trading-redis redis-cli"
echo "   View logs: docker logs trading-redis"
echo "   Stop Redis: docker-compose -f docker-compose-redis.yml down"
echo ""
echo "🔍 Test Connection:"
echo "   Run: redis-cli -h localhost -p 6379 ping"
echo "   Expected output: PONG"
echo ""
echo "=========================================="
