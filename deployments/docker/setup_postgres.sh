#!/bin/bash

# PostgreSQL Docker Setup Script for Trading System
# This script sets up PostgreSQL in Docker for development

set -e

echo "=========================================="
echo "PostgreSQL Docker Setup"
echo "=========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Error: Docker is not running"
    echo "Please start Docker and try again"
    exit 1
fi

echo "✅ Docker is installed and running"

# Navigate to docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo ""
echo "Starting PostgreSQL container..."
docker-compose -f docker-compose-postgres.yml up -d

echo ""
echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Wait for PostgreSQL to be healthy
for i in {1..30}; do
    if docker exec trading-postgres pg_isready -U postgres -d trading_db &> /dev/null; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    echo "Waiting for PostgreSQL... ($i/30)"
    sleep 2
done

# Verify connection
if docker exec trading-postgres psql -U postgres -d trading_db -c "SELECT 1;" &> /dev/null; then
    echo "✅ Database connection successful"
    echo ""
    echo "=========================================="
    echo "PostgreSQL Setup Complete!"
    echo "=========================================="
    echo ""
    echo "📊 Database Information:"
    echo "  Host: localhost"
    echo "  Port: 5432"
    echo "  Database: trading_db"
    echo "  Username: postgres"
    echo "  Password: postgres"
    echo ""
    echo "📝 Connection String:"
    echo "  postgresql://postgres:postgres@localhost:5432/trading_db?sslmode=disable"
    echo ""
    echo "🔧 Useful Commands:"
    echo "  Stop:    docker-compose -f docker-compose-postgres.yml down"
    echo "  Restart: docker-compose -f docker-compose-postgres.yml restart"
    echo "  Logs:    docker-compose -f docker-compose-postgres.yml logs -f postgres"
    echo "  Shell:   docker exec -it trading-postgres psql -U postgres -d trading_db"
    echo ""
    echo "📁 Migrations automatically applied from:"
    echo "  services/user-config/migrations/"
    echo ""
else
    echo "❌ Failed to connect to database"
    echo "Check logs with: docker-compose -f docker-compose-postgres.yml logs postgres"
    exit 1
fi
