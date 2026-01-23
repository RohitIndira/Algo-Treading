#!/bin/bash

# Elasticsearch Setup Script for Linux/Mac
# This script sets up and starts Elasticsearch using Docker Compose

echo "=== Elasticsearch Setup for Trading System ==="
echo ""

# Check if Docker is running
echo "Checking Docker status..."
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi
echo "Docker is running."
echo ""

# Prefer modern Docker Compose V2 (`docker compose`) but fall back to legacy `docker-compose` if needed
if docker compose version > /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose > /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo "Error: Neither 'docker compose' nor 'docker-compose' is available. Please install Docker Compose."
    exit 1
fi

# Navigate to the docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Stop and remove existing containers
echo "Stopping existing Elasticsearch containers..."
${COMPOSE_CMD} -f docker-compose-elasticsearch.yml down -v
echo ""

# Start Elasticsearch
echo "Starting Elasticsearch..."
${COMPOSE_CMD} -f docker-compose-elasticsearch.yml up -d

# Wait for Elasticsearch to be ready
echo ""
echo "Waiting for Elasticsearch to be ready..."
max_attempts=30
attempt=0
ready=false

while [ "$ready" = false ] && [ $attempt -lt $max_attempts ]; do
    attempt=$((attempt + 1))
    sleep 2
    
    if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        ready=true
    else
        echo -n "."
    fi
done

echo ""
if [ "$ready" = true ]; then
    echo "Elasticsearch is ready!"
    echo ""
    echo "Elasticsearch URL: http://localhost:9200"
    echo "Kibana URL: http://localhost:5601"
    echo ""
    
    # Display cluster health
    echo "Cluster Health:"
    curl -s http://localhost:9200/_cluster/health | jq '.'
    echo ""
    
    echo "Setup complete! You can now run your Rules Engine service."
else
    echo "Elasticsearch failed to start within the timeout period."
    echo "Check logs with: ${COMPOSE_CMD} -f docker-compose-elasticsearch.yml logs"
    exit 1
fi

# Display running containers
echo ""
echo "Running containers:"
${COMPOSE_CMD} -f docker-compose-elasticsearch.yml ps
