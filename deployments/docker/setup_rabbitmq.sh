#!/bin/bash

# RabbitMQ Setup Script for Linux/Mac
# This script sets up and starts RabbitMQ using Docker Compose

echo "=== RabbitMQ Setup for Trading System ==="
echo ""

# Check if Docker is running
echo "Checking Docker status..."
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi
echo "Docker is running."
echo ""

# Navigate to the docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Stop and remove existing containers
echo "Stopping existing RabbitMQ containers..."
docker-compose -f docker-compose-rabbitmq.yml down -v
echo ""

# Start RabbitMQ
echo "Starting RabbitMQ..."
docker-compose -f docker-compose-rabbitmq.yml up -d

# Wait for RabbitMQ to be ready
echo ""
echo "Waiting for RabbitMQ to be ready..."
max_attempts=30
attempt=0
ready=false

while [ "$ready" = false ] && [ $attempt -lt $max_attempts ]; do
    attempt=$((attempt + 1))
    sleep 2
    
    if curl -s http://localhost:15672 > /dev/null 2>&1; then
        ready=true
    else
        echo -n "."
    fi
done

echo ""
if [ "$ready" = true ]; then
    echo "RabbitMQ is ready!"
    echo ""
    echo "RabbitMQ AMQP Port: amqp://localhost:5672"
    echo "RabbitMQ Management UI: http://localhost:15672"
    echo "Default Credentials: admin / admin123"
    echo ""
    
    echo "Setup complete! You can now run your services."
else
    echo "RabbitMQ failed to start within the timeout period."
    echo "Check logs with: docker-compose -f docker-compose-rabbitmq.yml logs"
    exit 1
fi

# Display running containers
echo ""
echo "Running containers:"
docker-compose -f docker-compose-rabbitmq.yml ps
