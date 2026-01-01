#!/bin/bash

echo "================================================"
echo "Kafka Setup for Trading System"
echo "================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Change to script directory
cd "$(dirname "$0")"

echo "${YELLOW}Step 1: Checking Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo "${RED}✗ Docker is not installed${NC}"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    echo "Or run: chmod +x deployments/docker/install_docker_compose.sh && ./deployments/docker/install_docker_compose.sh"
    exit 1
fi

# Check for docker compose (plugin) or docker-compose (standalone)
DOCKER_COMPOSE_CMD=""
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
    echo "${GREEN}✓ Docker Compose plugin found${NC}"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
    echo "${GREEN}✓ Docker Compose standalone found${NC}"
else
    echo "${RED}✗ Docker Compose is not installed${NC}"
    echo "Please install Docker Compose first"
    echo "Run: chmod +x deployments/docker/install_docker_compose.sh && ./deployments/docker/install_docker_compose.sh"
    exit 1
fi

echo ""

echo "${YELLOW}Step 2: Stopping existing Kafka containers and removing old data...${NC}"
$DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml down -v 2>/dev/null
echo "${GREEN}✓ Cleanup complete (volumes removed)${NC}"
echo ""

echo "${YELLOW}Step 3: Starting Kafka services...${NC}"
$DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml up -d

if [ $? -eq 0 ]; then
    echo "${GREEN}✓ Kafka services started${NC}"
else
    echo "${RED}✗ Failed to start Kafka services${NC}"
    exit 1
fi
echo ""

echo "${YELLOW}Step 4: Waiting for Kafka to be ready (this may take 30-60 seconds)...${NC}"
echo "Checking Kafka readiness..."

MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        echo "${GREEN}✓ Kafka is ready!${NC}"
        break
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    echo -n "."
    sleep 2
done

echo ""

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "${RED}✗ Kafka did not become ready in time${NC}"
    echo "Check logs with: docker-compose -f docker-compose-kafka.yml logs kafka"
    exit 1
fi

echo ""
echo "${YELLOW}Step 5: Creating required topics for all services...${NC}"

# Comprehensive array of all topics needed for the trading system
topics=(
    "user-configs"          # User Config Service - strategy updates
    "news-events"           # Data Ingestion - incoming market news
    "trade-signals"         # Rules Engine - matched trading signals
    "trade-executions"      # Trade Execution - execution results
    "risk-approvals"        # Risk Management - approved trades
    "order-updates"         # Trade Execution - order status updates
)

for topic in "${topics[@]}"; do
    # Extract topic name (before the comment)
    topic_name=$(echo "$topic" | awk '{print $1}')
    
    echo "Creating topic: $topic_name"
    docker exec trading-kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic "$topic_name" \
        --if-not-exists 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "${GREEN}✓ Topic $topic_name created${NC}"
    else
        echo "${YELLOW}! Topic $topic_name might already exist${NC}"
    fi
done

echo ""
echo "${YELLOW}Step 6: Verifying setup...${NC}"

echo ""
echo "Available topics:"
docker exec trading-kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Kafka cluster info:"
docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 | head -n 1

echo ""
echo "${GREEN}================================================${NC}"
echo "${GREEN}Kafka Setup Complete!${NC}"
echo "${GREEN}================================================${NC}"
echo ""
echo "Services running:"
docker-compose -f docker-compose-kafka.yml ps
echo ""
echo "Access Kafka UI at: ${GREEN}http://localhost:8082${NC}"
echo ""
echo "Kafka broker available at: ${GREEN}localhost:9092${NC}"
echo ""
echo "Useful commands:"
echo "  - View logs: docker-compose -f docker-compose-kafka.yml logs -f"
echo "  - Stop Kafka: docker-compose -f docker-compose-kafka.yml down"
echo "  - Restart: docker-compose -f docker-compose-kafka.yml restart"
echo ""
echo "Next steps:"
echo "  1. Update your service .env file:"
echo "     KAFKA_ENABLED=true"
echo "     KAFKA_BROKERS=localhost:9092"
echo "  2. Restart your services"
echo "  3. Check Kafka UI for messages: http://localhost:8082"
echo ""
