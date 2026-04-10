#!/bin/bash

# Unified Setup Script for Trading System Infrastructure
# Usage:
#   ./setup.sh              - Start all services
#   ./setup.sh postgres     - Start only PostgreSQL
#   ./setup.sh kafka        - Start only Kafka + Zookeeper + create topics
#   ./setup.sh redis        - Start only Redis
#   ./setup.sh rabbitmq     - Start only RabbitMQ
#   ./setup.sh down         - Stop all services
#   ./setup.sh ui           - Start all services + web UIs (Redis Commander, Kafka UI)

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
SERVICE="${1:-all}"

# Detect docker compose command
if docker compose version &>/dev/null; then
    DC="docker compose -f $COMPOSE_FILE"
elif command -v docker-compose &>/dev/null; then
    DC="docker-compose -f $COMPOSE_FILE"
else
    echo -e "${RED}Docker Compose is not installed${NC}"
    exit 1
fi

check_docker() {
    if ! docker info &>/dev/null; then
        echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
        exit 1
    fi
    echo -e "${GREEN}Docker is running${NC}"
}

wait_for_container() {
    local container=$1
    local check_cmd=$2
    local max_attempts=${3:-30}
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if eval "$check_cmd" &>/dev/null; then
            return 0
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done
    echo ""
    return 1
}

setup_postgres() {
    echo -e "${YELLOW}Starting PostgreSQL...${NC}"
    $DC up -d postgres
    echo -n "Waiting for PostgreSQL"
    if wait_for_container "trading-postgres" "docker exec trading-postgres pg_isready -U postgres -d trading_db"; then
        echo -e "\n${GREEN}PostgreSQL is ready!${NC}"
        echo "  Host: localhost:5432 | DB: trading_db | User: postgres / postgres"
    else
        echo -e "${RED}PostgreSQL failed to start${NC}"
        return 1
    fi
}

setup_redis() {
    echo -e "${YELLOW}Starting Redis...${NC}"
    $DC up -d redis
    echo -n "Waiting for Redis"
    if wait_for_container "trading-redis" "docker exec trading-redis redis-cli ping"; then
        echo -e "\n${GREEN}Redis is ready!${NC}"
        echo "  Host: localhost:6379 | No password"
    else
        echo -e "${RED}Redis failed to start${NC}"
        return 1
    fi
}

setup_rabbitmq() {
    echo -e "${YELLOW}Starting RabbitMQ...${NC}"
    $DC up -d rabbitmq
    echo -n "Waiting for RabbitMQ"
    if wait_for_container "trading-rabbitmq" "docker exec trading-rabbitmq rabbitmq-diagnostics -q ping" 30; then
        echo -e "\n${GREEN}RabbitMQ is ready!${NC}"
        echo "  AMQP: localhost:5672 | Management UI: http://localhost:15672 | User: guest / guest"
    else
        echo -e "${RED}RabbitMQ failed to start${NC}"
        return 1
    fi
}

setup_kafka() {
    echo -e "${YELLOW}Starting Kafka + Zookeeper...${NC}"
    $DC up -d zookeeper kafka
    echo -n "Waiting for Kafka"
    if wait_for_container "trading-kafka" "docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092" 30; then
        echo -e "\n${GREEN}Kafka is ready!${NC}"
    else
        echo -e "${RED}Kafka failed to start${NC}"
        return 1
    fi

    echo -e "${YELLOW}Creating Kafka topics...${NC}"
    topics=("user-config-events" "news-events" "trade-signals" "trade-executions" "risk-approvals" "order-updates")
    for topic in "${topics[@]}"; do
        docker exec trading-kafka kafka-topics --create \
            --bootstrap-server localhost:9092 \
            --replication-factor 1 \
            --partitions 3 \
            --topic "$topic" \
            --if-not-exists 2>/dev/null && \
            echo -e "  ${GREEN}$topic${NC}" || \
            echo -e "  ${YELLOW}$topic (already exists)${NC}"
    done
    echo "  Broker: localhost:9092"
}

# ===== Main =====

echo "================================================"
echo "Trading System Infrastructure Setup"
echo "================================================"
echo ""

check_docker

case "$SERVICE" in
    postgres)  setup_postgres ;;
    redis)     setup_redis ;;
    rabbitmq)  setup_rabbitmq ;;
    kafka)     setup_kafka ;;
    down)
        echo -e "${YELLOW}Stopping all services...${NC}"
        $DC --profile ui down
        echo -e "${GREEN}All services stopped${NC}"
        ;;
    ui)
        echo -e "${YELLOW}Starting all services with web UIs...${NC}"
        $DC --profile ui up -d
        echo -n "Waiting for services"
        sleep 10
        echo -e "\n${GREEN}Services started!${NC}"
        setup_kafka  # Create topics after kafka is up
        echo ""
        echo "Web UIs:"
        echo "  Kafka UI:         http://localhost:8082"
        echo "  Redis Commander:  http://localhost:8081"
        echo "  RabbitMQ:         http://localhost:15672"
        ;;
    all|"")
        setup_postgres
        echo ""
        setup_redis
        echo ""
        setup_rabbitmq
        echo ""
        setup_kafka
        ;;
    *)
        echo "Usage: $0 [postgres|redis|rabbitmq|kafka|down|ui|all]"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}Done!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Useful commands:"
echo "  Status:  $DC ps"
echo "  Logs:    $DC logs -f [service]"
echo "  Stop:    $0 down"
