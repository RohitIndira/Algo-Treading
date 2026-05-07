#!/bin/bash
# Script to manage Algo-Trading environments (Live and Staging)

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Algo-Trading - Live & Staging Environment Manager      ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Function to display menu
show_menu() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "LIVE Setup (Original - docker-compose.yml)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  API Gateway:        http://localhost:8081"
    echo "  PostgreSQL:         5432"
    echo "  MongoDB:            27017"
    echo "  Redis:              6379"
    echo "  Kafka:              9092 (internal), 29092 (external)"
    echo "  RabbitMQ:           5672 (AMQP), 15672 (UI)"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "STAGING Setup (New - docker-compose.staging.yml)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  API Gateway:        http://localhost:8181"
    echo "  PostgreSQL:         5433"
    echo "  MongoDB:            27018"
    echo "  Redis:              6380"
    echo "  Kafka:              9093 (internal), 29093 (external)"
    echo "  RabbitMQ:           5673 (AMQP), 15673 (UI)"
    echo ""
    echo "Choose an option:"
    echo "  1) Start LIVE environment"
    echo "  2) Start STAGING environment"
    echo "  3) Start BOTH (Live + Staging)"
    echo "  4) Stop LIVE environment"
    echo "  5) Stop STAGING environment"
    echo "  6) Stop ALL environments"
    echo "  7) View logs (LIVE)"
    echo "  8) View logs (STAGING)"
    echo "  9) Status check"
    echo "  0) Exit"
    echo ""
}

# Main loop
while true; do
    show_menu
    read -p "Enter your choice [0-9]: " choice
    echo ""

    case $choice in
        1)
            echo "🚀 Starting LIVE environment..."
            cd "$PROJECT_ROOT"
            docker-compose up -d
            echo "✅ LIVE environment started!"
            echo "   API Gateway: http://localhost:8081"
            sleep 2
            ;;
        2)
            echo "🚀 Starting STAGING environment..."
            cd "$PROJECT_ROOT"
            docker-compose -f docker-compose.staging.yml up -d
            echo "✅ STAGING environment started!"
            echo "   API Gateway: http://localhost:8181"
            sleep 2
            ;;
        3)
            echo "🚀 Starting BOTH environments..."
            cd "$PROJECT_ROOT"
            docker-compose up -d
            echo "✅ LIVE environment started (port 8081)"
            sleep 2
            docker-compose -f docker-compose.staging.yml up -d
            echo "✅ STAGING environment started (port 8181)"
            sleep 2
            ;;
        4)
            echo "🛑 Stopping LIVE environment..."
            cd "$PROJECT_ROOT"
            docker-compose down
            echo "✅ LIVE environment stopped!"
            ;;
        5)
            echo "🛑 Stopping STAGING environment..."
            cd "$PROJECT_ROOT"
            docker-compose -f docker-compose.staging.yml down
            echo "✅ STAGING environment stopped!"
            ;;
        6)
            echo "🛑 Stopping ALL environments..."
            cd "$PROJECT_ROOT"
            docker-compose down
            docker-compose -f docker-compose.staging.yml down
            echo "✅ All environments stopped!"
            ;;
        7)
            echo "📋 LIVE logs (Press Ctrl+C to stop)..."
            cd "$PROJECT_ROOT"
            docker-compose logs -f
            ;;
        8)
            echo "📋 STAGING logs (Press Ctrl+C to stop)..."
            cd "$PROJECT_ROOT"
            docker-compose -f docker-compose.staging.yml logs -f
            ;;
        9)
            echo "📊 Environment Status Check:"
            echo ""
            echo "LIVE Containers:"
            docker ps --filter "label!=com.docker.compose.project=algotreading-staging" --format "table {{.Names}}\t{{.Status}}" | grep trading- || echo "  No containers running"
            echo ""
            echo "STAGING Containers:"
            docker ps --filter "label=com.docker.compose.project=algotreading-staging" --format "table {{.Names}}\t{{.Status}}" || echo "  No containers running"
            ;;
        0)
            echo "👋 Goodbye!"
            exit 0
            ;;
        *)
            echo "❌ Invalid option. Please try again."
            ;;
    esac
    echo ""
    read -p "Press Enter to continue..."
    clear
done
