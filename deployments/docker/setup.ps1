# Unified Setup Script for Trading System Infrastructure (Windows PowerShell)
# Usage:
#   .\setup.ps1              - Start all services
#   .\setup.ps1 postgres     - Start only PostgreSQL
#   .\setup.ps1 kafka        - Start only Kafka + Zookeeper + create topics
#   .\setup.ps1 redis        - Start only Redis
#   .\setup.ps1 rabbitmq     - Start only RabbitMQ
#   .\setup.ps1 down         - Stop all services
#   .\setup.ps1 ui           - Start all services + web UIs (Redis Commander, Kafka UI)

param(
    [Parameter(Position = 0)]
    [ValidateSet("all", "postgres", "redis", "rabbitmq", "kafka", "down", "ui")]
    [string]$Service = "all"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ComposeFile = Join-Path $ScriptDir "docker-compose.yml"

function Check-Docker {
    try {
        docker info 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw }
        Write-Host "Docker is running" -ForegroundColor Green
    } catch {
        Write-Host "Docker is not running. Please start Docker first." -ForegroundColor Red
        exit 1
    }
}

function Wait-ForContainer {
    param([string]$CheckCmd, [int]$MaxAttempts = 30)
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        try {
            Invoke-Expression $CheckCmd 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) { return $true }
        } catch {}
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
    }
    Write-Host ""
    return $false
}

function Setup-Postgres {
    Write-Host "Starting PostgreSQL..." -ForegroundColor Yellow
    docker compose -f $ComposeFile up -d postgres
    Write-Host "Waiting for PostgreSQL" -NoNewline
    if (Wait-ForContainer "docker exec trading-postgres pg_isready -U postgres -d trading_db") {
        Write-Host "`nPostgreSQL is ready!" -ForegroundColor Green
        Write-Host "  Host: localhost:5432 | DB: trading_db | User: postgres / postgres"
    } else {
        Write-Host "PostgreSQL failed to start" -ForegroundColor Red
    }
}

function Setup-Redis {
    Write-Host "Starting Redis..." -ForegroundColor Yellow
    docker compose -f $ComposeFile up -d redis
    Write-Host "Waiting for Redis" -NoNewline
    if (Wait-ForContainer "docker exec trading-redis redis-cli ping") {
        Write-Host "`nRedis is ready!" -ForegroundColor Green
        Write-Host "  Host: localhost:6379 | No password"
    } else {
        Write-Host "Redis failed to start" -ForegroundColor Red
    }
}

function Setup-RabbitMQ {
    Write-Host "Starting RabbitMQ..." -ForegroundColor Yellow
    docker compose -f $ComposeFile up -d rabbitmq
    Write-Host "Waiting for RabbitMQ" -NoNewline
    if (Wait-ForContainer "docker exec trading-rabbitmq rabbitmq-diagnostics -q ping" 30) {
        Write-Host "`nRabbitMQ is ready!" -ForegroundColor Green
        Write-Host "  AMQP: localhost:5672 | Management UI: http://localhost:15672 | User: guest / guest"
    } else {
        Write-Host "RabbitMQ failed to start" -ForegroundColor Red
    }
}

function Setup-Kafka {
    Write-Host "Starting Kafka + Zookeeper..." -ForegroundColor Yellow
    docker compose -f $ComposeFile up -d zookeeper kafka
    Write-Host "Waiting for Kafka" -NoNewline
    if (Wait-ForContainer "docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092" 30) {
        Write-Host "`nKafka is ready!" -ForegroundColor Green
    } else {
        Write-Host "Kafka failed to start" -ForegroundColor Red
        return
    }

    Write-Host "Creating Kafka topics..." -ForegroundColor Yellow
    $topics = @("user-config-events", "news-events", "trade-signals", "trade-executions", "risk-approvals", "order-updates")
    foreach ($topic in $topics) {
        docker exec trading-kafka kafka-topics --create `
            --bootstrap-server localhost:9092 `
            --replication-factor 1 `
            --partitions 3 `
            --topic $topic `
            --if-not-exists 2>$null
        Write-Host "  $topic" -ForegroundColor Green
    }
    Write-Host "  Broker: localhost:9092"
}

# ===== Main =====

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Trading System Infrastructure Setup" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

Check-Docker

switch ($Service) {
    "postgres"  { Setup-Postgres }
    "redis"     { Setup-Redis }
    "rabbitmq"  { Setup-RabbitMQ }
    "kafka"     { Setup-Kafka }
    "down" {
        Write-Host "Stopping all services..." -ForegroundColor Yellow
        docker compose -f $ComposeFile --profile ui down
        Write-Host "All services stopped" -ForegroundColor Green
    }
    "ui" {
        Write-Host "Starting all services with web UIs..." -ForegroundColor Yellow
        docker compose -f $ComposeFile --profile ui up -d
        Start-Sleep -Seconds 10
        Setup-Kafka
        Write-Host ""
        Write-Host "Web UIs:"
        Write-Host "  Kafka UI:         http://localhost:8082"
        Write-Host "  Redis Commander:  http://localhost:8081"
        Write-Host "  RabbitMQ:         http://localhost:15672"
    }
    default {
        Setup-Postgres; Write-Host ""
        Setup-Redis; Write-Host ""
        Setup-RabbitMQ; Write-Host ""
        Setup-Kafka
    }
}

Write-Host ""
Write-Host "================================================" -ForegroundColor Green
Write-Host "Done!" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  Status:  docker compose -f $ComposeFile ps"
Write-Host "  Logs:    docker compose -f $ComposeFile logs -f [service]"
Write-Host "  Stop:    .\setup.ps1 down"
