Write-Host "================================================"
Write-Host "Kafka Setup for Trading System"
Write-Host "================================================"
Write-Host ""

$GREEN  = "Green"
$YELLOW = "Yellow"
$RED    = "Red"

# Change to script directory
Set-Location -Path $PSScriptRoot

Write-Host "Step 1: Checking Docker..." -ForegroundColor $YELLOW

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker is not installed" -ForegroundColor $RED
    Write-Host "Install Docker Desktop from:"
    Write-Host "https://docs.docker.com/desktop/"
    exit 1
}

# Detect docker compose command
$DOCKER_COMPOSE_CMD = ""

$composePlugin = $false
try {
    docker compose version | Out-Null
    $composePlugin = $true
}
catch {
    $composePlugin = $false
}

if ($composePlugin) {
    $DOCKER_COMPOSE_CMD = "docker compose"
    Write-Host "Docker Compose plugin found" -ForegroundColor $GREEN
}
elseif (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    $DOCKER_COMPOSE_CMD = "docker-compose"
    Write-Host "Docker Compose standalone found" -ForegroundColor $GREEN
}
else {
    Write-Host "Docker Compose is not installed" -ForegroundColor $RED
    exit 1
}

Write-Host ""

Write-Host "Step 2: Stopping existing Kafka containers and removing old data..." -ForegroundColor $YELLOW
Invoke-Expression "$DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml down -v" | Out-Null
Write-Host "Cleanup complete" -ForegroundColor $GREEN
Write-Host ""

Write-Host "Step 3: Starting Kafka services..." -ForegroundColor $YELLOW
Invoke-Expression "$DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml up -d"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to start Kafka services" -ForegroundColor $RED
    exit 1
}

Write-Host "Kafka services started" -ForegroundColor $GREEN
Write-Host ""

Write-Host "Step 4: Waiting for Kafka to be ready (this may take 30-60 seconds)..." -ForegroundColor $YELLOW
Write-Host "Checking Kafka readiness..."

$MAX_ATTEMPTS = 30
$ATTEMPT = 0
$READY = $false

while ($ATTEMPT -lt $MAX_ATTEMPTS) {
    docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>$null
    if ($LASTEXITCODE -eq 0) {
        $READY = $true
        Write-Host "Kafka is ready" -ForegroundColor $GREEN
        break
    }

    Write-Host "." -NoNewline
    Start-Sleep -Seconds 2
    $ATTEMPT++
}

Write-Host ""

if (-not $READY) {
    Write-Host "Kafka did not become ready in time" -ForegroundColor $RED
    Write-Host "Check logs with:"
    Write-Host "$DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml logs kafka"
    exit 1
}

Write-Host ""
Write-Host "Step 5: Creating required topics..." -ForegroundColor $YELLOW

$topics = @(
    "user-configs",
    "news-events",
    "market.data.news",
    "market.data.52w_breakouts",
    "trade-signals",
    "trade-executions",
    "risk-approvals",
    "order-updates",
    "portfolio.allocations"
    "market.data.live"
)

foreach ($topic in $topics) {
    Write-Host "Creating topic: $topic"
    docker exec trading-kafka kafka-topics `
        --create `
        --bootstrap-server localhost:9092 `
        --replication-factor 1 `
        --partitions 3 `
        --topic $topic `
        --if-not-exists 2>$null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "Topic $topic ready" -ForegroundColor $GREEN
    }
    else {
        Write-Host "Topic $topic already exists" -ForegroundColor $YELLOW
    }
}

Write-Host ""
Write-Host "Step 6: Verifying setup..." -ForegroundColor $YELLOW

Write-Host ""
Write-Host "Available topics:"
docker exec trading-kafka kafka-topics --list --bootstrap-server localhost:9092

Write-Host ""
Write-Host "Kafka cluster info:"
docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 | Select-Object -First 1

Write-Host ""
Write-Host "================================================" -ForegroundColor $GREEN
Write-Host "Kafka Setup Complete" -ForegroundColor $GREEN
Write-Host "================================================" -ForegroundColor $GREEN
Write-Host ""

Write-Host "Services running:"
Invoke-Expression "$DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml ps"

Write-Host ""
Write-Host "Kafka UI: http://localhost:8082" -ForegroundColor $GREEN
Write-Host "Kafka broker: localhost:9092" -ForegroundColor $GREEN
Write-Host ""
Write-Host "Useful commands:"
Write-Host "View logs: $DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml logs -f"
Write-Host "Stop Kafka: $DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml down"
Write-Host "Restart: $DOCKER_COMPOSE_CMD -f docker-compose-kafka.yml restart"
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Update your .env file:"
Write-Host "   KAFKA_ENABLED=true"
Write-Host "   KAFKA_BROKERS=localhost:9092"
Write-Host "2. Restart your services"
Write-Host "3. Check Kafka UI for messages: http://localhost:8082"
Write-Host ""
