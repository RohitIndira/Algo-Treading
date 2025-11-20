# RabbitMQ Setup Script for Windows
# This script sets up and starts RabbitMQ using Docker Compose

Write-Host "=== RabbitMQ Setup for Trading System ===" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Yellow
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}
Write-Host "Docker is running." -ForegroundColor Green
Write-Host ""

# Navigate to the docker directory
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

# Stop and remove existing containers
Write-Host "Stopping existing RabbitMQ containers..." -ForegroundColor Yellow
docker-compose -f docker-compose-rabbitmq.yml down -v
Write-Host ""

# Start RabbitMQ
Write-Host "Starting RabbitMQ..." -ForegroundColor Yellow
docker-compose -f docker-compose-rabbitmq.yml up -d

# Wait for RabbitMQ to be ready
Write-Host ""
Write-Host "Waiting for RabbitMQ to be ready..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
$ready = $false

while (-not $ready -and $attempt -lt $maxAttempts) {
    $attempt++
    Start-Sleep -Seconds 2
    
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:15672" -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            $ready = $true
        }
    } catch {
        Write-Host "." -NoNewline
    }
}

Write-Host ""
if ($ready) {
    Write-Host "RabbitMQ is ready!" -ForegroundColor Green
    Write-Host ""
    Write-Host "RabbitMQ AMQP Port: amqp://localhost:5672" -ForegroundColor Cyan
    Write-Host "RabbitMQ Management UI: http://localhost:15672" -ForegroundColor Cyan
    Write-Host "Default Credentials: admin / admin123" -ForegroundColor Cyan
    Write-Host ""
    
    Write-Host "Setup complete! You can now run your services." -ForegroundColor Green
} else {
    Write-Host "RabbitMQ failed to start within the timeout period." -ForegroundColor Red
    Write-Host "Check logs with: docker-compose -f docker-compose-rabbitmq.yml logs" -ForegroundColor Yellow
    exit 1
}

# Display running containers
Write-Host ""
Write-Host "Running containers:" -ForegroundColor Yellow
docker-compose -f docker-compose-rabbitmq.yml ps
