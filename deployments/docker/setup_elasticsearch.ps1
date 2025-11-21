# Elasticsearch Setup Script for Windows
# This script sets up and starts Elasticsearch using Docker Compose

Write-Host "=== Elasticsearch Setup for Trading System ===" -ForegroundColor Cyan
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
Write-Host "Stopping existing Elasticsearch containers..." -ForegroundColor Yellow
docker-compose -f docker-compose-elasticsearch.yml down -v
Write-Host ""

# Start Elasticsearch
Write-Host "Starting Elasticsearch..." -ForegroundColor Yellow
docker-compose -f docker-compose-elasticsearch.yml up -d

# Wait for Elasticsearch to be ready
Write-Host ""
Write-Host "Waiting for Elasticsearch to be ready..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
$ready = $false

while (-not $ready -and $attempt -lt $maxAttempts) {
    $attempt++
    Start-Sleep -Seconds 2
    
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:9200/_cluster/health" -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            $ready = $true
        }
    } catch {
        Write-Host "." -NoNewline
    }
}

Write-Host ""
if ($ready) {
    Write-Host "Elasticsearch is ready!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Elasticsearch URL: http://localhost:9200" -ForegroundColor Cyan
    Write-Host "Kibana URL: http://localhost:5601" -ForegroundColor Cyan
    Write-Host ""
    
    # Display cluster health
    Write-Host "Cluster Health:" -ForegroundColor Yellow
    $health = Invoke-RestMethod -Uri "http://localhost:9200/_cluster/health" -Method Get
    Write-Host "Status: $($health.status)" -ForegroundColor Green
    Write-Host "Number of nodes: $($health.number_of_nodes)" -ForegroundColor Green
    Write-Host ""
    
    Write-Host "Setup complete! You can now run your Rules Engine service." -ForegroundColor Green
} else {
    Write-Host "Elasticsearch failed to start within the timeout period." -ForegroundColor Red
    Write-Host "Check logs with: docker-compose -f docker-compose-elasticsearch.yml logs" -ForegroundColor Yellow
    exit 1
}

# Display running containers
Write-Host ""
Write-Host "Running containers:" -ForegroundColor Yellow
docker-compose -f docker-compose-elasticsearch.yml ps
