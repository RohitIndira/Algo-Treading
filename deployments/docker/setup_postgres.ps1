# PostgreSQL Docker Setup Script for Trading System (Windows PowerShell)
# This script sets up PostgreSQL in Docker for development

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "PostgreSQL Docker Setup" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Check if Docker is installed
try {
    $dockerVersion = docker --version
    Write-Host "✅ Docker is installed: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Error: Docker is not installed" -ForegroundColor Red
    Write-Host "Please install Docker Desktop: https://docs.docker.com/desktop/install/windows-install/" -ForegroundColor Yellow
    exit 1
}

# Check if Docker is running
try {
    docker info | Out-Null
    Write-Host "✅ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "❌ Error: Docker is not running" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again" -ForegroundColor Yellow
    exit 1
}

# Navigate to docker directory
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

Write-Host ""
Write-Host "Starting PostgreSQL container..." -ForegroundColor Yellow
docker-compose -f docker-compose-postgres.yml up -d

Write-Host ""
Write-Host "Waiting for PostgreSQL to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Wait for PostgreSQL to be healthy
$maxAttempts = 30
$attempt = 0
$ready = $false

while ($attempt -lt $maxAttempts) {
    $attempt++
    try {
        docker exec trading-postgres pg_isready -U postgres -d trading_db 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ PostgreSQL is ready!" -ForegroundColor Green
            $ready = $true
            break
        }
    } catch {
        # Ignore errors and continue waiting
    }
    Write-Host "Waiting for PostgreSQL... ($attempt/$maxAttempts)" -ForegroundColor Yellow
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    Write-Host "❌ PostgreSQL failed to start within the timeout period" -ForegroundColor Red
    Write-Host "Check logs with: docker-compose -f docker-compose-postgres.yml logs postgres" -ForegroundColor Yellow
    exit 1
}

# Verify connection
try {
    docker exec trading-postgres psql -U postgres -d trading_db -c "SELECT 1;" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Database connection successful" -ForegroundColor Green
        Write-Host ""
        Write-Host "==========================================" -ForegroundColor Cyan
        Write-Host "PostgreSQL Setup Complete!" -ForegroundColor Green
        Write-Host "==========================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "📊 Database Information:" -ForegroundColor Yellow
        Write-Host "  Host: localhost"
        Write-Host "  Port: 5432"
        Write-Host "  Database: trading_db"
        Write-Host "  Username: postgres"
        Write-Host "  Password: postgres"
        Write-Host ""
        Write-Host "📝 Connection String:" -ForegroundColor Yellow
        Write-Host "  postgresql://postgres:postgres@localhost:5432/trading_db?sslmode=disable"
        Write-Host ""
        Write-Host "🔧 Useful Commands:" -ForegroundColor Yellow
        Write-Host "  Stop:    docker-compose -f docker-compose-postgres.yml down"
        Write-Host "  Restart: docker-compose -f docker-compose-postgres.yml restart"
        Write-Host "  Logs:    docker-compose -f docker-compose-postgres.yml logs -f postgres"
        Write-Host "  Shell:   docker exec -it trading-postgres psql -U postgres -d trading_db"
        Write-Host ""
        Write-Host "📁 Migrations automatically applied from:" -ForegroundColor Yellow
        Write-Host "  services/user-config/migrations/"
        Write-Host ""
    } else {
        throw "Connection test failed"
    }
} catch {
    Write-Host "❌ Failed to connect to database" -ForegroundColor Red
    Write-Host "Check logs with: docker-compose -f docker-compose-postgres.yml logs postgres" -ForegroundColor Yellow
    exit 1
}
