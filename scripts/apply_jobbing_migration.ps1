#!/usr/bin/env pwsh
# PowerShell script to apply Jobbing strategy database migration

$ErrorActionPreference = "Stop"

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Jobbing Strategy Database Migration" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

# Load environment variables
$envFile = ".\services\user-config\.env"
if (Test-Path $envFile) {
    Write-Host "Loading environment from: $envFile" -ForegroundColor Yellow
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]*)\s*=\s*(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
            Write-Host "  $key = $value" -ForegroundColor DarkGray
        }
    }
} else {
    Write-Host "Warning: .env file not found at $envFile" -ForegroundColor Yellow
    Write-Host "Using environment variables or defaults" -ForegroundColor Yellow
}

# Database connection parameters
$DB_HOST = if ($env:DB_HOST) { $env:DB_HOST } else { "localhost" }
$DB_PORT = if ($env:DB_PORT) { $env:DB_PORT } else { "5432" }
$DB_USER = if ($env:DB_USER) { $env:DB_USER } else { "postgres" }
$DB_PASSWORD = if ($env:DB_PASSWORD) { $env:DB_PASSWORD } else { "postgres" }
$DB_NAME = if ($env:DB_NAME) { $env:DB_NAME } else { "user_config_db" }

Write-Host ""
Write-Host "Database Connection Parameters:" -ForegroundColor Cyan
Write-Host "  Host: $DB_HOST" -ForegroundColor White
Write-Host "  Port: $DB_PORT" -ForegroundColor White
Write-Host "  Database: $DB_NAME" -ForegroundColor White
Write-Host "  User: $DB_USER" -ForegroundColor White
Write-Host ""

# Set PGPASSWORD for psql
$env:PGPASSWORD = $DB_PASSWORD

# Migration file path
$migrationFile = ".\services\user-config\migrations\004_create_jobbing_configs.sql"

if (-not (Test-Path $migrationFile)) {
    Write-Host "Error: Migration file not found: $migrationFile" -ForegroundColor Red
    exit 1
}

Write-Host "Migration file: $migrationFile" -ForegroundColor Green
Write-Host ""

# Check if psql is available
try {
    $null = Get-Command psql -ErrorAction Stop
    Write-Host "[OK] PostgreSQL client (psql) found" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] PostgreSQL client (psql) not found in PATH" -ForegroundColor Red
    Write-Host "Please install PostgreSQL client tools or add to PATH" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "Applying migration..." -ForegroundColor Cyan

# Test database connection first
Write-Host "Testing database connection..." -ForegroundColor Yellow
$testQuery = "SELECT version()"
$testResult = psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c $testQuery 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Database connection failed!" -ForegroundColor Red
    Write-Host $testResult -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Database connection successful" -ForegroundColor Green
Write-Host ""

# Apply migration
Write-Host "Applying Jobbing strategy migration..." -ForegroundColor Cyan
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f $migrationFile

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[OK] Migration applied successfully!" -ForegroundColor Green
    Write-Host ""
    
    # Verify table creation
    Write-Host "Verifying table creation..." -ForegroundColor Cyan
    $verifyQuery = @"
SELECT 
    COUNT(*) as table_count,
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'jobbing_configs') as column_count,
    (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_name = 'jobbing_configs') as constraint_count
FROM information_schema.tables 
WHERE table_name = 'jobbing_configs';
"@
    
    $verifyResult = psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c $verifyQuery
    Write-Host $verifyResult -ForegroundColor White
    Write-Host ""
    
    # Show table structure
    Write-Host "Table structure:" -ForegroundColor Cyan
    $structureQuery = "\d jobbing_configs"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c $structureQuery
    
    Write-Host ""
    Write-Host "=====================================" -ForegroundColor Green
    Write-Host "Migration completed successfully!" -ForegroundColor Green
    Write-Host "=====================================" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "[ERROR] Migration failed!" -ForegroundColor Red
    Write-Host "Check the error messages above for details" -ForegroundColor Yellow
    exit 1
}

# Cleanup
$env:PGPASSWORD = $null
