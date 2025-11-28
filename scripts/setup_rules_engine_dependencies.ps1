# Setup Rules Engine Dependencies
# This script creates the required database table and Kafka topic

Write-Host "=== Setting up Rules Engine Dependencies ===" -ForegroundColor Cyan

# PostgreSQL Configuration
$POSTGRES_HOST = "localhost"
$POSTGRES_PORT = "5432"
$POSTGRES_USER = "postgres"
$POSTGRES_DB = "trading_db"
$PGPASSWORD = "postgres123"

# Kafka Configuration
$KAFKA_BROKER = "localhost:9092"

Write-Host "`n1. Creating trade_signals table in PostgreSQL..." -ForegroundColor Yellow

# Set PostgreSQL password environment variable
$env:PGPASSWORD = $PGPASSWORD

# Execute the migration SQL
$migrationFile = "D:\Algo_Trade\Algo-Treading\services\rules-engine\migrations\001_create_trade_signals_table.sql"

if (Test-Path $migrationFile) {
    try {
        $result = & psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -f $migrationFile 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ trade_signals table created successfully" -ForegroundColor Green
        } else {
            Write-Host "✗ Failed to create table: $result" -ForegroundColor Red
        }
    } catch {
        Write-Host "✗ Error executing migration: $_" -ForegroundColor Red
    }
} else {
    Write-Host "✗ Migration file not found: $migrationFile" -ForegroundColor Red
}

# Clear password
$env:PGPASSWORD = $null

Write-Host "`n2. Creating trade-signals Kafka topic..." -ForegroundColor Yellow

# Check if kafka-topics.bat exists
$kafkaTopicsCmd = "kafka-topics"

try {
    # Create trade-signals topic
    $createResult = & $kafkaTopicsCmd --bootstrap-server $KAFKA_BROKER `
        --create `
        --topic trade-signals `
        --partitions 3 `
        --replication-factor 1 `
        --if-not-exists 2>&1
    
    if ($LASTEXITCODE -eq 0 -or $createResult -match "already exists") {
        Write-Host "✓ trade-signals topic created/verified" -ForegroundColor Green
        
        # Describe the topic
        Write-Host "`n   Topic details:" -ForegroundColor Cyan
        & $kafkaTopicsCmd --bootstrap-server $KAFKA_BROKER --describe --topic trade-signals
    } else {
        Write-Host "✗ Failed to create topic: $createResult" -ForegroundColor Red
    }
} catch {
    Write-Host "✗ Error creating Kafka topic: $_" -ForegroundColor Red
    Write-Host "   Make sure Kafka is running and kafka-topics command is in your PATH" -ForegroundColor Yellow
}

Write-Host "`n3. Verifying database table..." -ForegroundColor Yellow
$env:PGPASSWORD = $PGPASSWORD
$verifyQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'trade_signals';"
$verifyResult = & psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -t -c $verifyQuery 2>&1
$env:PGPASSWORD = $null

if ($verifyResult -match "trade_signals") {
    Write-Host "✓ trade_signals table exists" -ForegroundColor Green
} else {
    Write-Host "✗ trade_signals table not found" -ForegroundColor Red
}

Write-Host "`n4. Verifying Kafka topic..." -ForegroundColor Yellow
try {
    $topicList = & $kafkaTopicsCmd --bootstrap-server $KAFKA_BROKER --list 2>&1
    if ($topicList -match "trade-signals") {
        Write-Host "✓ trade-signals topic exists" -ForegroundColor Green
    } else {
        Write-Host "✗ trade-signals topic not found" -ForegroundColor Red
    }
} catch {
    Write-Host "✗ Could not verify topic" -ForegroundColor Red
}

Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host "You can now restart the rules-engine service" -ForegroundColor Green
