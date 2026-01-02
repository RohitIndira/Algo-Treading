param(
    [string]$DbHost = "localhost",
    [string]$DbPort = "5432",
    [string]$DbUser = "postgres",
    [string]$DbPassword = "postgres",
    [string]$DbName = "trading_db",
    [string]$ElasticsearchUrl = "http://localhost:9200",
    [string]$RedisHost = "localhost",
    [string]$RedisPort = "6379"
)

Write-Host "Clearing All Strategies from All Systems" -ForegroundColor Cyan
Write-Host ""

# 1. Clear PostgreSQL
Write-Host "1. Clearing PostgreSQL Database..." -ForegroundColor Yellow
$env:PGPASSWORD = $DbPassword
try {
    $sql = "DELETE FROM trade_signals; DELETE FROM trade_configs; DELETE FROM risk_limits; DELETE FROM strategy_conditions; DELETE FROM strategies; SELECT COUNT(*) as count FROM strategies;"
    $sql | psql -h $DbHost -U $DbUser -d $DbName 2>&1
    Write-Host "   OK: PostgreSQL cleared" -ForegroundColor Green
} catch {
    Write-Host "   ERROR: $_" -ForegroundColor Red
}
Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue

Write-Host ""

# 2. Clear Elasticsearch
Write-Host "2. Clearing Elasticsearch..." -ForegroundColor Yellow
try {
    $indexName = "user_strategies"
    Invoke-WebRequest -Uri "$ElasticsearchUrl/$indexName" -Method DELETE -ErrorAction SilentlyContinue | Out-Null
    Write-Host "   OK: Elasticsearch cleared" -ForegroundColor Green
} catch {
    Write-Host "   INFO: Index may not exist" -ForegroundColor Cyan
}

Write-Host ""

# 3. Clear Redis Cache
Write-Host "3. Clearing Redis Cache..." -ForegroundColor Yellow
try {
    if (Get-Command redis-cli -ErrorAction SilentlyContinue) {
        redis-cli -h $RedisHost -p $RedisPort FLUSHALL 2>&1 | Out-Null
        Write-Host "   OK: Redis cleared" -ForegroundColor Green
    } else {
        Write-Host "   SKIP: redis-cli not found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "   ERROR: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "Cleanup Complete!" -ForegroundColor Cyan
