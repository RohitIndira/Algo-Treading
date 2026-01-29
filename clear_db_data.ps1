# PowerShell script to clear all data from databases while preserving schema
# This script truncates all tables without dropping them or their structure

$ContainerName = "trading-postgres"

# Helper function to run SQL commands inside Docker container
function Run-Sql {
    param(
        [string]$Database,
        [string]$SqlCommand,
        [bool]$IgnoreError = $false
    )
    
    # Use temporary file to avoid quoting issues with complex SQL
    $TempFile = [System.IO.Path]::GetTempFileName()
    $SqlCommand | Set-Content $TempFile
    
    $DestFile = "/tmp/sql_cmd.sql"
    docker cp $TempFile "${ContainerName}:${DestFile}" | Out-Null
    
    if ($IgnoreError) {
        docker exec $ContainerName psql -U postgres -d $Database -f $DestFile 2>$null
    } else {
        docker exec $ContainerName psql -U postgres -d $Database -f $DestFile
    }
    
    Remove-Item $TempFile
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Database Data Clearing Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Confirm before proceeding
Write-Host "WARNING: This will delete ALL data from the following databases:" -ForegroundColor Yellow
Write-Host "  - trading_db (all tables)" -ForegroundColor Yellow
Write-Host "  - trading_execution (all tables)" -ForegroundColor Yellow
Write-Host ""
$confirmation = Read-Host "Are you sure you want to proceed? (yes/no)"

if ($confirmation -ne "yes") {
    Write-Host "Operation cancelled." -ForegroundColor Red
    exit
}

Write-Host ""
Write-Host "Starting data clearing process..." -ForegroundColor Cyan
Write-Host ""

# Clear trading_db tables
Write-Host "Clearing trading_db tables..." -ForegroundColor Cyan

# Clear user-login-service tables
Write-Host "  -> Clearing user-login-service tables..."
Run-Sql "trading_db" @"
TRUNCATE TABLE login_history CASCADE;
TRUNCATE TABLE user_sessions CASCADE;
TRUNCATE TABLE user_credentials CASCADE;
"@

# Clear rules-engine tables
Write-Host "  -> Clearing rules-engine tables..."
Run-Sql "trading_db" @"
TRUNCATE TABLE trade_signals CASCADE;
"@

# Clear user-config tables (note: strategy_conditions and trade_configs will cascade)
Write-Host "  -> Clearing user-config tables..."
Run-Sql "trading_db" @"
TRUNCATE TABLE trade_configs CASCADE;
TRUNCATE TABLE strategy_conditions CASCADE;
TRUNCATE TABLE strategies CASCADE;
"@

# Clear risk-management tables
Write-Host "  -> Clearing risk-management tables..."
Run-Sql "trading_db" @"
TRUNCATE TABLE position_history CASCADE;
TRUNCATE TABLE risk_audit CASCADE;
TRUNCATE TABLE risk_limits CASCADE;
"@

Write-Host ""
Write-Host "Clearing trading_execution tables..." -ForegroundColor Cyan

# Clear trade-execution tables (execution_events will cascade)
Write-Host "  -> Clearing trade-execution tables..."
Run-Sql "trading_execution" @"
TRUNCATE TABLE execution_events CASCADE;
TRUNCATE TABLE orders CASCADE;
"@

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Data clearing completed successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# Verify tables are empty
Write-Host "Verifying data has been cleared..." -ForegroundColor Yellow
Write-Host ""

Write-Host "trading_db table counts:" -ForegroundColor Cyan
Run-Sql "trading_db" @"
SELECT 'user_credentials' as table_name, COUNT(*) as row_count FROM user_credentials
UNION ALL
SELECT 'user_sessions', COUNT(*) FROM user_sessions
UNION ALL
SELECT 'login_history', COUNT(*) FROM login_history
UNION ALL
SELECT 'trade_signals', COUNT(*) FROM trade_signals
UNION ALL
SELECT 'strategies', COUNT(*) FROM strategies
UNION ALL
SELECT 'strategy_conditions', COUNT(*) FROM strategy_conditions
UNION ALL
SELECT 'trade_configs', COUNT(*) FROM trade_configs
UNION ALL
SELECT 'risk_limits', COUNT(*) FROM risk_limits
UNION ALL
SELECT 'risk_audit', COUNT(*) FROM risk_audit
UNION ALL
SELECT 'position_history', COUNT(*) FROM position_history;
"@

Write-Host ""
Write-Host "trading_execution table counts:" -ForegroundColor Cyan
Run-Sql "trading_execution" @"
SELECT 'orders' as table_name, COUNT(*) as row_count FROM orders
UNION ALL
SELECT 'execution_events', COUNT(*) FROM execution_events;
"@

Write-Host ""
Write-Host "All data has been cleared. Schema remains intact." -ForegroundColor Green
