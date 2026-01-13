Write-Host "================================================"
Write-Host "PostgreSQL Database Setup for All Services (Docker)"
Write-Host "================================================"
Write-Host ""

$ErrorActionPreference = "Stop"

# ---------- Colors ----------
$GREEN  = "Green"
$YELLOW = "Yellow"
$RED    = "Red"
$BLUE   = "Cyan"

# ---------- Docker / DB config ----------
$POSTGRES_CONTAINER = "635a924dcf23"   # <-- your container ID
$DB_USER     = "postgres"
$DB_PASSWORD = "postgres"

$USER_CONFIG_DB     = "trading_db"
$TRADE_EXECUTION_DB = "trading_execution"
$RULES_ENGINE_DB    = "trading_db"
$USER_LOGIN_DB      = "trading_db"

# ---------- Helper: run psql inside Docker ----------
function Run-Psql {
    param (
        [string]$Database,
        [string]$Command = "",
        [string]$File = ""
    )

    if ($File -ne "") {
        docker exec -e PGPASSWORD=$DB_PASSWORD `
            $POSTGRES_CONTAINER `
            psql -U $DB_USER -d $Database -f $File
    } else {
        docker exec -e PGPASSWORD=$DB_PASSWORD `
            $POSTGRES_CONTAINER `
            psql -U $DB_USER -d $Database -c $Command
    }

    return ($LASTEXITCODE -eq 0)
}

# ---------- Step 0: Check Docker container ----------
Write-Host "Checking PostgreSQL Docker container..." -ForegroundColor $BLUE

docker inspect $POSTGRES_CONTAINER > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] PostgreSQL container not found or not running" -ForegroundColor $RED
    exit 1
}

Write-Host "[OK] PostgreSQL container is running" -ForegroundColor $GREEN
Write-Host ""

# ---------- Step 1: Set postgres password ----------
Write-Host "Step 1: Setting postgres user password..." -ForegroundColor $YELLOW
Run-Psql -Database "postgres" -Command "ALTER USER postgres PASSWORD 'postgres';" | Out-Null
Write-Host "[OK] Password configured" -ForegroundColor $GREEN
Write-Host ""

# ---------- Step 2: Create databases ----------
Write-Host "Step 2: Creating databases..." -ForegroundColor $YELLOW

foreach ($db in @($USER_CONFIG_DB, $TRADE_EXECUTION_DB)) {
    Write-Host "Creating database: $db"
    if (Run-Psql -Database "postgres" -Command "CREATE DATABASE $db;") {
        Write-Host "[OK] Database '$db' created" -ForegroundColor $GREEN
    } else {
        Write-Host "[WARN] Database '$db' may already exist" -ForegroundColor $YELLOW
    }
}

# ---------- Create trading_user ----------
Write-Host "Creating user: trading_user"
if (Run-Psql -Database "postgres" -Command "CREATE USER trading_user WITH PASSWORD 'your_secure_password';") {
    Write-Host "[OK] User 'trading_user' created" -ForegroundColor $GREEN
} else {
    Write-Host "[WARN] User 'trading_user' may already exists" -ForegroundColor $YELLOW
}

Run-Psql -Database "postgres" -Command "GRANT ALL PRIVILEGES ON DATABASE $TRADE_EXECUTION_DB TO trading_user;" | Out-Null
Write-Host ""

# ---------- Step 3: Run migrations ----------
Write-Host "Step 3: Running database migrations..." -ForegroundColor $YELLOW
Write-Host ""

function Run-Migrations {
    param (
        [string]$ServiceName,
        [string]$MigrationsDir,
        [string]$Database
    )

    Write-Host "=== $ServiceName ===" -ForegroundColor $BLUE

    if (-not (Test-Path $MigrationsDir)) {
        Write-Host "[WARN] No migrations found for $ServiceName" -ForegroundColor $YELLOW
        Write-Host ""
        return
    }

    Get-ChildItem $MigrationsDir -Filter *.sql | Sort-Object Name | ForEach-Object {
        Write-Host "Running migration: $($_.Name)"
        if (Run-Psql -Database $Database -File $_.FullName) {
            Write-Host "[OK] Migration completed: $($_.Name)" -ForegroundColor $GREEN
        } else {
            Write-Host "[FAIL] Migration failed: $($_.Name)" -ForegroundColor $RED
        }
    }

    Write-Host ""
}

Run-Migrations "User Config Service"      "..\services\user-config\migrations"        $USER_CONFIG_DB
Run-Migrations "Trade Execution Service"  "..\services\trade-execution\migrations"    $TRADE_EXECUTION_DB
Run-Migrations "User Login Service"       "..\services\user-login-service\migrations" $USER_LOGIN_DB
Run-Migrations "Rules Engine Service"     "..\services\rules-engine\migrations"       $RULES_ENGINE_DB
Run-Migrations "Risk Management Service"  "..\services\risk-management\migrations"    $USER_CONFIG_DB

# ---------- Permissions ----------
Run-Psql -Database $TRADE_EXECUTION_DB -Command "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading_user;" | Out-Null
Run-Psql -Database $TRADE_EXECUTION_DB -Command "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading_user;" | Out-Null

# ---------- Step 4: Verify ----------
Write-Host "Step 4: Verifying tables..." -ForegroundColor $YELLOW
Run-Psql -Database $USER_CONFIG_DB     -Command "\dt"
Run-Psql -Database $TRADE_EXECUTION_DB -Command "\dt"

# ---------- Summary ----------
Write-Host "================================================" -ForegroundColor $GREEN
Write-Host "Database Setup Complete!" -ForegroundColor $GREEN
Write-Host "================================================" -ForegroundColor $GREEN
Write-Host ""

Write-Host "Databases:"
Write-Host " - $USER_CONFIG_DB"
Write-Host " - $TRADE_EXECUTION_DB"
Write-Host ""
Write-Host "[WARN] Change default passwords in production!" -ForegroundColor $YELLOW
