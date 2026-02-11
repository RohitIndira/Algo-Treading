# Simple Jobbing API Tests
$API_BASE_URL = "http://localhost:8080"
$USER_ID = "ISPL19027"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Jobbing Strategy API Tests" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Test 1: Configure Jobbing Strategy
Write-Host "Test 1: Configure Jobbing Strategy..." -ForegroundColor Yellow
$configBody = @{
    user_id = $USER_ID
    configs = @(
        @{
            token = "30274"
            symbol = "SILVERCASE"
            exchange = "NSE"
            lower_range = 10.0
            higher_range = 15.0
            initial_buy_offset = 0.01
            distance_continue = 0.01
            quantity_per_order = 1
            max_quantity = 10
            trading_mode = "PAPER"
            enabled = $true
        }
    )
} | ConvertTo-Json -Depth 10

try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/strategies/jobbing/configure" `
        -Method POST -ContentType "application/json" -Body $configBody
    Write-Host "Success: $($response | ConvertTo-Json -Depth 5)" -ForegroundColor Green
} catch {
    Write-Host "Failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 2: Get All Configs
Write-Host "Test 2: Get All Configs..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/strategies/jobbing?user_id=$USER_ID" -Method GET
    Write-Host "Success: Found $($response.configs.Count) configs" -ForegroundColor Green
} catch {
    Write-Host "Failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 3: Get Single Config
Write-Host "Test 3: Get Single Config for token 30274..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/strategies/jobbing/30274?user_id=$USER_ID" -Method GET
    Write-Host "Success: Token=$($response.config.token) Symbol=$($response.config.symbol)" -ForegroundColor Green
} catch {
    Write-Host "Failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 4: Update Config
Write-Host "Test 4: Update Config..." -ForegroundColor Yellow
$updateBody = @{
    user_id = $USER_ID
    max_quantity = 15
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/strategies/jobbing/30274" `
        -Method PUT -ContentType "application/json" -Body $updateBody
    Write-Host "Success: max_quantity updated to $($response.config.max_quantity)" -ForegroundColor Green
} catch {
    Write-Host "Failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 5: Disable Config
Write-Host "Test 5: Disable Config..." -ForegroundColor Yellow
$disableBody = @{
    user_id = $USER_ID
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/strategies/jobbing/30274/disable" `
        -Method POST -ContentType "application/json" -Body $disableBody
    Write-Host "Success: $($response.message)" -ForegroundColor Green
} catch {
    Write-Host "Failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 6: Enable Config
Write-Host "Test 6: Enable Config..." -ForegroundColor Yellow
$enableBody = @{
    user_id = $USER_ID
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/strategies/jobbing/30274/enable" `
        -Method POST -ContentType "application/json" -Body $enableBody
    Write-Host "Success: $($response.message)" -ForegroundColor Green
} catch {
    Write-Host "Failed: $_" -ForegroundColor Red
}
Write-Host ""

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Tests Complete!" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
