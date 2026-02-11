#!/usr/bin/env powershell
# Simple Jobbing API Test Script

$BASE_URL = "http://localhost:8080/api/v1"
$USER_ID = "testuser1"

Write-Host "=== Testing Jobbing Strategy API ===" -ForegroundColor Cyan
Write-Host "Base URL: $BASE_URL" -ForegroundColor White
Write-Host "Test User: $USER_ID" -ForegroundColor White
Write-Host ""

# Test 1: Configure Jobbing Strategy
Write-Host "1. Testing Configure Jobbing Strategy..." -ForegroundColor Yellow

$configPayload = @{
    user_id = $USER_ID
    configs = @(
        @{
            token = "22"
            symbol = "SBIN"
            exchange = "NSE"
            lower_range = 10.0
            higher_range = 15.0
            initial_buy_offset = 0.01
            distance_continue = 0.01
            quantity_per_order = 1
            max_quantity = 10
            trading_mode = "PAPER"
            enabled = $true
        },
        @{
            token = "2475"
            symbol = "RELIANCE"
            exchange = "NSE"
            lower_range = 2400.0
            higher_range = 2800.0
            initial_buy_offset = 0.50
            distance_continue = 0.50
            quantity_per_order = 1
            max_quantity = 5
            trading_mode = "PAPER"
            enabled = $true
        }
    )
} | ConvertTo-Json -Depth 3

try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/strategies/jobbing/configure" -Method POST -Body $configPayload -ContentType "application/json"
    Write-Host "✓ Configure Success: $($response.message)" -ForegroundColor Green
    Write-Host "  Created $($response.total_count) configurations" -ForegroundColor White
} catch {
    Write-Host "✗ Configure Failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 2: Get All Jobbing Configurations  
Write-Host ""
Write-Host "2. Testing Get All Jobbing Configurations..." -ForegroundColor Yellow

try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/strategies/jobbing?user_id=$USER_ID" -Method GET
    Write-Host "✓ Get All Success: Found $($response.configs.Count) configurations" -ForegroundColor Green
    foreach ($config in $response.configs) {
        Write-Host "  - Token: $($config.token), Symbol: $($config.symbol), Enabled: $($config.enabled)" -ForegroundColor White
    }
} catch {
    Write-Host "✗ Get All Failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 3: Get Specific Token Configuration
Write-Host ""
Write-Host "3. Testing Get Specific Token Configuration..." -ForegroundColor Yellow

try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/strategies/jobbing/22?user_id=$USER_ID" -Method GET
    Write-Host "✓ Get Token Success: Token $($response.config.token) - $($response.config.symbol)" -ForegroundColor Green
    Write-Host "  Range: $($response.config.lower_range) - $($response.config.higher_range)" -ForegroundColor White
    Write-Host "  Max Quantity: $($response.config.max_quantity)" -ForegroundColor White
} catch {
    Write-Host "✗ Get Token Failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 4: Update Configuration
Write-Host ""
Write-Host "4. Testing Update Configuration..." -ForegroundColor Yellow

$updatePayload = @{
    user_id = $USER_ID
    max_quantity = 15
    enabled = $true
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/strategies/jobbing/22" -Method PUT -Body $updatePayload -ContentType "application/json"
    Write-Host "✓ Update Success: $($response.message)" -ForegroundColor Green
} catch {
    Write-Host "✗ Update Failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 5: Disable Configuration
Write-Host ""
Write-Host "5. Testing Disable Configuration..." -ForegroundColor Yellow

$disablePayload = @{
    user_id = $USER_ID
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/strategies/jobbing/2475/disable" -Method POST -Body $disablePayload -ContentType "application/json"
    Write-Host "✓ Disable Success: $($response.message)" -ForegroundColor Green
} catch {
    Write-Host "✗ Disable Failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 6: Enable Configuration
Write-Host ""
Write-Host "6. Testing Enable Configuration..." -ForegroundColor Yellow

$enablePayload = @{
    user_id = $USER_ID
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/strategies/jobbing/2475/enable" -Method POST -Body $enablePayload -ContentType "application/json"
    Write-Host "✓ Enable Success: $($response.message)" -ForegroundColor Green
} catch {
    Write-Host "✗ Enable Failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

Write-Host ""
Write-Host "=== Test Complete ===" -ForegroundColor Cyan
Write-Host "Check the rules-engine logs to see if configurations were received via Kafka" -ForegroundColor Yellow