# DEPTH MARKET STRATEGY API - POWERSHELL TEST SCRIPT
# This script tests the new depth market strategy creation endpoint

param(
    [string]$ApiBaseUrl = "http://localhost:8080",
    [string]$BearerToken = "your_jwt_token_here",
    [string]$AppId = "your_app_id",
    [string]$Source = "WEB"
)

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "DEPTH MARKET STRATEGY API TEST SCRIPT" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "API Base URL: $ApiBaseUrl"
Write-Host "Bearer Token: $($BearerToken.Substring(0, [Math]::Min(20, $BearerToken.Length)))..."
Write-Host "App ID: $AppId"
Write-Host "Source: $Source"
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

$headers = @{
    "Content-Type" = "application/json"
    "Authorization" = "Bearer $BearerToken"
    "appId" = $AppId
    "source" = $Source
}

$endpoint = "$ApiBaseUrl/api/v1/strategies/depth-market/create"

# Test 1: Minimal request
Write-Host "TEST 1: MINIMAL DEPTH MARKET STRATEGY" -ForegroundColor Yellow
Write-Host "---" -ForegroundColor Yellow
$body1 = @{
    user_id = "test_user_001"
    strategy_name = "Minimal Depth Strategy"
    stock_codes = @(1, 2, 3)
    exchanges = @("NSE")
    order_type = "LIMIT"
    order_side = "BUY"
    quantity = 100
    exchange = "NSE"
    position_sizing = "FIXED"
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Headers $headers -Body $body1
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host ""

# Test 2: Comprehensive request
Write-Host "TEST 2: COMPREHENSIVE DEPTH MARKET STRATEGY" -ForegroundColor Yellow
Write-Host "---" -ForegroundColor Yellow
$body2 = @{
    user_id = "test_user_002"
    strategy_name = "Comprehensive Depth Strategy"
    description = "Full-featured depth market trading strategy"
    stock_codes = @(1, 2, 3, 4, 5)
    exchanges = @("NSE", "BSE")
    min_bid_quantity = 200
    min_ask_quantity = 200
    max_spread_pct = 0.3
    require_ltp_between_spread = $true
    price_range_min = 100.0
    price_range_max = 5000.0
    volume_threshold = 1000000
    min_market_cap = 100.0
    max_market_cap = 100000.0
    order_type = "LIMIT"
    order_side = "BUY"
    quantity = 100
    exchange = "NSE"
    limit_price = 245.50
    max_position_size = 100000.0
    stop_loss_pct = 2.0
    take_profit_pct = 3.0
    validity = "DAY"
    stop_loss_type = "FIXED"
    product_type = "INTRADAY"
    max_daily_trades = 20
    max_loss_per_day = 10000.0
    position_sizing = "FIXED"
    max_portfolio_exposure_pct = 10.0
    max_per_trade_risk = 1000.0
    enable_risk_checks = $true
    enable_auto_square_off = $true
    auto_square_off_time = "15:05"
    activate_immediately = $false
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Headers $headers -Body $body2
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host ""

# Test 3: Trailing stop loss strategy
Write-Host "TEST 3: TRAILING STOP LOSS STRATEGY" -ForegroundColor Yellow
Write-Host "---" -ForegroundColor Yellow
$body3 = @{
    user_id = "test_user_003"
    strategy_name = "Trailing Stop Depth Strategy"
    description = "Strategy with trailing stop loss for trend following"
    stock_codes = @(10, 11, 12)
    exchanges = @("NSE")
    min_bid_quantity = 300
    min_ask_quantity = 300
    max_spread_pct = 0.4
    order_type = "LIMIT"
    order_side = "BUY"
    quantity = 200
    exchange = "NSE"
    stop_loss_type = "TRAILING"
    trailing_sl_pct = 2.0
    take_profit_pct = 5.0
    validity = "DAY"
    product_type = "INTRADAY"
    position_sizing = "PERCENTAGE"
    enable_risk_checks = $true
    enable_auto_square_off = $true
    auto_square_off_time = "15:30"
    activate_immediately = $false
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Headers $headers -Body $body3
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host ""

# Test 4: Error case - missing required fields
Write-Host "TEST 4: ERROR CASE - MISSING REQUIRED FIELDS" -ForegroundColor Yellow
Write-Host "---" -ForegroundColor Yellow
$body4 = @{
    user_id = "test_user_004"
    strategy_name = "Incomplete Strategy"
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Headers $headers -Body $body4
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
} catch {
    Write-Host "Error (Expected): $($_.Exception.Message)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host ""

# Test 5: Error case - invalid quantity
Write-Host "TEST 5: ERROR CASE - INVALID QUANTITY" -ForegroundColor Yellow
Write-Host "---" -ForegroundColor Yellow
$body5 = @{
    user_id = "test_user_005"
    strategy_name = "Invalid Quantity Strategy"
    stock_codes = @(1, 2)
    exchanges = @("NSE")
    order_type = "LIMIT"
    order_side = "BUY"
    quantity = 0
    exchange = "NSE"
    position_sizing = "FIXED"
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Headers $headers -Body $body5
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
} catch {
    Write-Host "Error (Expected): $($_.Exception.Message)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host ""

# Test 6: Conservative strategy
Write-Host "TEST 6: CONSERVATIVE STRATEGY" -ForegroundColor Yellow
Write-Host "---" -ForegroundColor Yellow
$body6 = @{
    user_id = "test_user_006"
    strategy_name = "Conservative Depth Trading"
    description = "Low-risk strategy with strict controls"
    stock_codes = @(20, 21, 22)
    exchanges = @("NSE")
    min_bid_quantity = 500
    min_ask_quantity = 500
    max_spread_pct = 0.25
    require_ltp_between_spread = $true
    price_range_min = 500.0
    price_range_max = 2000.0
    volume_threshold = 5000000
    min_market_cap = 1000.0
    order_type = "LIMIT"
    order_side = "BUY"
    quantity = 50
    exchange = "NSE"
    limit_price = 1250.0
    max_position_size = 50000.0
    stop_loss_pct = 1.5
    take_profit_pct = 2.5
    stop_loss_type = "FIXED"
    validity = "DAY"
    product_type = "INTRADAY"
    max_daily_trades = 10
    max_loss_per_day = 5000.0
    position_sizing = "FIXED"
    max_portfolio_exposure_pct = 5.0
    max_per_trade_risk = 500.0
    enable_risk_checks = $true
    enable_auto_square_off = $true
    auto_square_off_time = "15:00"
    activate_immediately = $false
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Headers $headers -Body $body6
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host ""

# Test 7: Aggressive strategy
Write-Host "TEST 7: AGGRESSIVE GROWTH STRATEGY" -ForegroundColor Yellow
Write-Host "---" -ForegroundColor Yellow
$body7 = @{
    user_id = "test_user_007"
    strategy_name = "Aggressive Growth Strategy"
    description = "High-risk, high-reward strategy"
    stock_codes = @(30, 31, 32, 33, 34, 35)
    exchanges = @("NSE")
    min_bid_quantity = 100
    min_ask_quantity = 100
    max_spread_pct = 0.5
    order_type = "LIMIT"
    order_side = "BUY"
    quantity = 500
    exchange = "NSE"
    stop_loss_pct = 1.0
    take_profit_pct = 5.0
    stop_loss_type = "FIXED"
    validity = "DAY"
    product_type = "INTRADAY"
    max_daily_trades = 50
    max_loss_per_day = 20000.0
    position_sizing = "PERCENTAGE"
    max_portfolio_exposure_pct = 25.0
    max_per_trade_risk = 2000.0
    enable_risk_checks = $true
    enable_auto_square_off = $true
    auto_square_off_time = "15:20"
    activate_immediately = $false
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method POST -Headers $headers -Body $body7
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "TEST SUITE COMPLETED" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
