$ApiUrl = "http://localhost:8080"
$BearerToken = "your_jwt_token_here"
$AppId = "your_app_id"
$Source = "WEB"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Creating Depth Market Strategy" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

$headers = @{
    "Content-Type" = "application/json"
    "Authorization" = "Bearer $BearerToken"
    "appId" = $AppId
    "source" = $Source
}

$body = @{
    user_id = "test_user_001"
    strategy_name = "Balanced Market Depth Strategy"
    description = "Trade stocks with good liquidity and tight spreads based on market depth analysis"
    stock_codes = @(1, 2, 3, 4, 5)
    exchanges = @("NSE")
    impact_score_threshold = 5
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
}

$bodyJson = $body | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$ApiUrl/api/v1/strategies/depth-market/create" `
        -Method POST `
        -Headers $headers `
        -Body $bodyJson

    Write-Host "✓ Strategy Created Successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
}
catch {
    Write-Host "✗ Error Creating Strategy:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    if ($_.Exception.Response) {
        $errorContent = $_.Exception.Response.Content.ReadAsStringAsync().Result
        Write-Host "Response: $errorContent" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
