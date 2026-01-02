$ApiUrl = "http://localhost:8080"
$BearerToken = "test_token"
$AppId = "test_app"
$Source = "WEB"

Write-Host "Testing Depth Market Strategy API" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

$headers = @{
    "Content-Type" = "application/json"
    "Authorization" = "Bearer $BearerToken"
    "appId" = $AppId
    "source" = $Source
}

# MINIMAL TEST - Only required fields
$minimalBody = @{
    user_id = "test_user"
    strategy_name = "Test Strategy"
    stock_codes = @(1)
    exchanges = @("NSE")
    order_type = "LIMIT"
    order_side = "BUY"
    quantity = 100
    exchange = "NSE"
    position_sizing = "FIXED"
} | ConvertTo-Json

Write-Host "MINIMAL REQUEST BODY:" -ForegroundColor Yellow
Write-Host $minimalBody -ForegroundColor Gray
Write-Host ""

try {
    $response = Invoke-RestMethod -Uri "$ApiUrl/api/v1/strategies/depth-market/create" `
        -Method POST `
        -Headers $headers `
        -Body $minimalBody

    Write-Host "SUCCESS!" -ForegroundColor Green
    Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor Green
}
catch {
    Write-Host "FAILED!" -ForegroundColor Red
    Write-Host "Status: $($_.Exception.Response.StatusCode)" -ForegroundColor Red
    Write-Host "Message: $($_.Exception.Message)" -ForegroundColor Red
    
    try {
        $stream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $errorContent = $reader.ReadToEnd()
        $reader.Close()
        if ($errorContent) {
            Write-Host "Response:" -ForegroundColor Red
            Write-Host $errorContent -ForegroundColor Red
        }
    }
    catch { }
}
