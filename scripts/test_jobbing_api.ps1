#!/usr/bin/env pwsh
# PowerShell script to test Jobbing Strategy API endpoints

$ErrorActionPreference = "Stop"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Jobbing Strategy API Test Suite" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$API_BASE_URL = if ($env:API_BASE_URL) { $env:API_BASE_URL } else { "http://localhost:8080" }
$USER_ID = if ($env:TEST_USER_ID) { $env:TEST_USER_ID } else { "ISPL19027" }

Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  API Base URL: $API_BASE_URL" -ForegroundColor White
Write-Host "  Test User ID: $USER_ID" -ForegroundColor White
Write-Host ""

# Test counter
$testCount = 0
$passedTests = 0
$failedTests = 0

function Test-Endpoint {
    param(
        [string]$Name,
        [string]$Method,
        [string]$Url,
        [object]$Body = $null,
        [int]$ExpectedStatus = 200
    )
    
    $script:testCount++
    Write-Host "Test $testCount`: $Name" -ForegroundColor Cyan
    Write-Host "  Method: $Method" -ForegroundColor DarkGray
    Write-Host "  URL: $Url" -ForegroundColor DarkGray
    
    try {
        $params = @{
            Uri = $Url
            Method = $Method
            ContentType = "application/json"
            ErrorAction = "Stop"
        }
        
        if ($Body) {
            $jsonBody = $Body | ConvertTo-Json -Depth 10
            Write-Host "  Body: $jsonBody" -ForegroundColor DarkGray
            $params.Body = $jsonBody
        }
        
        $response = Invoke-RestMethod @params
        $statusCode = 200  # Invoke-RestMethod doesn't expose status code easily
        
        Write-Host "  Response:" -ForegroundColor Green
        Write-Host ($response | ConvertTo-Json -Depth 10) -ForegroundColor White
        
        $script:passedTests++
        Write-Host "  ✓ PASSED" -ForegroundColor Green
        Write-Host ""
        return $response
        
    } catch {
        $script:failedTests++
        Write-Host "  ✗ FAILED" -ForegroundColor Red
        Write-Host "  Error: $_" -ForegroundColor Red
        if ($_.Exception.Response) {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $responseBody = $reader.ReadToEnd()
            Write-Host "  Response: $responseBody" -ForegroundColor Red
        }
        Write-Host ""
        return $null
    }
}

# Test 1: Configure Jobbing Strategy (Create)
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 1: Configure Jobbing Strategy" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

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
        },
        @{
            token = "500325"
            symbol = "RELIANCE"
            exchange = "NSE"
            lower_range = 2400.0
            higher_range = 2600.0
            initial_buy_offset = 0.50
            distance_continue = 0.50
            quantity_per_order = 1
            max_quantity = 5
            trading_mode = "PAPER"
            enabled = $true
        }
    )
}

$configResult = Test-Endpoint `
    -Name "Configure jobbing strategy for 2 tokens" `
    -Method "POST" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing/configure" `
    -Body $configBody

Start-Sleep -Seconds 1

# Test 2: Get All Jobbing Configs
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 2: Get All Jobbing Configs" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

Test-Endpoint `
    -Name "Get all jobbing configs for user" `
    -Method "GET" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing?user_id=$USER_ID"

Start-Sleep -Seconds 1

# Test 3: Get Single Jobbing Config
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 3: Get Single Jobbing Config" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

Test-Endpoint `
    -Name "Get jobbing config for specific token" `
    -Method "GET" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing/30274?user_id=$USER_ID"

Start-Sleep -Seconds 1

# Test 4: Update Jobbing Config
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 4: Update Jobbing Config" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

$updateBody = @{
    user_id = $USER_ID
    lower_range = 11.0
    higher_range = 14.0
    max_quantity = 15
}

Test-Endpoint `
    -Name "Update jobbing config parameters" `
    -Method "PUT" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing/30274" `
    -Body $updateBody

Start-Sleep -Seconds 1

# Test 5: Disable Jobbing Config
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 5: Disable Jobbing Config" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

$disableBody = @{
    user_id = $USER_ID
}

Test-Endpoint `
    -Name "Disable jobbing config" `
    -Method "POST" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing/30274/disable" `
    -Body $disableBody

Start-Sleep -Seconds 1

# Test 6: Get Enabled Only
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 6: Get Enabled Configs Only" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

Test-Endpoint `
    -Name "Get only enabled jobbing configs" `
    -Method "GET" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing?user_id=$USER_ID`&enabled_only=true"

Start-Sleep -Seconds 1

# Test 7: Enable Jobbing Config
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 7: Enable Jobbing Config" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

$enableBody = @{
    user_id = $USER_ID
}

Test-Endpoint `
    -Name "Enable jobbing config" `
    -Method "POST" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing/30274/enable" `
    -Body $enableBody

Start-Sleep -Seconds 1

# Test 8: Update to LIVE mode
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 8: Update Trading Mode to LIVE" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

$liveModeBody = @{
    user_id = $USER_ID
    trading_mode = "LIVE"
}

Test-Endpoint `
    -Name "Update trading mode to LIVE" `
    -Method "PUT" `
    -Url "$API_BASE_URL/api/v1/strategies/jobbing/30274" `
    -Body $liveModeBody

Start-Sleep -Seconds 1

# Test 9: Delete Jobbing Config (Cleanup)
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "Test 9: Delete Jobbing Config (Optional)" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
Write-Host ""

Write-Host "Skipping delete test to preserve test data" -ForegroundColor Yellow
Write-Host "To delete, uncomment the following code:" -ForegroundColor DarkGray
Write-Host @"
# Test-Endpoint ``
#     -Name "Delete jobbing config" ``
#     -Method "DELETE" ``
#     -Url "$API_BASE_URL/api/v1/strategies/jobbing/500325?user_id=$USER_ID"
"@ -ForegroundColor DarkGray
Write-Host ""

# Summary
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Test Summary" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Total Tests: $testCount" -ForegroundColor White
Write-Host "Passed: $passedTests" -ForegroundColor Green
Write-Host "Failed: $failedTests" -ForegroundColor $(if ($failedTests -gt 0) { "Red" } else { "Green" })
Write-Host ""

if ($failedTests -eq 0) {
    Write-Host "✓ All tests passed!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "✗ Some tests failed!" -ForegroundColor Red
    exit 1
}
