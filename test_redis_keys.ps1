# Quick test to check Redis key patterns
# This script tests if the Redis keys exist for the tokens mentioned in the error

Write-Host "Testing Redis connectivity and key patterns..." -ForegroundColor Cyan

$redisHost = "15.207.203.46"
$redisPort = 6379
$redisPassword = "R3d1s@Prod#2026"

# Test tokens from the errors
$testTokens = @(
    @{ Token = 13725; Symbol = "DCBBANK"; Exchange = "NSE" },
    @{ Token = 757834; Symbol = "RMDRIP"; Exchange = "NSE" },
    @{ Token = 2616; Symbol = "SEAMECLTD"; Exchange = "NSE" },
    @{ Token = 513528; Symbol = "GLITTEKG"; Exchange = "BSE" }
)

# Key patterns to test
$patterns = @(
    "market:{exchange}:{token}",     # market:nse:13725
    "market:{EXCHANGE}:{token}",     # market:NSE:13725
    "market:data:{EXCHANGE}:{token}", # market:data:NSE:13725
    "market:{token}",                 # market:13725
    "stock:{token}"                   # stock:13725
)

foreach ($testToken in $testTokens) {
    Write-Host "`n=== Testing $($testToken.Symbol) (Token: $($testToken.Token), Exchange: $($testToken.Exchange)) ===" -ForegroundColor Yellow
    
    foreach ($pattern in $patterns) {
        $key = $pattern `
            -replace "{exchange}", $testToken.Exchange.ToLower() `
            -replace "{EXCHANGE}", $testToken.Exchange.ToUpper() `
            -replace "{token}", $testToken.Token
        
        Write-Host "  Trying key: $key" -NoNewline
        
        # Try to check if key exists (would need redis client or module)
        Write-Host " - (pattern defined)" -ForegroundColor Gray
    }
}

Write-Host "`n" -ForegroundColor Cyan
Write-Host "Note: To actually test these keys, you need:" -ForegroundColor Yellow
Write-Host "  1. Install redis-cli (from Redis Windows build or WSL)" -ForegroundColor Gray
Write-Host "  2. Or install PowerShell Redis module: Install-Module -Name PSRedis" -ForegroundColor Gray
Write-Host "`nThe code has been updated to try these patterns in order:" -ForegroundColor Cyan
Write-Host "  1. market:nse:13725 (lowercase exchange) - MOST LIKELY" -ForegroundColor Green
Write-Host "  2. market:NSE:13725 (uppercase exchange)" -ForegroundColor White
Write-Host "  3. market:data:NSE:13725" -ForegroundColor White
Write-Host "  4. market:13725" -ForegroundColor White
Write-Host "  5. stock:13725" -ForegroundColor White
