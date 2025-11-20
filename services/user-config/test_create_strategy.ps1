# Test script to create user config strategies for testing
# This PowerShell script uses grpcurl to create test strategies

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Creating Test User Config Strategies" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Check if grpcurl is installed
$grpcurlPath = Get-Command grpcurl -ErrorAction SilentlyContinue
if (-not $grpcurlPath) {
    Write-Host "❌ grpcurl is not installed. Install it with:" -ForegroundColor Red
    Write-Host "   go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest" -ForegroundColor Yellow
    exit 1
}

# Test: High Score Strategy (optimized to score above 80)
Write-Host "Creating 'High Score Match Strategy' (optimized to score above 80)..." -ForegroundColor Yellow

$json1 = Get-Content -Path "test_news_config.json" -Raw
$json1 | grpcurl -plaintext -d '@' localhost:50051 user_config.UserConfigService/CreateStrategy

Write-Host ""
Write-Host "Strategy created successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Strategy Scoring Breakdown (needs 80+ to match):" -ForegroundColor Yellow
Write-Host "  - Impact Score:  25% (always matches with threshold=1)" -ForegroundColor White
Write-Host "  - Stock:         20% (empty array = matches all)" -ForegroundColor White
Write-Host "  - Sentiment:     15% (all 3 sentiments = matches all)" -ForegroundColor White
Write-Host "  - Category:      15% (15 categories = high coverage)" -ForegroundColor White
Write-Host "  - Price Range:   10% (1-100000 = matches almost all)" -ForegroundColor White
Write-Host "  - Volume:        7.5% (threshold=1 = matches all)" -ForegroundColor White
Write-Host "  - Pct Change:    5% (0.01% = matches all)" -ForegroundColor White
Write-Host "  - Exchange:      2.5% (NSE+BSE = matches most)" -ForegroundColor White
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "To list strategy:" -ForegroundColor Yellow
Write-Host "   grpcurl -plaintext -d '{`"user_id`": `"TEST_USER_001`"}' localhost:50051 user_config.UserConfigService/ListUserStrategies" -ForegroundColor White
