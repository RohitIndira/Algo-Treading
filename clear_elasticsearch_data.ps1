# PowerShell script to clear all data from Elasticsearch
# This script deletes all indices in the local Elasticsearch instance

$EsUrl = "http://localhost:9200"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Elasticsearch Data Clearing Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Elasticsearch is running
Write-Host "Checking Elasticsearch connectivity..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$EsUrl/_cluster/health" -Method Get -ErrorAction Stop -TimeoutSec 5
    Write-Host "Elasticsearch is running (Status: $($response.status))" -ForegroundColor Green
} catch {
    Write-Host "Error: Could not connect to Elasticsearch at $EsUrl" -ForegroundColor Red
    Write-Host "Please ensure the Elasticsearch container is running." -ForegroundColor Yellow
    exit 1
}

Write-Host ""

# List current indices
Write-Host "Fetching current indices..." -ForegroundColor Yellow
try {
    $indices = Invoke-RestMethod -Uri "$EsUrl/_cat/indices?format=json" -Method Get
    
    if (-not $indices -or $indices.Count -eq 0) {
        Write-Host "No indices found to delete." -ForegroundColor Green
        exit
    }

    Write-Host "Found $($indices.Count) indices:" -ForegroundColor Cyan
    foreach ($index in $indices) {
        Write-Host "  - $($index.index) (Docs: $($index.'docs.count'))" -ForegroundColor White
    }
} catch {
    Write-Host "Error fetching indices: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "WARNING: This will delete ALL data (indices) listed above." -ForegroundColor Yellow
Write-Host "This action cannot be undone." -ForegroundColor Red
Write-Host ""
$confirmation = Read-Host "Are you sure you want to proceed? (yes/no)"

if ($confirmation -ne "yes") {
    Write-Host "Operation cancelled." -ForegroundColor Red
    exit
}

Write-Host ""
Write-Host "Clearing Elasticsearch data..." -ForegroundColor Cyan

# Delete all indices
try {
    # Using wildcard to delete all indices
    $deleteResponse = Invoke-RestMethod -Uri "$EsUrl/*" -Method Delete
    
    if ($deleteResponse.acknowledged -eq $true) {
        Write-Host "Successfully deleted all indices." -ForegroundColor Green
    } else {
        Write-Host "Command sent, but acknowledgement was not true: $($deleteResponse | ConvertTo-Json -Depth 2)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "Error deleting indices: $_" -ForegroundColor Red
    
    # Fallback: Try deleting indices individually if wildcard deletion fails or is protected
    Write-Host "Attempting to delete specific indices mentioned in configuration..." -ForegroundColor Yellow
    $knownIndices = @("user_strategies", "trading_*", "strategies")
    
    foreach ($idx in $knownIndices) {
        try {
            Invoke-RestMethod -Uri "$EsUrl/$idx" -Method Delete -ErrorAction SilentlyContinue | Out-Null
            Write-Host "  - Deleted matching pattern: $idx" -ForegroundColor Green
        } catch {
            # Ignore errors for non-existent indices in fallback
        }
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Data clearing process completed!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# Verify
Write-Host "Verifying..." -ForegroundColor Yellow
$remainingIndices = Invoke-RestMethod -Uri "$EsUrl/_cat/indices?format=json" -Method Get -ErrorAction SilentlyContinue

if (-not $remainingIndices -or $remainingIndices.Count -eq 0) {
    Write-Host "All indices cleared. Elasticsearch is empty." -ForegroundColor Green
} else {
    Write-Host "Some indices still remain:" -ForegroundColor Red
    foreach ($index in $remainingIndices) {
        Write-Host "  - $($index.index)" -ForegroundColor White
    }
}
