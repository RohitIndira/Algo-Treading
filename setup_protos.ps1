# PowerShell script to generate Go code from Protocol Buffer files
# This script automates proto generation for all services.

$ErrorActionPreference = "Stop"

Write-Host "============================" -ForegroundColor Cyan
Write-Host "Proto Generation Tool" -ForegroundColor Cyan
Write-Host "============================" -ForegroundColor Cyan

# 1. Check for required tools
function Check-Tool {
    param([string]$Name, [string]$Command)
    try {
        & $Command --version | Out-Null
        Write-Host "✅ $Name is installed" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "❌ $Name is NOT installed" -ForegroundColor Red
        return $false
    }
}

$hasProtoc = Check-Tool "protoc" "protoc"
$hasGoGen = Check-Tool "protoc-gen-go" "protoc-gen-go"
$hasGoGrpcGen = Check-Tool "protoc-gen-go-grpc" "protoc-gen-go-grpc"

if (-not ($hasProtoc -and $hasGoGen -and $hasGoGrpcGen)) {
    Write-Host ""
    Write-Host "Missing required tools!" -ForegroundColor Yellow
    Write-Host "Please ensure 'protoc' is in your PATH." -ForegroundColor White
    Write-Host "For Go plugins, run:" -ForegroundColor White
    Write-Host "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
    Write-Host "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
    Write-Host ""
    
    $install = Read-Host "Would you like to try installing Go plugins now? (y/n)"
    if ($install -eq "y") {
        Write-Host "Installing tools..." -ForegroundColor Yellow
        go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
        go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
        Write-Host "Tools installed! Please ensure %GOPATH%/bin is in your PATH." -ForegroundColor Green
    } else {
        Write-Host "Exiting..." -ForegroundColor Red
        exit 1
    }
}

# 2. Define Proto Files
$ProtoFiles = @(
    "api/proto/common/common.proto",
    "api/proto/user_config/user_config.proto",
    "api/proto/risk_management/risk_management.proto",
    "api/proto/trade_execution/trade_execution.proto",
    "api/proto/rules_engine/rules_engine.proto"
)

# 3. Generate Code
Write-Host ""
Write-Host "Generating code..." -ForegroundColor Cyan

foreach ($proto in $ProtoFiles) {
    if (Test-Path $proto) {
        Write-Host "  -> Generating for $proto..." -ForegroundColor Yellow
        # Run protoc from the project root
        # Using source_relative paths as established in the Makefile
        protoc --proto_path=. `
               --go_out=. --go_opt=paths=source_relative `
               --go-grpc_out=. --go-grpc_opt=paths=source_relative `
               --experimental_allow_proto3_optional `
               $proto
    } else {
        Write-Host "  ⚠️ Warning: $proto not found, skipping." -ForegroundColor Magenta
    }
}

Write-Host ""
Write-Host "Proto generation completed successfully!" -ForegroundColor Green
Write-Host "============================" -ForegroundColor Cyan
