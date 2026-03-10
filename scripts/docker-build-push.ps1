# Docker build and push script for Algo Trading System (PowerShell)
# Usage: .\scripts\docker-build-push.ps1 -Version "v1.0.0" -Push

param(
    [string]$Version = "latest",
    [string]$Registry = $env:DOCKER_REGISTRY -or "docker.io",
    [string]$Namespace = $env:DOCKER_NAMESPACE -or "rohitindira",
    [switch]$Push = $false,
    [switch]$NoBuildKit = $false
)

# Error handling
$ErrorActionPreference = "Stop"

# Colors
function Write-Info { Write-Host $args[0] -ForegroundColor Cyan }
function Write-Success { Write-Host $args[0] -ForegroundColor Green }
function Write-Error { Write-Host $args[0] -ForegroundColor Red }
function Write-Warning { Write-Host $args[0] -ForegroundColor Yellow }

Write-Info "Docker Build and Push Script"
Write-Info "Registry: $Registry"
Write-Info "Namespace: $Namespace"
Write-Info "Version: $Version"
Write-Info ""

# Get git info
$gitCommit = (git rev-parse --short HEAD 2>$null) -or "unknown"
$buildDate = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

# Services to build
$services = @(
    @{Name = "api-gateway"; Path = "api/gateway"},
    @{Name = "user-config"; Path = "services/user-config"},
    @{Name = "data-ingestion"; Path = "services/data-ingestion"},
    @{Name = "rules-engine"; Path = "services/rules-engine"},
    @{Name = "trade-execution"; Path = "services/trade-execution"},
    @{Name = "risk-management"; Path = "services/risk-management"}
)

$builtImages = @()

Write-Warning "=== Building Services ==="

foreach ($service in $services) {
    $serviceName = $service.Name
    $servicePath = $service.Path
    $imageTag = "$Registry/$Namespace/trading-$serviceName`:$Version"
    
    Write-Info "Building $serviceName..."
    
    try {
        # Build command
        $buildArgs = @(
            "build",
            "--file", "$servicePath/Dockerfile",
            "--tag", $imageTag,
            "--label", "version=$Version",
            "--label", "build_date=$buildDate",
            "--label", "git_commit=$gitCommit"
        )
        
        if ($NoBuildKit) {
            $buildArgs += "--progress=plain"
        }
        
        $buildArgs += "."
        
        & docker $buildArgs
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "✓ Built: $imageTag"
            $builtImages += $imageTag
        } else {
            Write-Error "✗ Failed to build $serviceName"
            exit 1
        }
    }
    catch {
        Write-Error "Error building $serviceName: $_"
        exit 1
    }
}

Write-Info ""
Write-Warning "=== Summary ==="
Write-Success "Built images:"
$builtImages | ForEach-Object { Write-Host "  $_" -ForegroundColor Green }

# Ask to push
if ($Push) {
    Write-Warning "=== Pushing Services ==="
    
    foreach ($imageTag in $builtImages) {
        Write-Info "Pushing $imageTag..."
        
        try {
            & docker push $imageTag
            
            if ($LASTEXITCODE -eq 0) {
                Write-Success "✓ Pushed: $imageTag"
            } else {
                Write-Error "✗ Failed to push $imageTag"
                exit 1
            }
        }
        catch {
            Write-Error "Error pushing $imageTag: $_"
            exit 1
        }
    }
    
    Write-Info ""
    Write-Success "✓ All images pushed successfully!"
}

Write-Info ""
Write-Success "=== Build Complete ==="
Write-Info "To use locally:"
Write-Host "  docker-compose up -d" -ForegroundColor Gray
Write-Info ""
Write-Info "To use from registry:"
$builtImages | ForEach-Object { Write-Host "  docker pull $_" -ForegroundColor Gray }
