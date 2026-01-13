# # Single-entry script to setup all service databases.
# # This wrapper delegates to scripts/setup_all_databases.ps1 which contains
# # the per-service migrations and DB creation logic.
# #
# # Usage:
# #   .\setup_all_services_db.ps1      # runs the full setup
# #
# # Note: On Windows, run PowerShell as Administrator if you need elevated privileges

# # Enable strict error handling
# $ErrorActionPreference = "Stop"

# # Get the script's directory (equivalent to BASH_SOURCE[0])
# $ROOT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
# $SCRIPT = Join-Path $ROOT_DIR "\setup_all_services_db.ps1"


# # Check if the target script exists
# if (-not (Test-Path $SCRIPT)) {
#     Write-Host "ERROR: central setup script not found: $SCRIPT" -ForegroundColor Red
#     Write-Host "Make sure you're in the repository root and that scripts\setup_all_databases.ps1 exists." -ForegroundColor Red
#     exit 1
# }

# Write-Host "Running database setup using: $SCRIPT" -ForegroundColor Cyan
# Write-Host "Tip: run PowerShell as Administrator if the script needs elevated privileges for PostgreSQL service." -ForegroundColor Yellow
# Write-Host ""

# # Execute the setup script
# try {
#     & $SCRIPT
#     if ($LASTEXITCODE -ne 0) {
#         throw "Script execution failed with exit code: $LASTEXITCODE"
#     }
# } catch {
#     Write-Host "ERROR: Failed to execute setup script" -ForegroundColor Red
#     Write-Host $_.Exception.Message -ForegroundColor Red
#     exit 1
# }

# Write-Host ""
# Write-Host "Done. If any migrations failed, re-run the script to see errors and fix them." -ForegroundColor Green
# Rohit Telang, domain_disabled, Now
# # Single-entry script to setup all service databases.
# # This wrapper delegates to scripts/setup_all_databases.ps1 which contains
# # the per-service migrations and DB creation logic.
# #
# # Usage:
# #   .\setup_all_services_db.ps1      # runs the full setup
# #
# # Note: On Windows, run PowerShell as Administrator if you need elevated privileges

# # Enable strict error handling
# $ErrorActionPreference = "Stop"

# # Get the script's directory (equivalent to BASH_SOURCE[0])
# $ROOT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
# $SCRIPT = Join-Path $ROOT_DIR "scripts\setup_all_databases.ps1"

# # Check if the target script exists
# if (-not (Test-Path $SCRIPT)) {
#     Write-Host "ERROR: central setup script not found: $SCRIPT" -ForegroundColor Red
#     Write-Host "Make sure you're in the repository root and that scripts\setup_all_databases.ps1 exists." -ForegroundColor Red
#     exit 1
# }

# Write-Host "Running database setup using: $SCRIPT" -ForegroundColor Cyan
# Write-Host "Tip: run PowerShell as Administrator if the script needs elevated privileges for PostgreSQL service." -ForegroundColor Yellow
# Write-Host ""

# # Execute the setup script
# try {
#     & $SCRIPT
#     if ($LASTEXITCODE -ne 0) {
#         throw "Script execution failed with exit code: $LASTEXITCODE"
#     }
# } catch {
#     Write-Host "ERROR: Failed to execute setup script" -ForegroundColor Red
#     Write-Host $_.Exception.Message -ForegroundColor Red
#     exit 1
# }

# Write-Host ""
# Write-Host "Done. If any migrations failed, re-run the script to see errors and fix them." -ForegroundColor Green


# Single-entry script to setup all service databases.
# This wrapper delegates to scripts/setup_all_databases.ps1
#
# Usage:
#   .\setup_all_services_db.ps1
#
# Note: Run PowerShell as Administrator if PostgreSQL needs elevated privileges

# Enable strict error handling
$ErrorActionPreference = "Stop"

# Get repository root (directory of this script)
$ROOT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path

# Target worker script (DO NOT point to this file)
$SCRIPT = Join-Path $ROOT_DIR "scripts\setup_all_databases.ps1"

# Validate script exists
if (-not (Test-Path $SCRIPT)) {
    Write-Host "ERROR: central setup script not found: $SCRIPT" -ForegroundColor Red
    Write-Host "Make sure scripts\setup_all_databases.ps1 exists." -ForegroundColor Red
    exit 1
}

Write-Host "Running database setup using: $SCRIPT" -ForegroundColor Cyan
Write-Host "Tip: run PowerShell as Administrator if PostgreSQL needs elevated privileges." -ForegroundColor Yellow
Write-Host ""

# Execute worker script
try {
    & $SCRIPT
    if ($LASTEXITCODE -ne 0) {
        throw "Script execution failed with exit code: $LASTEXITCODE"
    }
} catch {
    Write-Host "ERROR: Failed to execute setup script" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Done. If any migrations failed, re-run the script to see errors and fix them." -ForegroundColor Green
