@echo off
REM Docker build and push script for Algo Trading System (Batch)
REM Usage: docker-build-push.bat [version] [push]
REM Example: docker-build-push.bat v1.0.0 push

setlocal enabledelayedexpansion

set VERSION=%1
if "!VERSION!"=="" set VERSION=latest

set PUSH_IMAGES=%2
if "!PUSH_IMAGES!"=="" set PUSH_IMAGES=no

set REGISTRY=docker.io
set NAMESPACE=rohitindira

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    color 0C
    echo Docker is not installed or not in PATH
    exit /b 1
)

color 0B
echo.
echo ======================================
echo Docker Build and Push Script
echo ======================================
echo Registry: %REGISTRY%
echo Namespace: %NAMESPACE%
echo Version: %VERSION%
echo Push images: %PUSH_IMAGES%
echo.

REM Get git commit hash if available
for /f "tokens=*" %%i in ('git rev-parse --short HEAD 2^>nul') do set GIT_COMMIT=%%i
if "!GIT_COMMIT!"=="" set GIT_COMMIT=unknown

REM Get build date
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set BUILD_DATE=%%c-%%a-%%b)

echo Build Date: !BUILD_DATE!
echo Git Commit: !GIT_COMMIT!
echo.

REM Services to build
set SERVICES[0]=api-gateway:api/gateway
set SERVICES[1]=user-config:services/user-config
set SERVICES[2]=data-ingestion:services/data-ingestion
set SERVICES[3]=rules-engine:services/rules-engine
set SERVICES[4]=trade-execution:services/trade-execution
set SERVICES[5]=risk-management:services/risk-management

color 0E
echo ===== Building Services =====
echo.

set INDEX=0
for %%s in (!SERVICES[0]! !SERVICES[1]! !SERVICES[2]! !SERVICES[3]! !SERVICES[4]! !SERVICES[5]!) do (
    for /f "tokens=1,2 delims=:" %%a in ("%%s") do (
        set SERVICE_NAME=%%a
        set SERVICE_PATH=%%b
        set IMAGE_TAG=%REGISTRY%/%NAMESPACE%/trading-!SERVICE_NAME!:%VERSION%
        
        color 0E
        echo Building !SERVICE_NAME!...
        
        docker build ^
            --file "!SERVICE_PATH!\Dockerfile" ^
            --tag "!IMAGE_TAG!" ^
            --label "version=%VERSION%" ^
            --label "build_date=!BUILD_DATE!" ^
            --label "git_commit=!GIT_COMMIT!" ^
            .
        
        if errorlevel 1 (
            color 0C
            echo Error building !SERVICE_NAME!
            exit /b 1
        )
        
        color 0A
        echo [OK] Built: !IMAGE_TAG!
        echo !IMAGE_TAG! >> built_images.txt
    )
)

echo.
color 0B
echo ===== Summary =====
color 0A
echo Built images:
for /f "delims=" %%i in (built_images.txt) do (
    echo   %%i
)

echo.
if /i "!PUSH_IMAGES!"=="push" (
    color 0E
    echo ===== Pushing Services =====
    echo.
    
    setlocal enabledelayedexpansion
    for /f "delims=" %%i in (built_images.txt) do (
        color 0E
        echo Pushing %%i...
        
        docker push %%i
        
        if errorlevel 1 (
            color 0C
            echo Error pushing %%i
            exit /b 1
        )
        
        color 0A
        echo [OK] Pushed: %%i
    )
    
    echo.
    color 0A
    echo All images pushed successfully!
) else (
    echo To push images, run:
    echo   docker-build-push.bat %VERSION% push
)

del built_images.txt 2>nul

echo.
color 0B
echo ===== Build Complete =====
color 0A
echo.
echo To use locally:
echo   docker-compose up -d
echo.
echo To use from registry, update docker-compose.yml with image names above
echo.

color 07
