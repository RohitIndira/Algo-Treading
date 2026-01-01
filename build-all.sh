#!/bin/bash

# Master build script for all services
set -e

PROJECT_ROOT="/home/stockkask/algo-trading/Algo-Treading"

echo "=========================================="
echo "Building All Services"
echo "=========================================="

# Array of Go services with their paths
declare -a GO_SERVICES=(
    "api/gateway:API Gateway"
    "services/user-config:User Config Service"
    "services/risk-management:Risk Management Service"
    "services/rules-engine:Rules Engine Service"
    "services/trade-execution:Trade Execution Service"
    "services/data-ingestion:Data Ingestion Service"
)

# Build each Go service
for service in "${GO_SERVICES[@]}"
do
    IFS=':' read -r path name <<< "$service"
    echo ""
    echo "=========================================="
    echo "Building: $name"
    echo "Path: $path"
    echo "=========================================="
    
    cd "${PROJECT_ROOT}/${path}"
    
    if [ -f "build.sh" ]; then
        ./build.sh
    else
        echo "Warning: build.sh not found in ${path}"
    fi
done

echo ""
echo "=========================================="
echo "Build Summary"
echo "=========================================="
echo "Checking built binaries..."
echo ""

# Check all built binaries
for service in "${GO_SERVICES[@]}"
do
    IFS=':' read -r path name <<< "$service"
    binary_path="${PROJECT_ROOT}/${path}/bin"
    
    if [ -d "$binary_path" ]; then
        echo "✓ $name:"
        ls -lh "${binary_path}"
    else
        echo "✗ $name: No binary found"
    fi
done

echo ""
echo "=========================================="
echo "All builds complete!"
echo "=========================================="
