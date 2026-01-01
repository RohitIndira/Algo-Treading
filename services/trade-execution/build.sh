#!/bin/bash

# Build script for Trade Execution Service
set -e

SERVICE_NAME="trade-execution-service"
OUTPUT_DIR="bin"
BINARY_NAME="trade-execution"

echo "Building ${SERVICE_NAME}..."

# Create output directory if it doesn't exist
mkdir -p ${OUTPUT_DIR}

# Build the service
go build -o ${OUTPUT_DIR}/${BINARY_NAME} ./cmd/main.go

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"
