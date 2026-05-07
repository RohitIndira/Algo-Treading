#!/bin/bash

# Build script for API Gateway Service
set -e

SERVICE_NAME="api-gateway"
OUTPUT_DIR="bin"
BINARY_NAME="gateway"

echo "Building ${SERVICE_NAME}..."

# Create output directory if it doesn't exist
mkdir -p ${OUTPUT_DIR}

# Build the service
go build -o ${OUTPUT_DIR}/${BINARY_NAME} ./cmd/main.go

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"
