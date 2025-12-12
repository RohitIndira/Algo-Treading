#!/bin/bash

# Build script for Data Ingestion Service
set -e

SERVICE_NAME="data-ingestion-service"
OUTPUT_DIR="bin"
BINARY_NAME="data-ingestion"

echo "Building ${SERVICE_NAME}..."

# Create output directory if it doesn't exist
mkdir -p ${OUTPUT_DIR}

# Build the service
go build -o ${OUTPUT_DIR}/${BINARY_NAME} ./cmd/main.go

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"
