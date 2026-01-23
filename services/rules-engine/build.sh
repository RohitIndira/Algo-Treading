#!/bin/bash

# Build script for Rules Engine Service
set -e

SERVICE_NAME="rules-engine-service"
OUTPUT_DIR="bin"
BINARY_NAME="rules-engine"

echo "Building ${SERVICE_NAME}..."

# Create output directory if it doesn't exist
mkdir -p ${OUTPUT_DIR}

# Build the service
# NOTE: We build the entire ./cmd package so that helper files like
# jobbing_config_loader.go are included alongside main.go.
go build -o ${OUTPUT_DIR}/${BINARY_NAME} ./cmd

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"
