#!/bin/bash

# Build script for HFT Engine Service
set -e

SERVICE_NAME="hft-engine"
OUTPUT_DIR="bin"
BINARY_NAME="hft-engine"

echo "Building ${SERVICE_NAME}..."

# Create output directory if it doesn't exist
mkdir -p ${OUTPUT_DIR}

# Build the service. The cmd/ package holds main.go; cmd/odin-probe is a
# separate debug tool and is not built here.
go build -o ${OUTPUT_DIR}/${BINARY_NAME} ./cmd

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"
