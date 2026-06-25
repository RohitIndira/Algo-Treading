#!/bin/bash

# Build script for HFT Engine Service
set -e

SERVICE_NAME="hft-engine"
OUTPUT_DIR="bin"
BINARY_NAME="hft-engine"

echo "Building ${SERVICE_NAME}..."

# Create output directory if it doesn't exist
mkdir -p ${OUTPUT_DIR}

# Build the service. The cmd/ package holds main.go.
# (Standalone diagnostic CLIs cmd/odin-probe + cmd/orderws-probe were
# removed 2026-06-25 — they were never built here and weren't being used.)
go build -o ${OUTPUT_DIR}/${BINARY_NAME} ./cmd

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"
