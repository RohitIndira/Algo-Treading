#!/bin/bash

set -e

SERVICE_NAME="paper-execution"
OUTPUT_DIR="bin"
BINARY_NAME="paper-execution"

echo "Building ${SERVICE_NAME}..."

mkdir -p ${OUTPUT_DIR}

go build -o ${OUTPUT_DIR}/${BINARY_NAME} ./cmd/main.go

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"
