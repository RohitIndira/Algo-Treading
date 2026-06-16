#!/bin/bash

# Run script for API Gateway
set -e

BINARY_PATH="bin/gateway"

# Build if binary doesn't exist
if [ ! -f "${BINARY_PATH}" ]; then
    echo "Binary not found. Building..."
    ./build.sh
fi

echo "Starting API Gateway..."
./${BINARY_PATH}
