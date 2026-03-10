#!/bin/bash

# Run script for Risk Management Service
set -e

BINARY_PATH="bin/risk-management"

# Build if binary doesn't exist
if [ ! -f "${BINARY_PATH}" ]; then
    echo "Binary not found. Building..."
    ./build.sh
fi

echo "Starting Risk Management Service..."
./${BINARY_PATH}
