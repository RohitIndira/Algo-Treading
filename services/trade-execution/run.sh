#!/bin/bash

# Run script for Trade Execution Service
set -e

BINARY_PATH="bin/trade-execution"

# Build if binary doesn't exist
if [ ! -f "${BINARY_PATH}" ]; then
    echo "Binary not found. Building..."
    ./build.sh
fi

echo "Starting Trade Execution Service..."
./${BINARY_PATH}
