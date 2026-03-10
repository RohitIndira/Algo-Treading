#!/bin/bash

# Run script for Rules Engine Service
set -e

BINARY_PATH="bin/rules-engine"

# Build if binary doesn't exist
if [ ! -f "${BINARY_PATH}" ]; then
    echo "Binary not found. Building..."
    ./build.sh
fi

echo "Starting Rules Engine Service..."
./${BINARY_PATH}
