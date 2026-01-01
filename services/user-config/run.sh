#!/bin/bash

# Run script for User Config Service
set -e

BINARY_PATH="bin/user-config"

# Build if binary doesn't exist
if [ ! -f "${BINARY_PATH}" ]; then
    echo "Binary not found. Building..."
    ./build.sh
fi

echo "Starting User Config Service..."
./${BINARY_PATH}
