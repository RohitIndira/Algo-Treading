#!/bin/bash

# Run script for Data Ingestion Service
set -e

BINARY_PATH="bin/data-ingestion"

# Build if binary doesn't exist
if [ ! -f "${BINARY_PATH}" ]; then
    echo "Binary not found. Building..."
    ./build.sh
fi

echo "Starting Data Ingestion Service..."
./${BINARY_PATH}
