#!/bin/bash

set -e

BINARY_PATH="bin/paper-execution"

if [ ! -f "${BINARY_PATH}" ]; then
  echo "Binary not found. Building..."
  ./build.sh
fi

echo "Starting Paper Execution Service..."
./${BINARY_PATH}
