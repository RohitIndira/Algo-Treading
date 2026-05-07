#!/bin/bash

# Master build script for all services
# Dynamically resolves paths — works on any system/clone location
set -e

# Resolve PROJECT_ROOT to the directory containing this script
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Optional cross-compilation overrides (e.g. GOOS=linux GOARCH=amd64 ./build-all.sh)
export GOOS="${GOOS:-$(go env GOOS)}"
export GOARCH="${GOARCH:-$(go env GOARCH)}"

echo "=========================================="
echo "Building All Services"
echo "Project root : ${PROJECT_ROOT}"
echo "Target OS    : ${GOOS}"
echo "Target Arch  : ${GOARCH}"
echo "=========================================="

BUILD_ERRORS=()
BUILT=()

# Auto-discover all services that have a build.sh
# Looks in: api/*/build.sh and services/*/build.sh
mapfile -t BUILD_SCRIPTS < <(find "${PROJECT_ROOT}/api" "${PROJECT_ROOT}/services" \
    -maxdepth 2 -name "build.sh" 2>/dev/null | sort)

if [ ${#BUILD_SCRIPTS[@]} -eq 0 ]; then
    echo "No build.sh scripts found under api/ or services/"
    exit 1
fi

for script in "${BUILD_SCRIPTS[@]}"; do
    service_dir="$(dirname "$script")"
    # Relative path for display
    rel_path="${service_dir#"${PROJECT_ROOT}/"}"
    service_name="$(basename "$service_dir")"

    echo ""
    echo "=========================================="
    echo "Building : ${service_name}"
    echo "Path     : ${rel_path}"
    echo "=========================================="

    (
        cd "$service_dir"
        chmod +x build.sh
        ./build.sh
    ) && BUILT+=("${rel_path}") || BUILD_ERRORS+=("${rel_path}")
done

echo ""
echo "=========================================="
echo "Build Summary"
echo "=========================================="

# Report built binaries
for rel_path in "${BUILT[@]}"; do
    bin_dir="${PROJECT_ROOT}/${rel_path}/bin"
    if [ -d "$bin_dir" ]; then
        echo "  [OK] ${rel_path}:"
        ls -lh "${bin_dir}"
    else
        echo "  [OK] ${rel_path}: (no bin/ dir — check build.sh output)"
    fi
done

# Report failures
for rel_path in "${BUILD_ERRORS[@]}"; do
    echo "  [FAIL] ${rel_path}: build failed"
done

echo ""
if [ ${#BUILD_ERRORS[@]} -eq 0 ]; then
    echo "All ${#BUILT[@]} service(s) built successfully for ${GOOS}/${GOARCH}."
else
    echo "${#BUILT[@]} succeeded, ${#BUILD_ERRORS[@]} failed."
    exit 1
fi
echo "=========================================="
