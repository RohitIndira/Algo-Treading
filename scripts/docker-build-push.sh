#!/bin/bash
# Docker build and push script for Algo Trading System

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REGISTRY="${DOCKER_REGISTRY:-docker.io}"
NAMESPACE="${DOCKER_NAMESPACE:-rohitindira}"
VERSION="${1:-latest}"
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Services to build
SERVICES=(
  "api-gateway:api/gateway"
  "user-config:services/user-config"
  "data-ingestion:services/data-ingestion"
  "rules-engine:services/rules-engine"
  "trade-execution:services/trade-execution"
  "risk-management:services/risk-management"
)

echo -e "${YELLOW}Docker Build and Push Script${NC}"
echo "Registry: $REGISTRY"
echo "Namespace: $NAMESPACE"
echo "Version: $VERSION"
echo "Build Date: $BUILD_DATE"
echo "Git Commit: $GIT_COMMIT"
echo ""

# Function to build image
build_image() {
  local service_name=$1
  local service_path=$2
  local image_tag="$REGISTRY/$NAMESPACE/trading-$service_name:$VERSION"
  
  echo -e "${YELLOW}Building $service_name...${NC}"
  
  if docker build \
    --file "$service_path/Dockerfile" \
    --tag "$image_tag" \
    --label "version=$VERSION" \
    --label "build_date=$BUILD_DATE" \
    --label "git_commit=$GIT_COMMIT" \
    . ; then
    echo -e "${GREEN}✓ Built: $image_tag${NC}"
    echo "$image_tag" >> /tmp/built_images.txt
  else
    echo -e "${RED}✗ Failed to build $service_name${NC}"
    exit 1
  fi
}

# Function to push image
push_image() {
  local image_tag=$1
  echo -e "${YELLOW}Pushing $image_tag...${NC}"
  
  if docker push "$image_tag" ; then
    echo -e "${GREEN}✓ Pushed: $image_tag${NC}"
  else
    echo -e "${RED}✗ Failed to push $image_tag${NC}"
    exit 1
  fi
}

# Clear previous builds file
> /tmp/built_images.txt

# Build all services
echo -e "${YELLOW}=== Building Services ===${NC}"
for service_config in "${SERVICES[@]}"; do
  IFS=':' read -r service_name service_path <<< "$service_config"
  build_image "$service_name" "$service_path"
done

echo ""
echo -e "${YELLOW}=== Summary ===${NC}"
echo -e "${GREEN}Built images:${NC}"
cat /tmp/built_images.txt

# Ask to push
echo ""
read -p "Do you want to push images to registry? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo -e "${YELLOW}=== Pushing Services ===${NC}"
  while IFS= read -r image_tag; do
    push_image "$image_tag"
  done < /tmp/built_images.txt
  echo ""
  echo -e "${GREEN}✓ All images pushed successfully!${NC}"
else
  echo "Skipped pushing images."
fi

echo ""
echo -e "${GREEN}=== Build Complete ===${NC}"
echo "To use locally:"
echo "  docker-compose up -d"
echo ""
echo "To use from registry:"
echo "  docker pull $REGISTRY/$NAMESPACE/trading-api-gateway:$VERSION"
echo "  ..."
