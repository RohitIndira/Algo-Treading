#!/bin/bash

# ============================================
# Trading System - Complete Setup Script
# ============================================
# This script sets up and runs the entire trading system
# Including: Infrastructure + All Services
# ============================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project root
PROJECT_ROOT="/home/rohitt/Desktop/trading-system"

echo -e "${BLUE}"
echo "============================================"
echo "Trading System - Complete Setup"
echo "============================================"
echo -e "${NC}"

# ============================================
# STEP 1: Check Prerequisites
# ============================================
echo -e "${YELLOW}[STEP 1/10] Checking Prerequisites...${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not found. Please install Docker first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker found${NC}"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose not found. Installing...${NC}"
    bash "$PROJECT_ROOT/deployments/docker/install_docker_compose.sh"
fi
echo -e "${GREEN}✓ Docker Compose found${NC}"

# Check Go
if ! command -v go &> /dev/null; then
