#!/bin/bash

echo "================================================"
echo "Docker Compose Installation Script"
echo "================================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check if running on Linux
if [[ "$OSTYPE" != "linux-gnu"* ]]; then
    echo "${RED}This script is for Linux/Ubuntu systems${NC}"
    exit 1
fi

echo "${YELLOW}Checking Docker installation...${NC}"
if ! command -v docker &> /dev/null; then
    echo "${RED}✗ Docker is not installed${NC}"
    echo "Installing Docker..."
    
    # Update package index
    sudo apt-get update
    
    # Install prerequisites
    sudo apt-get install -y \
        ca-certificates \
        curl \
        gnupg \
        lsb-release
    
    # Add Docker's official GPG key
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    
    # Set up repository
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # Install Docker
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    
    # Add current user to docker group
    sudo usermod -aG docker $USER
    
    echo "${GREEN}✓ Docker installed${NC}"
    echo "${YELLOW}You may need to log out and back in for group changes to take effect${NC}"
else
    echo "${GREEN}✓ Docker is already installed${NC}"
fi

echo ""
echo "${YELLOW}Installing Docker Compose Plugin...${NC}"

# Check if Docker Compose plugin is installed
if docker compose version &> /dev/null; then
    echo "${GREEN}✓ Docker Compose plugin is already installed${NC}"
    docker compose version
else
    # Install Docker Compose plugin
    sudo apt-get update
    sudo apt-get install -y docker-compose-plugin
    
    if [ $? -eq 0 ]; then
        echo "${GREEN}✓ Docker Compose plugin installed${NC}"
        docker compose version
    else
        echo "${RED}✗ Failed to install Docker Compose plugin${NC}"
        exit 1
    fi
fi

echo ""
echo "${GREEN}================================================${NC}"
echo "${GREEN}Installation Complete!${NC}"
echo "${GREEN}================================================${NC}"
echo ""
echo "Docker version:"
docker --version
echo ""
echo "Docker Compose version:"
docker compose version
echo ""
echo "${YELLOW}Important:${NC}"
echo "If this is your first time installing Docker, you may need to:"
echo "1. Log out and log back in (for group permissions)"
echo "2. Or run: newgrp docker"
echo ""
echo "To test Docker:"
echo "  docker run hello-world"
echo ""
echo "Now you can run the Kafka setup:"
echo "  ./deployments/docker/setup_kafka.sh"
echo ""
